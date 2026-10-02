#pragma once

#include "StreamTypes.hpp"

#include <boost/beast/core/flat_buffer.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/unordered/unordered_node_map.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace AVEVA::Private
{
    // Identifies the (host, service) origin a pooled connection belongs to. Parameterized on
    // Stream so a key is only ever valid for the ConnectionPool<Stream> it was created for;
    // use it via the BasicConnectionKey/TlsConnectionKey aliases below rather than naming
    // ConnectionKey<Stream> directly.
    template <typename Stream> struct ConnectionKey
    {
        std::string host;
        std::string service;

        bool operator==(const ConnectionKey& other) const noexcept
        {
            return host == other.host && service == other.service;
        }
    };

    template <typename Stream> struct ConnectionKeyHash
    {
        std::size_t operator()(const ConnectionKey<Stream>& key) const noexcept
        {
            std::size_t seed = std::hash<std::string>{}(key.host);
            seed ^= std::hash<std::string>{}(key.service) + 0x9e3779b9U + (seed << 6) + (seed >> 2);
            return seed;
        }
    };

    template <typename Stream> struct ConnectionKeyEqual
    {
        bool operator()(const ConnectionKey<Stream>& lhs, const ConnectionKey<Stream>& rhs) const noexcept
        {
            return lhs == rhs;
        }
    };

    template <typename Stream> struct PooledConnection
    {
        std::unique_ptr<Stream> stream;
        beast::flat_buffer buffer;
    };

    // Aliases for the two connection pool element types used by HttpClient.
    using BasicConnectionKey = ConnectionKey<PlainStream>;
    using TlsConnectionKey = ConnectionKey<TlsStream>;

    namespace ConnectionPoolDetail
    {
        namespace intrusive = boost::intrusive;

        struct IdleTag;
        struct LruTag;

        // normal_link keeps the hooks pointer-sized and makes list::clear() O(1); the pool never
        // needs to ask a node whether it is currently linked.
        using IdleHook =
            intrusive::list_base_hook<intrusive::tag<IdleTag>, intrusive::link_mode<intrusive::normal_link>>;
        using LruHook = intrusive::list_base_hook<intrusive::tag<LruTag>, intrusive::link_mode<intrusive::normal_link>>;
    } // namespace ConnectionPoolDetail

    // Caches idle keep-alive connections per (scheme, host, port) so repeated requests to the
    // same origin can skip DNS resolution, TCP connect, and (for HTTPS) the TLS handshake.
    //
    // Every cached connection lives in a node that is threaded onto two intrusive lists at once:
    // the per-origin idle list that Acquire searches, and a single pool-wide LRU list ordered
    // oldest-first that makes expiry a bounded walk from the front instead of a per-origin scan.
    // Because the lists are intrusive, linking, unlinking, and moving a connection between them
    // costs a few pointer writes and never allocates, and retired nodes are recycled rather than
    // freed so a steady request stream performs no node allocation at all.
    template <typename Stream> class ConnectionPool
    {
      public:
        ConnectionPool(std::size_t maxIdlePerKey, std::chrono::seconds idleTimeout)
            : m_maxIdlePerKey(maxIdlePerKey), m_idleTimeout(idleTimeout)
        {
        }

        ConnectionPool(const ConnectionPool&) = delete;
        ConnectionPool& operator=(const ConnectionPool&) = delete;

        ~ConnectionPool()
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_origins.clear();
            m_lru.clear_and_dispose(NodeDisposer{});
            m_recycled.clear_and_dispose(NodeDisposer{});
        }

        std::optional<PooledConnection<Stream>> Acquire(const ConnectionKey<Stream>& key)
        {
            LruList retired;
            std::optional<PooledConnection<Stream>> connection;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                DropExpired(std::chrono::steady_clock::now(), retired);
                auto it = m_origins.find(key);
                if (it != m_origins.end())
                {
                    // Origins are erased as soon as they run dry and DropExpired has already
                    // removed every timed-out node, so this list holds only usable connections.
                    Origin& origin = it->second;
                    Node& node = origin.idle.back(); // Newest, and least likely to have been closed by the peer.
                    origin.idle.pop_back();
                    m_lru.erase(m_lru.iterator_to(node));
                    connection.emplace(std::move(node.connection));
                    Recycle(node);
                    if (origin.idle.empty())
                    {
                        m_origins.erase(it);
                    }
                }
            }
            // Closing the retired sockets runs outside the lock.
            retired.clear_and_dispose(NodeDisposer{});
            return connection;
        }

        void Release(const ConnectionKey<Stream>& key, PooledConnection<Stream> connection)
        {
            if (m_maxIdlePerKey == 0 || !connection.stream)
            {
                return;
            }
            LruList retired;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                const auto now = std::chrono::steady_clock::now();
                DropExpired(now, retired);

                Node& node = *Obtain();
                node.connection = std::move(connection);
                node.idleSince = now;

                auto [it, inserted] = m_origins.try_emplace(key);
                Origin& origin = it->second;
                if (inserted)
                {
                    // unordered_node_map keeps keys and mapped values at stable addresses, so a
                    // node can point back at its origin for O(1) unlinking during expiry.
                    origin.key = &it->first;
                }
                else if (origin.idle.size() >= m_maxIdlePerKey)
                {
                    Node& oldest = origin.idle.front();
                    origin.idle.pop_front();
                    m_lru.erase(m_lru.iterator_to(oldest));
                    retired.push_back(oldest);
                }

                node.origin = &origin;
                origin.idle.push_back(node);
                m_lru.push_back(node); // Released with the current time, so the LRU list stays sorted.
            }
            retired.clear_and_dispose(NodeDisposer{});
        }

      private:
        struct Origin;

        struct Node : ConnectionPoolDetail::IdleHook, ConnectionPoolDetail::LruHook
        {
            PooledConnection<Stream> connection;
            std::chrono::steady_clock::time_point idleSince{};
            Origin* origin = nullptr;
        };

        using IdleList = boost::intrusive::list<Node,
            boost::intrusive::base_hook<ConnectionPoolDetail::IdleHook>,
            boost::intrusive::constant_time_size<true>>;
        using LruList = boost::intrusive::list<Node,
            boost::intrusive::base_hook<ConnectionPoolDetail::LruHook>,
            boost::intrusive::constant_time_size<false>>;

        struct Origin
        {
            IdleList idle;
            const ConnectionKey<Stream>* key = nullptr;
        };

        struct NodeDisposer
        {
            void operator()(Node* node) const noexcept
            {
                delete node;
            }
        };

        // Unlinks every connection that has been idle past the timeout and hands it to the caller
        // for destruction outside the lock. The LRU list is ordered oldest-first, so the walk stops
        // at the first live node.
        void DropExpired(std::chrono::steady_clock::time_point now, LruList& retired)
        {
            while (!m_lru.empty())
            {
                Node& node = m_lru.front();
                if (now - node.idleSince <= m_idleTimeout)
                {
                    break;
                }
                m_lru.pop_front();
                Origin& origin = *node.origin;
                origin.idle.erase(origin.idle.iterator_to(node));
                if (origin.idle.empty())
                {
                    m_origins.erase(*origin.key);
                }
                retired.push_back(node);
            }
        }

        Node* Obtain()
        {
            if (m_recycled.empty())
            {
                return new Node();
            }
            Node& node = m_recycled.back();
            m_recycled.pop_back();
            return &node;
        }

        void Recycle(Node& node)
        {
            node.connection = PooledConnection<Stream>{};
            node.origin = nullptr;
            if (m_recycled.size() >= MaxRecycledNodes)
            {
                delete &node;
                return;
            }
            m_recycled.push_back(node);
        }

        static constexpr std::size_t MaxRecycledNodes = 32;

        std::size_t m_maxIdlePerKey;
        std::chrono::seconds m_idleTimeout;
        std::mutex m_mutex;
        boost::unordered_node_map<ConnectionKey<Stream>, Origin, ConnectionKeyHash<Stream>, ConnectionKeyEqual<Stream>>
            m_origins;
        LruList m_lru;
        IdleList m_recycled;
    };
} // namespace AVEVA::Private
