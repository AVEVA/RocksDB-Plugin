// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "StreamTypes.hpp"
#include "SweeperNotifier.hpp"
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <openssl/ssl.h>

#include "Timeouts.hpp"

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#if !defined(_WIN32)
#include <cerrno>
#include <sys/socket.h>
#else
#include <boost/asio/detail/socket_types.hpp>
#endif

namespace AVEVA::Private {
/// <summary>
/// Identifies the (host, service) origin a pooled connection belongs to. Parameterized on Stream so a key is
/// only ever valid for the ConnectionPool&lt;Stream&gt; it was created for; use it via the
/// BasicConnectionKey/TlsConnectionKey aliases below rather than naming ConnectionKey&lt;Stream&gt; directly.
/// </summary>
template <typename Stream> struct ConnectionKey {
    std::string host;
    std::string service;

    bool operator==(const ConnectionKey& other) const noexcept {
        return host == other.host && service == other.service;
    }
};

/// <summary>
/// Hashes a <see cref="ConnectionKey"/> by combining its host and service.
/// </summary>
template <typename Stream> struct ConnectionKeyHash {
    std::size_t operator()(const ConnectionKey<Stream>& key) const noexcept {
        std::size_t seed = std::hash<std::string>{}(key.host);
        seed ^= std::hash<std::string>{}(key.service) + 0x9e3779b9U + (seed << 6) + (seed >> 2);
        return seed;
    }
};

/// <summary>
/// Compares two <see cref="ConnectionKey"/> values for equality.
/// </summary>
template <typename Stream> struct ConnectionKeyEqual {
    bool operator()(const ConnectionKey<Stream>& lhs, const ConnectionKey<Stream>& rhs) const noexcept {
        return lhs == rhs;
    }
};

/// <summary>
/// Buffers up to this capacity are kept as-is when a connection is pooled so the next response on it does
/// not have to reallocate; larger ones are released so an idle connection does not pin a big response.
/// </summary>
inline constexpr std::size_t MaxRetainedBufferCapacity = 64 * 1024;

/// <summary>
/// Releases a buffer's memory if it grew past <see cref="MaxRetainedBufferCapacity"/>.
/// </summary>
/// <param name="buffer">The buffer of a connection about to be pooled.</param>
inline void TrimIdleBuffer(beast::flat_buffer& buffer) {
    if (buffer.capacity() > MaxRetainedBufferCapacity) {
        buffer.shrink_to_fit();
    }
}

/// <summary>
/// A connection held in (or taken from) the pool, together with the read buffer that belongs to it, which
/// may still hold bytes the stream has read.
/// </summary>
template <typename Stream> struct PooledConnection {
    std::unique_ptr<Stream> stream;
    beast::flat_buffer buffer;
};

/// <summary>
/// The key type for a pool of plain HTTP connections.
/// </summary>
using BasicConnectionKey = ConnectionKey<PlainStream>;

/// <summary>
/// The key type for a pool of HTTPS connections.
/// </summary>
using TlsConnectionKey = ConnectionKey<TlsStream>;

namespace ConnectionPoolDetail {
namespace intrusive = boost::intrusive;

struct IdleTag;
struct LruTag;

// normal_link keeps the hooks pointer-sized and makes list::clear() O(1); the pool never
// needs to ask a node whether it is currently linked.
using IdleHook = intrusive::list_base_hook<intrusive::tag<IdleTag>, intrusive::link_mode<intrusive::normal_link>>;
using LruHook = intrusive::list_base_hook<intrusive::tag<LruTag>, intrusive::link_mode<intrusive::normal_link>>;
} // namespace ConnectionPoolDetail

/// <summary>
/// Caches idle keep-alive connections per (scheme, host, port) so repeated requests to the
/// same origin can skip DNS resolution, TCP connect, and (for HTTPS) the TLS handshake.
///
/// Every cached connection lives in a node that is threaded onto two intrusive lists at once:
/// the per-origin idle list that Acquire searches, and a single pool-wide LRU list ordered
/// oldest-first that makes expiry a bounded walk from the front instead of a per-origin scan.
/// Because the lists are intrusive, linking, unlinking, and moving a connection between them
/// costs a few pointer writes and never allocates, and retired nodes are recycled rather than
/// freed so a steady request stream performs no node allocation at all.
///
/// All public operations are thread-safe. Retired sockets are always destroyed outside the lock.
/// </summary>
template <typename Stream, typename Clock = std::chrono::steady_clock, typename Notifier = SweeperNotifier>
class ConnectionPool {
  public:
    /// <summary>
    /// Bounds idle sockets across all origins, so a client that talks to many hosts cannot hold them
    /// unboundedly.
    /// </summary>
    static constexpr std::size_t DefaultMaxTotalIdle = 256;

    /// <summary>
    /// The largest idle timeout accepted; larger values would overflow when converted to the clock's
    /// nanosecond ticks, so they are clamped to this.
    /// </summary>
    static constexpr std::chrono::seconds MaxIdleTimeout{EffectivelyInfinite};

    /// <summary>
    /// Constructs a pool of idle keep-alive connections.
    /// </summary>
    /// <param name="maxIdlePerKey">How many idle connections to keep per origin; 0 disables pooling.</param>
    /// <param name="idleTimeout">How long a connection may sit idle before it is closed; clamped to
    /// <see cref="MaxIdleTimeout"/>.</param>
    /// <param name="maxTotalIdle">How many idle connections to keep across all origins; 0 disables pooling.</param>
    ConnectionPool(std::size_t maxIdlePerKey, std::chrono::seconds idleTimeout,
                   std::size_t maxTotalIdle = DefaultMaxTotalIdle)
        : m_maxIdlePerKey(maxIdlePerKey),
          m_idleTimeout(std::clamp(idleTimeout, std::chrono::seconds::zero(), MaxIdleTimeout)),
          m_maxTotalIdle(maxTotalIdle) {}

    /// <summary>
    /// Called (outside the lock) when a release makes an empty pool non-empty, so an owner can schedule
    /// Sweep().
    /// </summary>
    /// <param name="notifier">The notification to invoke; replaces any previous one.</param>
    void SetNotifier(Notifier notifier) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_notifier = std::move(notifier);
        m_hasNotifier = true;
    }

    /// <summary>
    /// Closes connections idle past the timeout without needing a request to arrive.
    /// </summary>
    /// <returns>How many idle connections remain.</returns>
    std::size_t Sweep() {
        LruList retired;
        std::size_t remaining = 0;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            DropExpired(Clock::now(), retired);
            remaining = m_lru.size();
        }
        retired.clear_and_dispose(NodeDisposer{});
        return remaining;
    }

    /// <summary>
    /// The number of idle connections currently held across every origin.
    /// </summary>
    /// <returns>The idle connection count.</returns>
    std::size_t IdleCount() {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_lru.size();
    }

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    ~ConnectionPool() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_origins.clear();
        m_lru.clear_and_dispose(NodeDisposer{});
        m_recycled.clear_and_dispose(NodeDisposer{});
    }

    /// <summary>
    /// Returns a connection the peer has not closed. A server may drop an idle keep-alive socket at any
    /// time, and a non-idempotent request sent on it could not be retried safely, so dead ones are discarded
    /// here.
    /// </summary>
    /// <param name="key">The origin to take a connection for.</param>
    /// <returns>A usable connection, or nullopt when the origin has none.</returns>
    std::optional<PooledConnection<Stream>> Acquire(const ConnectionKey<Stream>& key) {
        typename Clock::duration idleFor{};
        while (auto connection = AcquireAged(key, idleFor)) {
            // A connection returned moments ago has not had time to be closed by the peer; a request that does
            // hit a just-closed socket is retried by the caller where that is safe.
            if (idleFor < SkipProbeBelow || IsAlive(*connection)) {
                return connection;
            }
        }
        return std::nullopt;
    }

    /// <summary>
    /// Like <see cref="Acquire"/>, but without probing whether the peer has closed the connection.
    /// </summary>
    /// <param name="key">The origin to take a connection for.</param>
    /// <returns>The newest idle connection, or nullopt when the origin has none.</returns>
    std::optional<PooledConnection<Stream>> AcquireUnchecked(const ConnectionKey<Stream>& key) {
        typename Clock::duration idleFor{};
        return AcquireAged(key, idleFor);
    }

    /// <summary>
    /// Removes the newest idle connection for an origin without probing it. Expired connections are dropped
    /// first, so whatever is returned was within the idle timeout.
    /// </summary>
    /// <param name="key">The origin to take a connection for.</param>
    /// <param name="idleFor">Set to how long the returned connection had been idle; untouched when there is
    /// none.</param>
    /// <returns>The newest idle connection, or nullopt when the origin has none.</returns>
    std::optional<PooledConnection<Stream>> AcquireAged(const ConnectionKey<Stream>& key,
                                                        typename Clock::duration& idleFor) {
        LruList retired;
        std::optional<PooledConnection<Stream>> connection;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto now = Clock::now();
            DropExpired(now, retired);
            auto it = m_origins.find(key);
            if (it != m_origins.end()) {
                // Origins are erased as soon as they run dry and DropExpired has already
                // removed every timed-out node, so this list holds only usable connections.
                Origin& origin = it->second;
                Node& node = origin.idle.back(); // Newest, and least likely to have been closed by the peer.
                origin.idle.pop_back();
                idleFor = now - node.idleSince;
                m_lru.erase(m_lru.iterator_to(node));
                connection.emplace(std::move(node.connection));
                Recycle(node);
                if (origin.idle.empty()) {
                    m_origins.erase(it);
                }
            }
        }
        // Closing the retired sockets runs outside the lock.
        retired.clear_and_dispose(NodeDisposer{});
        return connection;
    }

    /// <summary>
    /// Returns a finished connection to the pool for reuse, evicting the oldest idle connection of the
    /// origin, and then of the pool, if either cap is exceeded. Does nothing when pooling is disabled or
    /// the connection holds no stream. Strongly exception-safe: if a node or map entry cannot be allocated
    /// the pool is left unchanged and the connection stays with the caller.
    /// </summary>
    /// <param name="key">The origin the connection belongs to.</param>
    /// <param name="connection">The connection and its read buffer, taken over on success.</param>
    void Release(const ConnectionKey<Stream>& key, PooledConnection<Stream> connection) {
        if (m_maxIdlePerKey == 0 || m_maxTotalIdle == 0 || !connection.stream) {
            return;
        }
        LruList retired;
        // Closes retired sockets after the lock is released on every path, including a throwing allocation.
        struct DisposeOnExit {
            LruList& list;
            ~DisposeOnExit() { list.clear_and_dispose(NodeDisposer{}); }
        } disposeRetired{retired};
        std::optional<Notifier> notify;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto now = Clock::now();
            DropExpired(now, retired);
            const bool wasEmpty = m_lru.empty();

            // Everything that can throw happens before any pool state changes, so a bad_alloc
            // leaves the pool consistent and the socket owned by the caller's (RAII) parameter.
            Node& node = *Obtain();
            std::pair<decltype(m_origins.begin()), bool> emplaced;
            try {
                emplaced = m_origins.try_emplace(key);
            } catch (...) {
                Recycle(node);
                throw;
            }
            auto [it, inserted] = emplaced;
            Origin& origin = it->second;
            if (inserted) {
                // unordered_node_map keeps keys and mapped values at stable addresses, so a
                // node can point back at its origin for O(1) unlinking during expiry.
                origin.key = &it->first;
            } else if (origin.idle.size() >= m_maxIdlePerKey) {
                Node& oldest = origin.idle.front();
                origin.idle.pop_front();
                m_lru.erase(m_lru.iterator_to(oldest));
                retired.push_back(oldest);
            }

            node.connection = std::move(connection);
            node.idleSince = now;
            node.origin = &origin;
            origin.idle.push_back(node);
            m_lru.push_back(node); // Released with the current time, so the LRU list stays sorted.

            // The global cap evicts the oldest idle connection of any origin (never the one just added).
            while (m_lru.size() > m_maxTotalIdle && &m_lru.front() != &node) {
                Node& oldest = m_lru.front();
                m_lru.pop_front();
                Origin& oldestOrigin = *oldest.origin;
                oldestOrigin.idle.erase(oldestOrigin.idle.iterator_to(oldest));
                if (oldestOrigin.idle.empty()) {
                    m_origins.erase(*oldestOrigin.key);
                }
                retired.push_back(oldest);
            }
            if (wasEmpty && m_hasNotifier) {
                notify = m_notifier;
            }
        }
        if (notify) {
            (*notify)();
        }
    }

  private:
    /// <summary>
    /// An idle socket must have nothing to read: pending data or EOF both mean the peer closed or
    /// misbehaved. Stream types without a real socket cannot be probed and are assumed alive.
    /// </summary>
    /// <param name="connection">The idle connection to probe.</param>
    /// <returns>True if the connection still looks usable.</returns>
    static bool IsAlive(PooledConnection<Stream>& connection) noexcept {
        if constexpr (!requires { beast::get_lowest_layer(*connection.stream).socket(); }) {
            return true; // Stream types without a real socket (test doubles) cannot be probed.
        } else
            try {
                auto& socket = beast::get_lowest_layer(*connection.stream).socket();
                if (!socket.is_open()) {
                    return false;
                }
                if constexpr (requires { connection.stream->native_handle(); }) {
                    // Decrypted bytes already buffered by TLS never show up on the socket.
                    if (SSL_pending(connection.stream->native_handle()) > 0) {
                        return false;
                    }
                }
#if defined(_WIN32)
                // A zero-timeout poll avoids toggling the socket's blocking mode.
                WSAPOLLFD pollFd{};
                pollFd.fd = socket.native_handle();
                pollFd.events = POLLRDNORM;
                const int ready = ::WSAPoll(&pollFd, 1, 0);
                return ready == 0;
#else
                // MSG_DONTWAIT makes just this call non-blocking, so the socket's mode is never toggled.
                char probe = 0;
                const auto received = ::recv(socket.native_handle(), &probe, 1, MSG_PEEK | MSG_DONTWAIT);
                return received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
#endif
            } catch (...) {
                return false;
            }
    }

    struct Origin;

    /// <summary>
    /// A cached connection, linked into both its origin's idle list and the pool-wide LRU list.
    /// </summary>
    struct Node : ConnectionPoolDetail::IdleHook, ConnectionPoolDetail::LruHook {
        PooledConnection<Stream> connection;
        typename Clock::time_point idleSince{};
        Origin* origin = nullptr;
    };

    using IdleList = boost::intrusive::list<Node, boost::intrusive::base_hook<ConnectionPoolDetail::IdleHook>,
                                            boost::intrusive::constant_time_size<true>>;
    using LruList = boost::intrusive::list<Node, boost::intrusive::base_hook<ConnectionPoolDetail::LruHook>,
                                           boost::intrusive::constant_time_size<true>>;

    /// <summary>
    /// One origin's idle connections, oldest first, plus a pointer back to the key that owns them so a node
    /// can erase its origin in O(1).
    /// </summary>
    struct Origin {
        IdleList idle;
        const ConnectionKey<Stream>* key = nullptr;
    };

    /// <summary>
    /// Destroys a node (and with it its socket) when an intrusive list is cleared.
    /// </summary>
    struct NodeDisposer {
        void operator()(Node* node) const noexcept { delete node; }
    };

    /// <summary>
    /// Unlinks every connection that has been idle past the timeout and hands it to the caller
    /// for destruction outside the lock. The LRU list is ordered oldest-first, so the walk stops
    /// at the first live node.
    /// </summary>
    /// <param name="now">The current time to measure idleness against.</param>
    /// <param name="retired">Receives the expired nodes, to be disposed of by the caller.</param>
    void DropExpired(typename Clock::time_point now, LruList& retired) {
        while (!m_lru.empty()) {
            Node& node = m_lru.front();
            if (now - node.idleSince <= m_idleTimeout) {
                break;
            }
            m_lru.pop_front();
            Origin& origin = *node.origin;
            origin.idle.erase(origin.idle.iterator_to(node));
            if (origin.idle.empty()) {
                m_origins.erase(*origin.key);
            }
            retired.push_back(node);
        }
    }

    /// <summary>
    /// Returns a node to hold a connection, reusing a retired one when possible. May throw.
    /// </summary>
    /// <returns>An unlinked node owned by the pool.</returns>
    Node* Obtain() {
        if (m_recycled.empty()) {
            return new Node();
        }
        Node& node = m_recycled.back();
        m_recycled.pop_back();
        return &node;
    }

    /// <summary>
    /// Clears a node and keeps it for reuse, freeing it once enough are already held.
    /// </summary>
    /// <param name="node">The node to recycle; must already be unlinked from both lists.</param>
    void Recycle(Node& node) {
        node.connection = PooledConnection<Stream>{};
        node.origin = nullptr;
        if (m_recycled.size() >= MaxRecycledNodes) {
            delete &node;
            return;
        }
        m_recycled.push_back(node);
    }

    /// <summary>
    /// How many retired nodes are kept for reuse before they are freed.
    /// </summary>
    static constexpr std::size_t MaxRecycledNodes = 32;

    /// <summary>
    /// A connection idle for less than this is handed out without a liveness probe, since the peer has had
    /// no real chance to close it.
    /// </summary>
    static constexpr std::chrono::milliseconds SkipProbeBelow{50};

    std::size_t m_maxIdlePerKey;
    std::chrono::seconds m_idleTimeout;
    std::size_t m_maxTotalIdle;
    Notifier m_notifier;
    bool m_hasNotifier = false;
    std::mutex m_mutex;
    boost::unordered_node_map<ConnectionKey<Stream>, Origin, ConnectionKeyHash<Stream>, ConnectionKeyEqual<Stream>>
        m_origins;
    LruList m_lru;
    IdleList m_recycled;
};
} // namespace AVEVA::Private
