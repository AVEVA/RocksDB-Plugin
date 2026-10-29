// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "StreamTypes.hpp"
#include "SweeperNotifier.hpp"
#include <boost/asio/post.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/scope/scope_exit.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <openssl/ssl.h>

#include "Timeouts.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <gsl/pointers>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
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

    /// <summary>Hashes the key by combining its host and service; found by boost::hash via ADL.</summary>
    friend std::size_t hash_value(const ConnectionKey& key) noexcept {
        std::size_t seed = 0;
        boost::hash_combine(seed, key.host);
        boost::hash_combine(seed, key.service);
        return seed;
    }
};

/// <summary>
/// Buffers up to this capacity are kept as-is when a connection is pooled so the next response on it does
/// not have to reallocate; larger ones are released so an idle connection does not pin a big response.
/// Kept small because up to maxTotalIdle connections may each hold one.
/// </summary>
inline constexpr std::size_t MaxRetainedBufferCapacity = 16 * 1024;

/// <summary>
/// Capacity reserved before reading a response (one TLS record) so large bodies need few reads.
/// Must not exceed <see cref="MaxRetainedBufferCapacity"/> or pooled connections would drop the buffer.
/// </summary>
inline constexpr std::size_t ReadChunk = 16 * 1024;
static_assert(ReadChunk <= MaxRetainedBufferCapacity);

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
struct DormantTag;

// normal_link keeps the hooks pointer-sized and makes list::clear() O(1); the pool never
// needs to ask a node whether it is currently linked.
using IdleHook = intrusive::list_base_hook<intrusive::tag<IdleTag>, intrusive::link_mode<intrusive::normal_link>>;
using LruHook = intrusive::list_base_hook<intrusive::tag<LruTag>, intrusive::link_mode<intrusive::normal_link>>;
using DormantHook = intrusive::list_base_hook<intrusive::tag<DormantTag>, intrusive::link_mode<intrusive::normal_link>>;
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
/// All public operations are thread-safe. Retired sockets are always destroyed outside the lock,
/// on their own executors.
/// </summary>
template <typename Stream, typename Clock = std::chrono::steady_clock, typename Notifier = SweeperNotifier>
class ConnectionPool {
    // Release has taken ownership of the connection by the time it notifies, so notifying must not fail.
    static_assert(std::is_nothrow_invocable_v<const Notifier&>, "The pool notifier must not throw");

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
    /// The default for how long a connection must have been idle before it is probed for liveness on reuse.
    /// A connection idle for less than this is handed out without a probe, since the peer has had no real
    /// chance to close it.
    /// </summary>
    static constexpr std::chrono::milliseconds DefaultProbeThreshold{50};

    /// <summary>
    /// Constructs a pool of idle keep-alive connections.
    /// </summary>
    /// <param name="maxIdlePerKey">How many idle connections to keep per origin; 0 disables pooling.</param>
    /// <param name="idleTimeout">How long a connection may sit idle before it is closed; clamped to
    /// <see cref="MaxIdleTimeout"/>.</param>
    /// <param name="maxTotalIdle">How many idle connections to keep across all origins; 0 disables pooling.</param>
    /// <param name="notifier">Invoked (outside the lock) when a release makes an empty pool non-empty, so an
    /// owner can schedule Sweep().</param>
    /// <param name="probeThreshold">Connections idle at least this long are probed for liveness when taken;
    /// zero probes every reused connection.</param>
    ConnectionPool(std::size_t maxIdlePerKey, std::chrono::milliseconds idleTimeout,
                   std::size_t maxTotalIdle = DefaultMaxTotalIdle, Notifier notifier = {},
                   std::chrono::milliseconds probeThreshold = DefaultProbeThreshold)
        : m_maxIdlePerKey(maxIdlePerKey),
          m_idleTimeout(std::clamp(idleTimeout, std::chrono::milliseconds::zero(),
                                   std::chrono::duration_cast<std::chrono::milliseconds>(MaxIdleTimeout))),
          m_maxTotalIdle(maxTotalIdle), m_notifier(std::move(notifier)),
          m_probeThreshold(std::max(probeThreshold, std::chrono::milliseconds::zero())) {}

    /// <summary>
    /// Whole-second timeouts, including values (such as seconds::max()) that would overflow if converted to
    /// milliseconds directly; they are clamped first.
    /// </summary>
    ConnectionPool(std::size_t maxIdlePerKey, std::chrono::seconds idleTimeout,
                   std::size_t maxTotalIdle = DefaultMaxTotalIdle, Notifier notifier = {},
                   std::chrono::milliseconds probeThreshold = DefaultProbeThreshold)
        : ConnectionPool(maxIdlePerKey,
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::clamp(idleTimeout, std::chrono::seconds::zero(), MaxIdleTimeout)),
                         maxTotalIdle, std::move(notifier), probeThreshold) {}

    /// <summary>
    /// Closes connections idle past the timeout without needing a request to arrive. Runs on the sweeper's
    /// thread, so the expired sockets are handed to their own executor to be destroyed there, where no
    /// operation can be using them and the io_context's thread owns their teardown.
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
        retired.clear_and_dispose(ExecutorDisposer{});
        return remaining;
    }

    /// <summary>
    /// The number of idle connections currently held across every origin.
    /// </summary>
    /// <returns>The idle connection count.</returns>
    std::size_t IdleCount() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_lru.size();
    }

    /// <summary>
    /// The number of origin entries currently retained, including dormant ones that hold no connection.
    /// </summary>
    /// <returns>The origin entry count.</returns>
    std::size_t OriginCount() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_origins.size();
    }

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    ~ConnectionPool() { Clear(); }

    /// <summary>
    /// Closes the pool for good and disposes every idle connection. Idle streams are handed to their own executor
    /// like Sweep does: SSL streams must not be torn down on whichever thread happens to call this. After this,
    /// Release declines every connection, so a request that was in flight during shutdown closes its connection
    /// itself instead of parking it where it would be destroyed after its executor is gone.
    /// </summary>
    void Clear() {
        LruList retired;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
            m_dormant.clear();
            m_origins.clear();
            retired.swap(m_lru);
            m_recycled.clear_and_dispose(NodeDisposer{});
        }
        retired.clear_and_dispose(ExecutorDisposer{});
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
            if (idleFor < m_probeThreshold || IsAlive(*connection)) {
                return connection;
            }
            DisposeConnection(std::move(*connection));
        }
        return std::nullopt;
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
            if (it != m_origins.end() && !it->second.idle.empty()) {
                // DropExpired has already removed every timed-out node, so this list holds only usable
                // connections. A dormant origin (empty list) has nothing to hand out.
                Origin& origin = it->second;
                Node& node = origin.idle.back(); // Newest, and least likely to have been closed by the peer.
                origin.idle.pop_back();
                idleFor = now - node.idleSince;
                m_lru.erase(m_lru.iterator_to(node));
                connection.emplace(std::move(node.connection));
                Recycle(node);
                if (origin.idle.empty()) {
                    MakeDormant(origin);
                }
            }
        }
        // Closing the retired sockets runs outside the lock, on their own executors.
        retired.clear_and_dispose(ExecutorDisposer{});
        return connection;
    }

    /// <summary>True unless pooling is disabled, in which case Release never takes a connection.</summary>
    [[nodiscard]] bool Enabled() const noexcept { return m_maxIdlePerKey != 0 && m_maxTotalIdle != 0; }

    /// <summary>
    /// Returns a finished connection to the pool for reuse, evicting the oldest idle connection of the
    /// origin, and then of the pool, if either cap is exceeded. Does nothing when pooling is disabled or
    /// the connection holds no stream. Strongly exception-safe: if a node or map entry cannot be allocated
    /// the pool is left unchanged and the connection stays with the caller.
    /// </summary>
    /// <param name="key">The origin the connection belongs to.</param>
    /// <param name="connection">The connection and its read buffer; moved from only when the pool takes it, so
    /// otherwise the caller still owns it and can close it gracefully.</param>
    /// <returns>True when the pool took ownership of the connection.</returns>
    bool Release(const ConnectionKey<Stream>& key, PooledConnection<Stream>&& connection) {
        if (m_maxIdlePerKey == 0 || m_maxTotalIdle == 0 || !connection.stream) {
            return false;
        }
        LruList retired;
        // Closes retired sockets after the lock is released on every path, including a throwing allocation.
        const boost::scope::scope_exit disposeRetired{[&retired] { retired.clear_and_dispose(ExecutorDisposer{}); }};
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed) {
                return false;
            }
            const auto now = Clock::now();
            DropExpired(now, retired);

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
                // unordered_node_map keeps elements at stable addresses (but not iterators), so the origin
                // remembers its own key for a fresh lookup when it is erased.
                origin.key = &it->first;
            } else if (origin.dormant) {
                m_dormant.erase(m_dormant.iterator_to(origin));
                origin.dormant = false;
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
                    EraseOrigin(oldestOrigin);
                }
                retired.push_back(oldest);
            }
        }
        // Every successful release notifies, not just the empty-to-nonempty one: a failed schedule leaves the
        // pending flag clear while the pool stays nonempty, and only a later release can recover the sweep.
        m_notifier();
        return true;
    }

  private:
    /// <summary>
    /// Destroys a connection on its stream's own executor when the stream has one, so an I/O object is never torn
    /// down on whichever thread happened to call into the pool. Falls back to destroying it in place if posting
    /// fails.
    /// </summary>
    /// <param name="connection">The connection to destroy.</param>
    static void DisposeConnection(PooledConnection<Stream>&& connection) noexcept {
        if constexpr (requires { connection.stream->get_executor(); }) {
            if (connection.stream) {
                auto executor = connection.stream->get_executor();
                try {
                    boost::asio::post(
                        executor, [moved = std::move(connection)]() mutable { moved = PooledConnection<Stream>{}; });
                } catch (...) {
                    // The handler (and the connection in it) is destroyed right here instead.
                }
            }
        }
    }

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
                // EAGAIN == EWOULDBLOCK on Linux, so comparing both trips -Wlogical-op.
                return received < 0 && (errno == EAGAIN
#if EAGAIN != EWOULDBLOCK
                                        || errno == EWOULDBLOCK
#endif
                                       );
#endif
            } catch (...) {
                return false;
            }
    }

    struct Origin;

    using OriginMap =
        boost::unordered_node_map<ConnectionKey<Stream>, Origin, boost::hash<ConnectionKey<Stream>>, std::equal_to<>>;

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
    /// One origin's idle connections, oldest first, plus a pointer to its own map key so a node can find
    /// and erase its origin (the key lives inside the element, whose address is stable).
    /// </summary>
    struct Origin : ConnectionPoolDetail::DormantHook {
        IdleList idle;
        const ConnectionKey<Stream>* key = nullptr;
        bool dormant = false;
    };

    using DormantList = boost::intrusive::list<Origin, boost::intrusive::base_hook<ConnectionPoolDetail::DormantHook>,
                                               boost::intrusive::constant_time_size<true>>;

    /// <summary>
    /// Keeps an origin whose last connection was just taken, so a serial request stream releasing that
    /// connection again finds the map entry (and its key) instead of allocating them anew. Only the newest
    /// <see cref="MaxDormantOrigins"/> are kept so touching many hosts cannot grow the map without bound.
    /// </summary>
    /// <param name="origin">The origin, still in the map, whose idle list is now empty.</param>
    void MakeDormant(Origin& origin) {
        origin.dormant = true;
        m_dormant.push_back(origin);
        if (m_dormant.size() > MaxDormantOrigins) {
            Origin& oldest = m_dormant.front();
            m_dormant.pop_front();
            oldest.dormant = false;
            EraseOrigin(oldest);
        }
    }

    /// <summary>
    /// How many empty origin entries are retained for reuse before the oldest is erased.
    /// </summary>
    static constexpr std::size_t MaxDormantOrigins = 32;

    /// <summary>
    /// Erases an origin's map entry. Map iterators do not survive a rehash, so the entry is looked up afresh;
    /// the origin's key pointer stays valid because the map keeps elements at stable addresses.
    /// </summary>
    /// <param name="origin">The origin, still in the map, to erase.</param>
    void EraseOrigin(const Origin& origin) {
        const auto it = m_origins.find(*origin.key);
        m_origins.erase(it);
    }

    /// <summary>
    /// Destroys a node (and with it its socket) when an intrusive list is cleared.
    /// </summary>
    struct NodeDisposer {
        void operator()(gsl::owner<Node*> node) const noexcept { delete node; }
    };

    /// <summary>
    /// Destroys a node like <see cref="NodeDisposer"/>, but moves its connection to the stream's own executor
    /// first when the stream has one. Falls back to destroying it in place if posting fails.
    /// </summary>
    struct ExecutorDisposer {
        void operator()(gsl::owner<Node*> node) const noexcept {
            DisposeConnection(std::move(node->connection));
            delete node;
        }
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
                EraseOrigin(origin);
            }
            retired.push_back(node);
        }
    }

    /// <summary>
    /// Returns a node to hold a connection, reusing a retired one when possible. May throw.
    /// </summary>
    /// <returns>An unlinked node owned by the pool.</returns>
    gsl::owner<Node*> Obtain() {
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

    std::size_t m_maxIdlePerKey;
    std::chrono::milliseconds m_idleTimeout;
    std::size_t m_maxTotalIdle;
    const Notifier m_notifier;
    std::chrono::milliseconds m_probeThreshold;
    mutable std::mutex m_mutex;
    bool m_closed = false; // Guarded by m_mutex.
    OriginMap m_origins;
    DormantList m_dormant;
    LruList m_lru;
    IdleList m_recycled;
};
} // namespace AVEVA::Private
