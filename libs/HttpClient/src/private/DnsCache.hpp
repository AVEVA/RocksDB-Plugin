// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "DiagnosticsSink.hpp"
#include "StreamTypes.hpp"

#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>

#include <chrono>
#include <cstddef>
#include <gsl/pointers>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace AVEVA::Private {
/// <summary>
/// A resolved endpoint list. Asio's results are already reference counted, so copying one (a cache hit or a
/// publish to a waiter) shares the endpoints instead of duplicating them.
/// </summary>
using ResolveResults = Tcp::resolver::results_type;

/// <summary>
/// What receives the outcome of a lookup led by another caller. It is called from whichever thread finishes the
/// lookup, possibly a destructor, so it must not throw; it is moved around under the cache lock, so moving must not
/// throw either.
/// </summary>
template <typename Waiter>
concept DnsWaiter = std::is_nothrow_move_constructible_v<Waiter> &&
                    std::is_nothrow_invocable_v<Waiter&, boost::system::error_code, const ResolveResults&>;

/// <summary>
/// Short-lived cache of successful resolutions, so a burst of fresh connections to one origin does a single
/// lookup. The TTL stays small because a long-lived connection is kept by a pool rather than here; a stale
/// address only costs a failed connect on the next request. Failures are never cached.
///
/// It also collapses concurrent lookups for one origin: the first caller leads the resolve and the rest
/// queue up for its outcome (see <see cref="Acquire"/>). All operations are thread-safe.
/// </summary>
/// <typeparam name="Clock">The clock entries expire against.</typeparam>
/// <typeparam name="Waiter">What receives the outcome of a lookup led by another caller. A template parameter
/// rather than a type-erased function, because each owner has exactly one kind of waiter.</typeparam>
template <typename Clock, DnsWaiter Waiter> class BasicDnsCache {
    /// <summary>
    /// A cached resolution and the point in time it stops being usable.
    /// </summary>
    struct Entry {
        ResolveResults results;
        typename Clock::time_point expires;
    };

    /// <summary>
    /// Lookups use views so a hit does not build a temporary key string.
    /// </summary>
    struct KeyView {
        std::string_view host;
        std::string_view service;
        const void* scope = nullptr;
    };

    /// <summary>
    /// The owning (host, service) key stored in the maps.
    /// </summary>
    struct Key {
        std::string host;
        std::string service;
        const void* scope = nullptr; // Only used by pending lookups; cached entries are shared.

        explicit Key(const KeyView& view) : host(view.host), service(view.service), scope(view.scope) {}
    };

    /// <summary>
    /// Transparent hash over either key form, so a <see cref="KeyView"/> can probe a map of
    /// <see cref="Key"/>.
    /// </summary>
    struct KeyHash {
        using is_transparent = void;

        static std::size_t Combine(std::string_view host, std::string_view service, const void* scope) noexcept {
            // Both key forms funnel through string_view so owning and view keys hash identically.
            std::size_t seed = 0;
            boost::hash_combine(seed, host);
            boost::hash_combine(seed, service);
            boost::hash_combine(seed, scope);
            return seed;
        }
        std::size_t operator()(const Key& key) const noexcept { return Combine(key.host, key.service, key.scope); }
        std::size_t operator()(const KeyView& key) const noexcept { return Combine(key.host, key.service, key.scope); }
    };

    /// <summary>
    /// Transparent equality over either key form, matching <see cref="KeyHash"/>.
    /// </summary>
    struct KeyEqual {
        using is_transparent = void;

        static KeyView View(const Key& key) noexcept { return {key.host, key.service, key.scope}; }
        static KeyView View(const KeyView& key) noexcept { return key; }
        template <typename L, typename R> bool operator()(const L& lhs, const R& rhs) const noexcept {
            const auto left = View(lhs);
            const auto right = View(rhs);
            return left.host == right.host && left.service == right.service && left.scope == right.scope;
        }
    };

  public:
    /// <summary>
    /// How long a cached resolution stays usable.
    /// </summary>
    static constexpr std::chrono::seconds DefaultTtl{5};

    /// <summary>
    /// The cap on cached origins; reaching it drops expired entries, and evicts one live entry if that was
    /// not enough.
    /// </summary>
    static constexpr std::size_t MaxEntries = 256;

    /// <summary>
    /// Constructs a cache whose entries live for a given time.
    /// </summary>
    /// <param name="ttl">How long a cached resolution stays usable.</param>
    explicit BasicDnsCache(std::chrono::seconds ttl = DefaultTtl) : m_ttl(ttl) {}

    BasicDnsCache(const BasicDnsCache&) = delete;
    BasicDnsCache& operator=(const BasicDnsCache&) = delete;

    /// <summary>A cache hit: the endpoints to use.</summary>
    struct CachedAnswer {
        ResolveResults results;
    };

    /// <summary>The caller's waiter was queued behind a lookup another caller leads.</summary>
    struct JoinedLookup {};

    /// <summary>
    /// The caller leads the lookup for its origin. The token is the obligation to publish the outcome: it must be
    /// consumed by <see cref="Publish"/>, and if it is destroyed instead (the lookup was abandoned), it publishes
    /// operation_aborted so the joined callers are not left waiting.
    /// </summary>
    class LeaderToken {
      public:
        LeaderToken(LeaderToken&& other) noexcept
            : m_cache(std::exchange(other.m_cache, nullptr)), m_key(std::move(other.m_key)),
              m_diagnostics(std::move(other.m_diagnostics)) {}
        LeaderToken& operator=(LeaderToken&&) = delete;
        LeaderToken(const LeaderToken&) = delete;
        LeaderToken& operator=(const LeaderToken&) = delete;
        ~LeaderToken() { std::move(*this).Publish(boost::asio::error::operation_aborted, {}); }

        /// <summary>
        /// Caches a successful outcome and publishes the outcome to every queued waiter. Consumes the token.
        /// </summary>
        /// <param name="error">The outcome of the lookup; a failure is published but not cached.</param>
        /// <param name="results">The endpoints found, if any.</param>
        void Publish(boost::system::error_code error, const ResolveResults& results) && noexcept {
            if (auto* const cache = std::exchange(m_cache, nullptr)) {
                cache->Finish(m_key, error, results, m_diagnostics);
            }
        }

      private:
        friend class BasicDnsCache;
        LeaderToken(BasicDnsCache& cache, Key key, std::shared_ptr<const DiagnosticsSink> diagnostics) noexcept
            : m_cache(&cache), m_key(std::move(key)), m_diagnostics(std::move(diagnostics)) {}

        BasicDnsCache* m_cache;
        Key m_key;
        std::shared_ptr<const DiagnosticsSink> m_diagnostics;
    };

    /// <summary>The decision a lookup reached.</summary>
    using Admission = std::variant<CachedAnswer, JoinedLookup, LeaderToken>;

    /// <summary>
    /// Checks the cache and then joins or leads the lookup in one critical section. Doing the two steps
    /// separately lets a leader publish in between, so a late caller would start a redundant resolve.
    /// </summary>
    /// <param name="host">The host to resolve.</param>
    /// <param name="service">The port or service name to resolve.</param>
    /// <param name="makeWaiter">Called (under the cache lock, so it must be cheap) to produce the waiter only
    /// when the caller joins a lookup in flight.</param>
    /// <param name="scope">Identifies who may share a lookup, normally the caller's io_context. Callers with
    /// different scopes never join each other, so a leader whose io_context stops cannot strand requests that
    /// run elsewhere. Cached results are still shared across scopes.</param>
    /// <param name="diagnostics">Where a leader's failure to cache its answer is reported; null reports
    /// nothing.</param>
    /// <returns>The cached answer, a joined lookup whose waiter will be called, or the token of a lookup the
    /// caller must now perform.</returns>
    template <typename MakeWaiter>
        requires std::is_invocable_r_v<Waiter, MakeWaiter&>
    Admission Acquire(std::string_view host, std::string_view service, MakeWaiter&& makeWaiter, const void* scope,
                      std::shared_ptr<const DiagnosticsSink> diagnostics) {
        const std::lock_guard<std::mutex> lock(m_mutex);
        if (auto cached = FindUnsafe(lock, host, service)) {
            return CachedAnswer{std::move(*cached)};
        }
        // A join probes with a view and allocates no owning key; only the leader materialises one.
        const auto [it, inserted] = m_pending.try_emplace(KeyView{host, service, scope});
        if (!inserted) {
            it->second.push_back(makeWaiter());
            return JoinedLookup{};
        }
        try {
            return LeaderToken(*this, Key(KeyView{host, service, scope}), std::move(diagnostics));
        } catch (...) {
            m_pending.erase(it); // Nobody could ever publish this claim.
            throw;
        }
    }

    /// <summary>
    /// Forgets a cached resolution, so the next request resolves afresh. Called when connecting to a cached
    /// address failed, because the address may be stale.
    /// </summary>
    /// <param name="host">The host whose entry to drop.</param>
    /// <param name="service">The port or service name whose entry to drop.</param>
    void Invalidate(std::string_view host, std::string_view service) {
        const std::lock_guard<std::mutex> lock(m_mutex);
        m_entries.erase(KeyView{host, service});
    }

    /// <summary>
    /// The process-wide cache, so every client shares one set of resolutions and one in-flight lookup per
    /// origin.
    /// </summary>
    /// <returns>The shared cache.</returns>
    static BasicDnsCache& Shared() {
        // Intentionally leaked rather than a function-local static object: a host's static io_context can be
        // destroyed after this translation unit's statics, and destroying its pending handlers runs
        // LeaderToken::~LeaderToken, which must still find a live cache.
        static const gsl::owner<BasicDnsCache*> cache = new BasicDnsCache;
        return *cache;
    }

  private:
    /// <summary>
    /// Caches a successful outcome and hands the outcome to every waiter queued for the leader's key. Caching is an
    /// optimisation: if it fails, the answer must still reach the waiters and the claim must still be released, or
    /// later requests would join a lookup nobody completes.
    /// </summary>
    void Finish(const Key& key, boost::system::error_code error, const ResolveResults& results,
                const std::shared_ptr<const DiagnosticsSink>& diagnostics) noexcept {
        if (!error) {
            try {
                Store(KeyView{key.host, key.service}, results);
            } catch (const std::exception& failure) {
                DiagnosticsSink::Report(diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Resolve,
                                        key.host, key.service, {}, failure.what());
            }
        }
        std::vector<Waiter> waiters;
        {
            const std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_pending.find(KeyEqual::View(key));
            if (it != m_pending.end()) {
                waiters = std::move(it->second);
                m_pending.erase(it);
            }
        }
        for (auto& waiter : waiters) {
            waiter(error, results);
        }
    }

    // Caches a resolution for the cache's TTL; an empty result set is ignored.
    void Store(KeyView key, const ResolveResults& results) {
        if (results.empty()) {
            return;
        }
        const std::lock_guard<std::mutex> lock(m_mutex);
        const auto now = Clock::now();
        // A refresh replaces in place; it must never cost an unrelated entry its slot.
        if (const auto it = m_entries.find(key); it != m_entries.end()) {
            it->second = Entry{results, now + m_ttl};
            return;
        }
        if (m_entries.size() >= MaxEntries) {
            boost::unordered::erase_if(m_entries, [now](const auto& entry) { return now >= entry.second.expires; });
            if (m_entries.size() >= MaxEntries) {
                // Dropping one arbitrary live entry keeps the rest; clearing everything made a burst of
                // origins re-resolve the whole working set (measured: 44% vs 15% lookups just over the cap).
                m_entries.erase(m_entries.begin());
            }
        }
        m_entries.insert_or_assign(Key(key), Entry{results, now + m_ttl});
    }

    std::optional<ResolveResults> FindUnsafe(const std::lock_guard<std::mutex>& /*heldMutex*/, std::string_view host,
                                             std::string_view service) {
        const auto it = m_entries.find(KeyView{host, service});
        if (it == m_entries.end()) {
            return std::nullopt;
        }
        if (Clock::now() >= it->second.expires) {
            m_entries.erase(it);
            return std::nullopt;
        }
        return it->second.results;
    }

    std::chrono::seconds m_ttl;
    std::mutex m_mutex;
    boost::unordered_flat_map<Key, Entry, KeyHash, KeyEqual> m_entries;
    boost::unordered_node_map<Key, std::vector<Waiter>, KeyHash, KeyEqual> m_pending;
};
} // namespace AVEVA::Private
