// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "StreamTypes.hpp"

#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace AVEVA::Private {
/// <summary>
/// Short-lived cache of successful resolutions, so a burst of fresh connections to one origin does a single
/// lookup. The TTL stays small because a long-lived connection is kept by a pool rather than here; a stale
/// address only costs a failed connect on the next request. Failures are never cached.
///
/// It also collapses concurrent lookups for one origin: the first caller leads the resolve and the rest
/// queue up for its outcome (see <see cref="JoinOrLead"/> and <see cref="Finish"/>). All operations are
/// thread-safe.
/// </summary>
/// <typeparam name="Clock">The clock entries expire against.</typeparam>
/// <typeparam name="Waiter">What receives the outcome of a lookup led by another caller; must be movable
/// and callable as <c>(boost::system::error_code, Tcp::resolver::results_type)</c>. A template parameter
/// rather than a type-erased function, because each owner has exactly one kind of waiter.</typeparam>
template <typename Clock, typename Waiter> class BasicDnsCache {
  public:
    /// <summary>
    /// How long a cached resolution stays usable.
    /// </summary>
    static constexpr std::chrono::seconds DefaultTtl{5};

    /// <summary>
    /// The cap on cached origins; reaching it drops expired entries, and clears the cache if that was not
    /// enough.
    /// </summary>
    static constexpr std::size_t MaxEntries = 256;

    /// <summary>
    /// Constructs a cache whose entries live for a given time.
    /// </summary>
    /// <param name="ttl">How long a cached resolution stays usable.</param>
    explicit BasicDnsCache(std::chrono::seconds ttl = DefaultTtl) : m_ttl(ttl) {}

    /// <summary>
    /// Looks up a cached resolution.
    /// </summary>
    /// <param name="host">The host to look up.</param>
    /// <param name="service">The port or service name to look up.</param>
    /// <returns>The cached endpoints, or nullopt if there is no entry or it has expired.</returns>
    std::optional<Tcp::resolver::results_type> Find(std::string_view host, std::string_view service) {
        std::lock_guard<std::mutex> lock(m_mutex);
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

    /// <summary>
    /// Caches a resolution for the cache's TTL. An empty result set is ignored.
    /// </summary>
    /// <param name="host">The host that was resolved.</param>
    /// <param name="service">The port or service name that was resolved.</param>
    /// <param name="results">The endpoints to cache.</param>
    void Store(std::string_view host, std::string_view service, const Tcp::resolver::results_type& results) {
        if (results.empty()) {
            return;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto now = Clock::now();
        if (m_entries.size() >= MaxEntries) {
            std::erase_if(m_entries, [now](const auto& entry) { return now >= entry.second.expires; });
            if (m_entries.size() >= MaxEntries) {
                m_entries.clear();
            }
        }
        m_entries.insert_or_assign(Key{std::string(host), std::string(service)}, Entry{results, now + m_ttl});
    }

    /// <summary>
    /// The process-wide cache, so every client shares one set of resolutions and one in-flight lookup per
    /// origin.
    /// </summary>
    /// <returns>The shared cache.</returns>
    static BasicDnsCache& Shared() {
        static BasicDnsCache cache;
        return cache;
    }

    /// <summary>
    /// Either claims the lookup for this origin or joins one already in flight.
    /// </summary>
    /// <param name="host">The host to resolve.</param>
    /// <param name="service">The port or service name to resolve.</param>
    /// <param name="waiter">Queued to receive the outcome when the caller is not the leader; left alone
    /// otherwise.</param>
    /// <param name="scope">Identifies who may share a lookup, normally the caller's io_context. Callers with
    /// different scopes never join each other, so a leader whose io_context stops cannot strand requests that
    /// run elsewhere. Cached results are still shared across scopes.</param>
    /// <returns>True when the caller must perform the lookup and then call <see cref="Finish"/>.</returns>
    bool JoinOrLead(std::string_view host, std::string_view service, Waiter& waiter, const void* scope = nullptr) {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto [it, inserted] = m_pending.try_emplace(Key{std::string(host), std::string(service), scope});
        if (inserted) {
            return true;
        }
        it->second.push_back(std::move(waiter));
        return false;
    }

    /// <summary>
    /// Caches a successful outcome and publishes the leader's outcome to every queued waiter. Must be called
    /// exactly once per leader, including when the lookup failed or was abandoned.
    /// </summary>
    /// <param name="host">The host that was resolved.</param>
    /// <param name="service">The port or service name that was resolved.</param>
    /// <param name="error">The outcome of the lookup; a failure is published but not cached.</param>
    /// <param name="results">The endpoints found, if any.</param>
    /// <param name="scope">The scope the leader passed to <see cref="JoinOrLead"/>.</param>
    void Finish(std::string_view host, std::string_view service, boost::system::error_code error,
                const Tcp::resolver::results_type& results, const void* scope = nullptr) {
        if (!error) {
            Store(host, service, results);
        }
        std::vector<Waiter> waiters;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_pending.find(KeyView{host, service, scope});
            if (it != m_pending.end()) {
                waiters = std::move(it->second);
                m_pending.erase(it);
            }
        }
        for (auto& waiter : waiters) {
            waiter(error, results);
        }
    }

  private:
    /// <summary>
    /// A cached resolution and the point in time it stops being usable.
    /// </summary>
    struct Entry {
        Tcp::resolver::results_type results;
        typename Clock::time_point expires;
    };

    /// <summary>
    /// The owning (host, service) key stored in the maps.
    /// </summary>
    struct Key {
        std::string host;
        std::string service;
        const void* scope = nullptr; // Only used by pending lookups; cached entries are shared.
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
    /// Transparent hash over either key form, so a <see cref="KeyView"/> can probe a map of
    /// <see cref="Key"/>.
    /// </summary>
    struct KeyHash {
        using is_transparent = void;

        static std::size_t Combine(std::string_view host, std::string_view service, const void* scope) noexcept {
            std::size_t seed = std::hash<std::string_view>{}(host);
            seed ^= std::hash<std::string_view>{}(service) + 0x9e3779b9U + (seed << 6) + (seed >> 2);
            seed ^= std::hash<const void*>{}(scope) + 0x9e3779b9U + (seed << 6) + (seed >> 2);
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

    std::chrono::seconds m_ttl;
    std::mutex m_mutex;
    std::unordered_map<Key, Entry, KeyHash, KeyEqual> m_entries;
    std::unordered_map<Key, std::vector<Waiter>, KeyHash, KeyEqual> m_pending;
};
} // namespace AVEVA::Private
