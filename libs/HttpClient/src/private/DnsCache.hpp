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

namespace AVEVA::Private
{
    // Short-lived cache of successful resolutions, so a burst of fresh connections to one origin does a single
    // lookup. The TTL stays small because the pool, not this cache, is what keeps long-lived connections; a stale
    // address only costs a failed connect on the next request. Failures are never cached.
    template <typename Clock = std::chrono::steady_clock> class BasicDnsCache
    {
      public:
        static constexpr std::chrono::seconds DefaultTtl{5};
        static constexpr std::size_t MaxEntries = 256;

        explicit BasicDnsCache(std::chrono::seconds ttl = DefaultTtl) : m_ttl(ttl)
        {
        }

        std::optional<Tcp::resolver::results_type> Find(std::string_view host, std::string_view service)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_entries.find(KeyView{host, service});
            if (it == m_entries.end())
            {
                return std::nullopt;
            }
            if (Clock::now() >= it->second.expires)
            {
                m_entries.erase(it);
                return std::nullopt;
            }
            return it->second.results;
        }

        void Store(std::string_view host, std::string_view service, const Tcp::resolver::results_type& results)
        {
            if (results.empty())
            {
                return;
            }
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto now = Clock::now();
            if (m_entries.size() >= MaxEntries)
            {
                std::erase_if(m_entries, [now](const auto& entry) { return now >= entry.second.expires; });
                if (m_entries.size() >= MaxEntries)
                {
                    m_entries.clear();
                }
            }
            m_entries.insert_or_assign(Key{std::string(host), std::string(service)}, Entry{results, now + m_ttl});
        }

        static BasicDnsCache& Shared()
        {
            static BasicDnsCache cache;
            return cache;
        }

        using Waiter = std::move_only_function<void(boost::system::error_code, Tcp::resolver::results_type)>;

        // Returns true when the caller must perform the lookup (and then call Finish). Otherwise the lookup is
        // already in flight and `waiter` has been queued to receive its outcome.
        bool JoinOrLead(std::string_view host, std::string_view service, Waiter& waiter)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto [it, inserted] = m_pending.try_emplace(Key{std::string(host), std::string(service)});
            if (inserted)
            {
                return true;
            }
            it->second.push_back(std::move(waiter));
            return false;
        }

        // Publishes the leader's outcome to every queued waiter. Must be called exactly once per leader.
        void Finish(std::string_view host,
            std::string_view service,
            boost::system::error_code error,
            const Tcp::resolver::results_type& results)
        {
            if (!error)
            {
                Store(host, service, results);
            }
            std::vector<Waiter> waiters;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                const auto it = m_pending.find(KeyView{host, service});
                if (it != m_pending.end())
                {
                    waiters = std::move(it->second);
                    m_pending.erase(it);
                }
            }
            for (auto& waiter : waiters)
            {
                waiter(error, results);
            }
        }

      private:
        struct Entry
        {
            Tcp::resolver::results_type results;
            typename Clock::time_point expires;
        };

        struct Key
        {
            std::string host;
            std::string service;
        };

        // Lookups use views so a hit does not build a temporary key string.
        struct KeyView
        {
            std::string_view host;
            std::string_view service;
        };

        struct KeyHash
        {
            using is_transparent = void;

            static std::size_t Combine(std::string_view host, std::string_view service) noexcept
            {
                std::size_t seed = std::hash<std::string_view>{}(host);
                seed ^= std::hash<std::string_view>{}(service) + 0x9e3779b9U + (seed << 6) + (seed >> 2);
                return seed;
            }
            std::size_t operator()(const Key& key) const noexcept
            {
                return Combine(key.host, key.service);
            }
            std::size_t operator()(const KeyView& key) const noexcept
            {
                return Combine(key.host, key.service);
            }
        };

        struct KeyEqual
        {
            using is_transparent = void;

            static KeyView View(const Key& key) noexcept
            {
                return {key.host, key.service};
            }
            static KeyView View(const KeyView& key) noexcept
            {
                return key;
            }
            template <typename L, typename R> bool operator()(const L& lhs, const R& rhs) const noexcept
            {
                const auto left = View(lhs);
                const auto right = View(rhs);
                return left.host == right.host && left.service == right.service;
            }
        };

        std::chrono::seconds m_ttl;
        std::mutex m_mutex;
        std::unordered_map<Key, Entry, KeyHash, KeyEqual> m_entries;
        std::unordered_map<Key, std::vector<Waiter>, KeyHash, KeyEqual> m_pending;
    };

    using DnsCache = BasicDnsCache<>;
} // namespace AVEVA::Private
