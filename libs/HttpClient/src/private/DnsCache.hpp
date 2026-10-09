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

        std::optional<Tcp::resolver::results_type> Find(const std::string& host, const std::string& service)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_entries.find(MakeKey(host, service));
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

        void Store(const std::string& host, const std::string& service, const Tcp::resolver::results_type& results)
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
            m_entries.insert_or_assign(MakeKey(host, service), Entry{results, now + m_ttl});
        }

        static BasicDnsCache& Shared()
        {
            static BasicDnsCache cache;
            return cache;
        }

        using Waiter = std::move_only_function<void(boost::system::error_code, Tcp::resolver::results_type)>;

        // Returns true when the caller must perform the lookup (and then call Finish). Otherwise the lookup is
        // already in flight and `waiter` has been queued to receive its outcome.
        bool JoinOrLead(const std::string& host, const std::string& service, Waiter& waiter)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto [it, inserted] = m_pending.try_emplace(MakeKey(host, service));
            if (inserted)
            {
                return true;
            }
            it->second.push_back(std::move(waiter));
            return false;
        }

        // Publishes the leader's outcome to every queued waiter. Must be called exactly once per leader.
        void Finish(const std::string& host,
            const std::string& service,
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
                const auto it = m_pending.find(MakeKey(host, service));
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

        static std::string MakeKey(const std::string& host, const std::string& service)
        {
            return host + '\n' + service;
        }

        std::chrono::seconds m_ttl;
        std::mutex m_mutex;
        std::unordered_map<std::string, Entry> m_entries;
        std::unordered_map<std::string, std::vector<Waiter>> m_pending;
    };

    using DnsCache = BasicDnsCache<>;
} // namespace AVEVA::Private
