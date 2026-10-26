// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <openssl/ssl.h>

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace AVEVA::Private {
struct SessionDeleter {
    void operator()(SSL_SESSION* session) const noexcept { SSL_SESSION_free(session); }
};
using UniqueSession = std::unique_ptr<SSL_SESSION, SessionDeleter>;

/// <summary>
/// Sessions remembered per origin ("host:service") for one SSL_CTX. Shared by every connection of the
/// owning client, so access is serialized.
/// </summary>
/// <typeparam name="MapAllocator">Allocator template for the origin index; a seam so tests can inject allocation
/// failures between the list and index updates.</typeparam>
template <template <class> class MapAllocator = std::allocator> class BasicTlsSessionCache {
  public:
    // Bounds memory when a client talks to many origins.
    static constexpr std::size_t MaxEntries = 128;

    /// <summary>
    /// Remembers the session for an origin, evicting the least recently used origin once full.
    /// If the index cannot grow the cache is left unchanged and the exception propagates; the list node is
    /// removed first so no node ever exists without an index entry, and eviction only follows a successful insert.
    /// </summary>
    void Store(const std::string& origin, UniqueSession session) {
        const std::scoped_lock lock(m_mutex);
        if (const auto found = m_sessions.find(origin); found != m_sessions.end()) {
            found->second->session = std::move(session);
            m_order.splice(m_order.begin(), m_order, found->second);
            return;
        }
        m_order.push_front(Entry{origin, std::move(session)});
        try {
            m_sessions.emplace(origin, m_order.begin());
        } catch (...) {
            m_order.pop_front();
            throw;
        }
        while (m_sessions.size() > MaxEntries) {
            m_sessions.erase(m_order.back().origin);
            m_order.pop_back();
        }
    }

    // A TLS 1.3 ticket must not be presented twice, so it is handed out once; older sessions are reusable.
    [[nodiscard]] UniqueSession Take(const std::string& origin) {
        const std::scoped_lock lock(m_mutex);
        const auto found = m_sessions.find(origin);
        if (found == m_sessions.end()) {
            return nullptr;
        }
        const auto entry = found->second;
        if (SSL_SESSION_get_protocol_version(entry->session.get()) == TLS1_3_VERSION) {
            UniqueSession session = std::move(entry->session);
            m_order.erase(entry);
            m_sessions.erase(found);
            return session;
        }
        m_order.splice(m_order.begin(), m_order, entry);
        SSL_SESSION_up_ref(entry->session.get());
        return UniqueSession{entry->session.get()};
    }

    [[nodiscard]] std::size_t ListSize() const {
        const std::scoped_lock lock(m_mutex);
        return m_order.size();
    }

    [[nodiscard]] std::size_t IndexSize() const {
        const std::scoped_lock lock(m_mutex);
        return m_sessions.size();
    }

  private:
    struct Entry {
        std::string origin;
        UniqueSession session;
    };
    using Order = std::list<Entry>;
    using IndexValue = std::pair<const std::string, typename Order::iterator>;

    mutable std::mutex m_mutex;
    Order m_order; // Most recently used first.
    std::unordered_map<std::string, typename Order::iterator, std::hash<std::string>, std::equal_to<>,
                       MapAllocator<IndexValue>>
        m_sessions;
};

using TlsSessionCache = BasicTlsSessionCache<>;
} // namespace AVEVA::Private
