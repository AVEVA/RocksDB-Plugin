// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <openssl/ssl.h>

#include <array>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
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
class TlsSessionCache {
  public:
    // Bounds memory when a client talks to many origins.
    static constexpr std::size_t MaxEntries = 128;

    TlsSessionCache() : m_index(IndexType::bucket_traits(m_buckets.data(), m_buckets.size())) {}
    TlsSessionCache(const TlsSessionCache&) = delete;
    TlsSessionCache& operator=(const TlsSessionCache&) = delete;
    ~TlsSessionCache() {
        m_index.clear();
        m_order.clear_and_dispose(std::default_delete<Entry>{});
    }

    /// <summary>
    /// Remembers the session for an origin, evicting the least recently used origin once full.
    /// The only throwing step is allocating the entry, which happens before anything is linked; linking into the
    /// index and then the list cannot fail, so the two never disagree.
    /// </summary>
    void Store(const std::string& origin, UniqueSession session) {
        const std::scoped_lock lock(m_mutex);
        if (const auto found = m_index.find(origin); found != m_index.end()) {
            found->session = std::move(session);
            m_order.splice(m_order.begin(), m_order, m_order.iterator_to(*found));
            return;
        }
        auto entry = std::make_unique<Entry>(origin, std::move(session));
        m_index.insert(*entry);
        m_order.push_front(*entry.release());
        while (m_index.size() > MaxEntries) {
            Entry& oldest = m_order.back();
            m_index.erase(m_index.iterator_to(oldest));
            m_order.pop_back_and_dispose(std::default_delete<Entry>{});
        }
    }

    // A TLS 1.3 ticket must not be presented twice, so it is handed out once; older sessions are reusable.
    [[nodiscard]] UniqueSession Take(const std::string& origin) {
        const std::scoped_lock lock(m_mutex);
        const auto found = m_index.find(origin);
        if (found == m_index.end()) {
            return nullptr;
        }
        Entry& entry = *found;
        if (SSL_SESSION_get_protocol_version(entry.session.get()) == TLS1_3_VERSION) {
            UniqueSession session = std::move(entry.session);
            m_index.erase(found);
            m_order.erase_and_dispose(m_order.iterator_to(entry), std::default_delete<Entry>{});
            return session;
        }
        m_order.splice(m_order.begin(), m_order, m_order.iterator_to(entry));
        SSL_SESSION_up_ref(entry.session.get());
        return UniqueSession{entry.session.get()};
    }

    [[nodiscard]] std::size_t ListSize() const {
        const std::scoped_lock lock(m_mutex);
        return m_order.size();
    }

    [[nodiscard]] std::size_t IndexSize() const {
        const std::scoped_lock lock(m_mutex);
        return m_index.size();
    }

  private:
    // One allocation per origin, linked into both the recency list and the origin index.
    struct Entry : boost::intrusive::list_base_hook<>, boost::intrusive::unordered_set_base_hook<> {
        Entry(std::string originKey, UniqueSession sessionValue)
            : origin(std::move(originKey)), session(std::move(sessionValue)) {}
        std::string origin;
        UniqueSession session;
    };
    struct OriginOf {
        using type = std::string;
        const std::string& operator()(const Entry& entry) const noexcept { return entry.origin; }
    };
    using Order = boost::intrusive::list<Entry>;
    using IndexType = boost::intrusive::unordered_set<Entry, boost::intrusive::key_of_value<OriginOf>,
                                                      boost::intrusive::hash<std::hash<std::string>>,
                                                      boost::intrusive::constant_time_size<true>>;

    // Twice MaxEntries keeps chains short; fixed so inserting never allocates.
    static constexpr std::size_t BucketCount = MaxEntries * 2;

    mutable std::mutex m_mutex;
    std::array<IndexType::bucket_type, BucketCount> m_buckets;
    IndexType m_index;
    Order m_order; // Most recently used first.
};
} // namespace AVEVA::Private
