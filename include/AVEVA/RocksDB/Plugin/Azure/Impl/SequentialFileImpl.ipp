// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Definitions of BasicSequentialFileImpl; included at the end of SequentialFileImpl.hpp.
#pragma once
#include <algorithm>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TBlob>
BasicSequentialFileImpl<TBlob>::BasicSequentialFileImpl(BasicReadableFileImpl<TBlob>&& file)
    : m_file(std::move(file)) {}

template <typename TBlob>
int64_t BasicSequentialFileImpl<TBlob>::SequentialRead(const int64_t bytesToRead, char* buffer) {
    if (bytesToRead <= 0) {
        return 0;
    }

    int64_t bytesRead = 0;
    if (bytesToRead >= kReadaheadBytes) {
        bytesRead = m_file.RandomRead(m_offset, bytesToRead, buffer);
    } else {
        bytesRead = ReadThroughReadahead(bytesToRead, buffer);
    }
    bytesRead = std::max<int64_t>(bytesRead, 0);

    m_offset += bytesRead;
    return bytesRead;
}

template <typename TBlob>
int64_t BasicSequentialFileImpl<TBlob>::ReadThroughReadahead(const int64_t bytesToRead, char* buffer) {
    const auto copyFromReadahead = [&]() -> int64_t {
        const auto available = m_readaheadStart + m_readaheadLength - m_offset;
        if (m_offset < m_readaheadStart || available <= 0 || !m_file.HasETag(m_readaheadEtag)) {
            return 0;
        }
        const auto n = std::min(bytesToRead, available);
        std::copy_n(m_readahead.get() + (m_offset - m_readaheadStart), n, buffer);
        return n;
    };

    auto served = copyFromReadahead();
    if (served == bytesToRead) {
        return served;
    }

    // The readahead is exhausted (or stale): refill it with one larger GET starting at the current position.
    if (m_readahead == nullptr) {
        // Allocated once and overwritten by each refill; zero-filling 1 MiB per refill would be wasted work.
        m_readahead = std::make_unique_for_overwrite<char[]>(static_cast<size_t>(kReadaheadBytes));
    }
    m_readaheadLength = 0;
    // Captured before the read: if the read refreshes the metadata, the buffer is tagged with the older ETag and
    // simply refetched next time, whereas tagging it afterwards could vouch for bytes downloaded under an older
    // ETag.
    const auto etagBeforeRead = m_file.GetETag();
    const auto fetched = std::max<int64_t>(m_file.RandomRead(m_offset + served, kReadaheadBytes, m_readahead.get()), 0);
    m_readaheadLength = fetched;
    m_readaheadStart = m_offset + served;
    m_readaheadEtag = etagBeforeRead;

    const auto n = std::min(bytesToRead - served, fetched);
    std::copy_n(m_readahead.get(), n, buffer + served);
    return served + n;
}

template <typename TBlob> int64_t BasicSequentialFileImpl<TBlob>::GetOffset() const { return m_offset; }

template <typename TBlob> void BasicSequentialFileImpl<TBlob>::Skip(const int64_t n) { m_offset += n; }

template <typename TBlob> int64_t BasicSequentialFileImpl<TBlob>::GetSize() const { return m_file.GetSize(); }
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
