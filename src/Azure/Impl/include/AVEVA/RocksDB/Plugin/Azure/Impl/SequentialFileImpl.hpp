// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// Reads a blob front to back from an internal position. All blob access is delegated to a ReadableFileImpl.
// Not thread safe: RocksDB uses a sequential file from one thread at a time.
template <typename TBlob> class BasicSequentialFileImpl {
    // Small reads (WAL/MANIFEST replay issues them in 32 KB steps) are served from a block fetched ahead in one GET.
    // The block is dropped when the blob's ETag changes.
    static constexpr int64_t kReadaheadBytes = 1024 * 1024;

    std::shared_ptr<BasicReadableFileImpl<TBlob>> m_file;
    int64_t m_offset = 0;
    std::unique_ptr<char[]> m_readahead;
    int64_t m_readaheadLength = 0;
    int64_t m_readaheadStart = 0;
    std::string m_readaheadEtag;

    int64_t ReadThroughReadahead(int64_t bytesToRead, char* buffer);

  public:
    explicit BasicSequentialFileImpl(std::shared_ptr<BasicReadableFileImpl<TBlob>> file);

    // NOTE: Increments the offset
    [[nodiscard]] int64_t SequentialRead(int64_t bytesToRead, char* buffer);

    [[nodiscard]] int64_t GetOffset() const;
    void Skip(int64_t n);
    [[nodiscard]] int64_t GetSize() const;
};
extern template class BasicSequentialFileImpl<PageBlobClient>;

/// <summary>The production file, over the real page blob client. Non-template so it can be forward declared.</summary>
class SequentialFileImpl : public BasicSequentialFileImpl<PageBlobClient> {
  public:
    using BasicSequentialFileImpl<PageBlobClient>::BasicSequentialFileImpl;
};

template <typename TBlob>
BasicSequentialFileImpl<TBlob>::BasicSequentialFileImpl(std::shared_ptr<BasicReadableFileImpl<TBlob>> file)
    : m_file(std::move(file)) {}

template <typename TBlob>
int64_t BasicSequentialFileImpl<TBlob>::SequentialRead(const int64_t bytesToRead, char* buffer) {
    if (bytesToRead <= 0) {
        return 0;
    }

    int64_t bytesRead = 0;
    if (bytesToRead >= kReadaheadBytes) {
        bytesRead = m_file->RandomRead(m_offset, bytesToRead, buffer);
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
        if (m_offset < m_readaheadStart || available <= 0 ||
            m_file->HasETag(m_readaheadEtag) == Impl::ReadableFileDetail::ETagMatch::Differs) {
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
    const auto etagBeforeRead = m_file->GetETag();
    const auto fetched =
        std::max<int64_t>(m_file->RandomRead(m_offset + served, kReadaheadBytes, m_readahead.get()), 0);
    m_readaheadLength = fetched;
    m_readaheadStart = m_offset + served;
    m_readaheadEtag = etagBeforeRead;

    const auto n = std::min(bytesToRead - served, fetched);
    std::copy_n(m_readahead.get(), n, buffer + served);
    return served + n;
}

template <typename TBlob> int64_t BasicSequentialFileImpl<TBlob>::GetOffset() const { return m_offset; }

template <typename TBlob> void BasicSequentialFileImpl<TBlob>::Skip(const int64_t n) {
    // Saturates instead of overflowing; no blob is that large, so every later read just reports end of file.
    m_offset = n > RangeCheck::MaxOffset - m_offset ? RangeCheck::MaxOffset : m_offset + std::max<int64_t>(n, 0);
}

template <typename TBlob> int64_t BasicSequentialFileImpl<TBlob>::GetSize() const { return m_file->GetSize(); }
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
