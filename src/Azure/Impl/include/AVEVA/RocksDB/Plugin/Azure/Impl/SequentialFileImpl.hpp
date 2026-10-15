// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// Reads a blob front to back from an internal position. All blob access is delegated to a ReadableFileImpl.
// Not thread safe: RocksDB uses a sequential file from one thread at a time.
template <typename TBlob> class BasicSequentialFileImpl {
    // Small reads (WAL/MANIFEST replay issues them in 32 KB steps) are served from a block fetched ahead in one GET.
    // The block is dropped when the blob's ETag changes.
    static constexpr int64_t kReadaheadBytes = 1024 * 1024;

    BasicReadableFileImpl<TBlob> m_file;
    int64_t m_offset = 0;
    std::unique_ptr<char[]> m_readahead;
    int64_t m_readaheadLength = 0;
    int64_t m_readaheadStart = 0;
    std::string m_readaheadEtag;

    int64_t ReadThroughReadahead(int64_t bytesToRead, char* buffer);

  public:
    explicit BasicSequentialFileImpl(BasicReadableFileImpl<TBlob>&& file);

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
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
