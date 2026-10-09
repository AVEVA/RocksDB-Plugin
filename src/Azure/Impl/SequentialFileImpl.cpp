// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"

#include <algorithm>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
SequentialFileImpl::SequentialFileImpl(ReadableFileImpl&& file) : m_file(std::move(file)) {}

int64_t SequentialFileImpl::SequentialRead(const int64_t bytesToRead, char* buffer) {
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

int64_t SequentialFileImpl::ReadThroughReadahead(const int64_t bytesToRead, char* buffer) {
    const auto copyFromReadahead = [&]() -> int64_t {
        const auto available = m_readaheadStart + static_cast<int64_t>(m_readahead.size()) - m_offset;
        if (m_offset < m_readaheadStart || available <= 0 || m_file.GetETag() != m_readaheadEtag) {
            return 0;
        }
        const auto n = std::min(bytesToRead, available);
        std::copy_n(m_readahead.data() + (m_offset - m_readaheadStart), n, buffer);
        return n;
    };

    auto served = copyFromReadahead();
    if (served == bytesToRead) {
        return served;
    }

    // The readahead is exhausted (or stale): refill it with one larger GET starting at the current position.
    m_readahead.clear();
    m_readahead.resize(static_cast<size_t>(kReadaheadBytes));
    const auto fetched =
        std::max<int64_t>(m_file.RandomRead(m_offset + served, kReadaheadBytes, m_readahead.data()), 0);
    m_readahead.resize(static_cast<size_t>(fetched));
    m_readaheadStart = m_offset + served;
    m_readaheadEtag = m_file.GetETag();

    const auto n = std::min(bytesToRead - served, fetched);
    std::copy_n(m_readahead.data(), n, buffer + served);
    return served + n;
}

int64_t SequentialFileImpl::GetOffset() const { return m_offset; }

void SequentialFileImpl::Skip(const int64_t n) { m_offset += n; }

int64_t SequentialFileImpl::GetSize() const { return m_file.GetSize(); }
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
