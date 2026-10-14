// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Definitions of BasicReadWriteFileImpl; included at the end of ReadWriteFileImpl.hpp.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include <algorithm>
#include <boost/log/trivial.hpp>

#include <exception>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TBlob>
BasicReadWriteFileImpl<TBlob>::BasicReadWriteFileImpl(
    std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_name(name), m_runtime(std::move(runtime)), m_blob(std::move(blob)), m_fileCache(std::move(fileCache)),
      m_logger(std::move(logger)), m_size(0), m_syncSize(0), m_capacity(0), m_closed(false),
      m_buffer(Configuration::PageBlob::DefaultBufferSize) {
    // One GetProperties call yields both the size and the capacity.
    const auto info = BlobOperations::GetInfo(*m_blob);
    m_size = info.Size;
    m_syncSize = info.Size;
    m_capacity = info.Capacity;
}

template <typename TBlob> BasicReadWriteFileImpl<TBlob>::~BasicReadWriteFileImpl() {
    CloseWithRetries(*m_logger, m_name, [this] { Close(); }, [] { return false; });
}

template <typename TBlob>
BasicReadWriteFileImpl<TBlob>::BasicReadWriteFileImpl(BasicReadWriteFileImpl<TBlob>&& other) noexcept
    : m_name(std::move(other.m_name)), m_runtime(std::move(other.m_runtime)), m_blob(std::move(other.m_blob)),
      m_fileCache(std::move(other.m_fileCache)), m_logger(std::move(other.m_logger)), m_size(other.m_size),
      m_syncSize(other.m_syncSize), m_capacity(other.m_capacity), m_closed(std::exchange(other.m_closed, true)),
      m_buffer(std::move(other.m_buffer)), m_bufferStats(std::move(other.m_bufferStats)) {}

template <typename TBlob>
BasicReadWriteFileImpl<TBlob>&
BasicReadWriteFileImpl<TBlob>::operator=(BasicReadWriteFileImpl<TBlob>&& other) noexcept {
    m_name = std::move(other.m_name);
    m_blob = std::move(other.m_blob);
    m_runtime = std::move(other.m_runtime);
    m_fileCache = std::move(other.m_fileCache);
    m_logger = std::move(other.m_logger);
    m_size = other.m_size;
    m_syncSize = other.m_syncSize;
    m_capacity = other.m_capacity;
    m_closed = std::exchange(other.m_closed, true);
    m_buffer = std::move(other.m_buffer);
    m_bufferStats = std::move(other.m_bufferStats);
    return *this;
}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Close() {
    if (!m_closed) {
        Sync();
        m_closed = true;
    }
}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Sync() {
    if (m_fileCache) {
        m_fileCache->MarkFileAsStaleIfExists(m_name);
    }

    Flush();

    BlobOperations::SetSize(*m_runtime, *m_blob, m_size);
    m_syncSize = m_size;
    BOOST_LOG_SEV(*m_logger, boost::log::trivial::debug)
        << "Synced read/writeable file '" << m_name << "' to " << m_size << " bytes";
}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Flush() {
    // The capacity is tracked locally (set on open and by Expand), so no round trip is needed here.
    // Pre-calculate the maximum size we'll need and expand if necessary
    auto maxSizeNeeded = m_size;
    for (const auto& chunk : m_bufferStats) {
        const auto chunkEnd = chunk.targetOffset + chunk.dataLength;
        if (chunkEnd > maxSizeNeeded) {
            maxSizeNeeded = chunkEnd;
        }
    }

    // Expand capacity if needed BEFORE processing chunks
    while ((maxSizeNeeded + Configuration::PageBlob::PageSize) > m_capacity) {
        Expand();
    }

    using Pending = BlobOperations::PendingDownload<TBlob>;

    // Starts the reads that fill a chunk's partial first and last pages with the blob's existing bytes. `sizeBefore`
    // is the file size just before this chunk is written.
    const auto startPaddingReads = [&](const auto& chunk, int64_t sizeBefore, std::vector<Pending>& reads) {
        assert(chunk.targetOffset >= chunk.prePadding && "Target Offset is smaller than Pre-padding");
        const auto targetStart = chunk.targetOffset - chunk.prePadding;
        assert(targetStart % Configuration::PageBlob::PageSize == 0 && "TargetStart should be page aligned");

        if (chunk.prePadding > 0) {
            assert(chunk.prePadding < Configuration::PageBlob::PageSize && "Pre-padding should not exceed page size");
            std::span<char> prePaddingBuffer(m_buffer.data() + chunk.bufferOffset,
                                             static_cast<size_t>(chunk.prePadding));
            reads.push_back(BlobOperations::StartDownload(*m_blob, prePaddingBuffer, targetStart, chunk.prePadding));
        }
        if (chunk.postPadding > 0) {
            const auto targetEnd = chunk.targetOffset + chunk.dataLength;
            assert((targetEnd + chunk.postPadding) % Configuration::PageBlob::PageSize == 0 &&
                   "TargetEnd should be page aligned");
            assert((targetEnd + chunk.postPadding) <= m_capacity &&
                   "We shouldn't try to read data that isn't at least reserved");

            // NOTE: If the data we're writing is APPENDED at the end, this post padding may be trash data and can be
            // skipped.
            if (!(targetEnd > sizeBefore)) {
                assert(chunk.postPadding < Configuration::PageBlob::PageSize &&
                       "PostPadding shouldn't be greater than a page's size");
                std::span<char> postPaddingBuffer(
                    &m_buffer[static_cast<size_t>(chunk.bufferOffset + chunk.prePadding + chunk.dataLength)],
                    static_cast<size_t>(chunk.postPadding));
                reads.push_back(
                    BlobOperations::StartDownload(*m_blob, postPaddingBuffer, targetEnd, chunk.postPadding));
            }
        }
    };

    // Waits for every read before reporting the first failure: the reads write into m_buffer, which must not be
    // touched or released while any of them is still in flight.
    const auto finishReads = [](std::vector<Pending>& reads) {
        std::exception_ptr failure;
        for (auto& read : reads) {
            try {
                [[maybe_unused]] const auto bytes = BlobOperations::FinishDownload(read);
            } catch (...) {
                if (!failure) {
                    failure = std::current_exception();
                }
            }
        }
        reads.clear();
        if (failure) {
            std::rethrow_exception(failure);
        }
    };

    const auto upload = [&](const auto& chunk) {
        const auto targetStart = chunk.targetOffset - chunk.prePadding;
        if ((chunk.targetOffset + chunk.dataLength) > m_size) {
            m_size = chunk.targetOffset + chunk.dataLength;
        }

        std::span<char> uploadBuffer(&m_buffer[static_cast<size_t>(chunk.bufferOffset)],
                                     static_cast<size_t>(chunk.ChunkSize()));
        BlobOperations::UploadPages(*m_runtime, *m_blob, uploadBuffer, targetStart);

        BOOST_LOG_SEV(*m_logger, boost::log::trivial::debug)
            << "Flushed " << chunk.ChunkSize() << " bytes to read/writeable file '" << m_name << "'";
    };

    // A chunk's padding must see the bytes earlier chunks wrote to the same page, so the reads can only all be issued
    // up front when no two chunks touch the same page.
    std::vector<std::pair<int64_t, int64_t>> pageRanges;
    pageRanges.reserve(m_bufferStats.size());
    for (const auto& chunk : m_bufferStats) {
        pageRanges.emplace_back(chunk.targetOffset - chunk.prePadding,
                                chunk.targetOffset + chunk.dataLength + chunk.postPadding);
    }
    std::ranges::sort(pageRanges);
    const bool independentPages = std::ranges::adjacent_find(pageRanges, [](const auto& earlier, const auto& later) {
                                      return later.first < earlier.second;
                                  }) == pageRanges.end();

    std::vector<Pending> reads;
    if (independentPages) {
        auto sizeBefore = m_size;
        try {
            for (const auto& chunk : m_bufferStats) {
                startPaddingReads(chunk, sizeBefore, reads);
                sizeBefore = std::max(sizeBefore, chunk.targetOffset + chunk.dataLength);
            }
        } catch (...) {
            try {
                finishReads(reads);
            } catch (...) {
            }
            throw;
        }
        finishReads(reads);
        for (const auto& chunk : m_bufferStats) {
            upload(chunk);
        }
    } else {
        for (const auto& chunk : m_bufferStats) {
            startPaddingReads(chunk, m_size, reads);
            finishReads(reads);
            upload(chunk);
        }
    }
    m_bufferStats.clear();

#if _DEBUG
    // NOTE: This is to ensure predictable output in tests.
    std::fill(m_buffer.begin(), m_buffer.end(), '\0');
#endif // _DEBUG
}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Write(int64_t offset, const char* data, int64_t size) {
    // This one is TRICKY. Incomplete pages must track the location within a page
    // along with length information and then fetch the relevant page before committing
    // likely to be lots of room for optimization maybe around allocation
    auto* dataPos = data;
    auto targetOffset = offset;
    auto bufferOffset =
        m_bufferStats.empty() ? 0LL : m_bufferStats.back().bufferOffset + m_bufferStats.back().ChunkSize();
    while (size > 0) {
        const int64_t space = static_cast<int64_t>(m_buffer.size()) - bufferOffset;
        auto numBytes = std::min(size, space);

        // calculate the relevant padding to leave space to merge
        // existing page data when flushing
        const auto startPaddingFirstPage = targetOffset % Configuration::PageBlob::PageSize;

        int64_t endPaddingLastPage;
        const auto dataEndPos = (targetOffset + numBytes) % Configuration::PageBlob::PageSize;
        if (dataEndPos == 0) {
            endPaddingLastPage = 0;
        } else {
            endPaddingLastPage = Configuration::PageBlob::PageSize - dataEndPos;
        }

        const auto totalBytesNeeded = startPaddingFirstPage + numBytes + endPaddingLastPage;
        assert(totalBytesNeeded % Configuration::PageBlob::PageSize == 0 && "totalBytesNeeded should be page aligned");

        if (totalBytesNeeded > space) {
            numBytes -= (Configuration::PageBlob::PageSize - endPaddingLastPage);
            endPaddingLastPage = 0;
        }

        assert((bufferOffset + startPaddingFirstPage + numBytes) <= static_cast<int64_t>(m_buffer.size()) &&
               "calculated total offset must be less than buffer size");
        auto* bufPos = m_buffer.data() + bufferOffset + startPaddingFirstPage;
        std::copy(dataPos, dataPos + numBytes, bufPos);

        size -= numBytes;
        BufferChunkInfo info(bufferOffset, targetOffset, numBytes, startPaddingFirstPage, endPaddingLastPage);
        m_bufferStats.push_back(info);
        targetOffset += numBytes;
        dataPos += numBytes;
        bufferOffset += info.ChunkSize();

        if (bufferOffset >= (Configuration::PageBlob::DefaultBufferSize - Configuration::PageBlob::PageSize)) {
            Flush();
            bufferOffset = 0;
        }
    }
}

template <typename TBlob>
int64_t BasicReadWriteFileImpl<TBlob>::Read(int64_t offset, int64_t bytesRequested, char* buffer) const {
    if (offset >= m_syncSize) {
        return 0;
    }

    if (m_fileCache) {
        const auto bytesRead = m_fileCache->ReadFile(m_name, offset, bytesRequested, buffer);
        if (bytesRead) {
            return static_cast<int64_t>(*bytesRead);
        }
    }

    const auto bytesCanRead = m_syncSize - offset;
    if (bytesCanRead < bytesRequested)
        bytesRequested = bytesCanRead;

    std::span<char> readBuffer(buffer, static_cast<size_t>(bytesRequested));
    return BlobOperations::DownloadToBuffer(*m_blob, readBuffer, offset, bytesRequested);
}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Expand() {
    const auto capacity = std::max(static_cast<int64_t>(1), m_capacity);
    const auto [_, rounded] = BlobHelpers::RoundToEndOfNearestPage((m_size + capacity) * 2);
    const auto desiredSize = rounded;
    BlobOperations::SetCapacity(*m_runtime, *m_blob, desiredSize);
    m_capacity = desiredSize;

    BOOST_LOG_SEV(*m_logger, boost::log::trivial::debug)
        << "Expanding read/writeable file '" << m_name << "' to " << desiredSize << " bytes";
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
