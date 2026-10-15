// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Definitions of BasicReadWriteFileImpl; included at the end of ReadWriteFileImpl.hpp.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include <algorithm>
#include <boost/log/trivial.hpp>

#include <exception>
#include <numeric>
#include <span>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TBlob>
BasicReadWriteFileImpl<TBlob>::BasicReadWriteFileImpl(
    std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_w(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger), 0), m_size(0),
      m_syncSize(0), m_closed(false), m_buffer(Configuration::PageBlob::DefaultBufferSize) {
    // One GetProperties call yields both the size and the capacity.
    const auto info = BlobOperations::GetInfo(*m_w.H.Blob);
    m_size = info.Size;
    m_syncSize = info.Size;
    m_w.AdoptCapacity(info.Capacity);
}

template <typename TBlob> BasicReadWriteFileImpl<TBlob>::~BasicReadWriteFileImpl() {
    CloseWithRetries(*m_w.H.Logger, m_w.H.Name, [this] { Close(); }, [this] { return m_w.H.Runtime->WritesFenced(); });
}

template <typename TBlob>
BasicReadWriteFileImpl<TBlob>::BasicReadWriteFileImpl(BasicReadWriteFileImpl<TBlob>&& other) noexcept
    : m_w(std::move(other.m_w)), m_size(other.m_size), m_syncSize(other.m_syncSize),
      m_closed(std::exchange(other.m_closed, true)), m_dirty(other.m_dirty), m_buffer(std::move(other.m_buffer)),
      m_bufferStats(std::move(other.m_bufferStats)) {}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Close() {
    if (!m_closed) {
        Sync();
        m_closed = true;
    }
}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Sync() {
    // A metadata write changes the blob's ETag and invalidates readers and the cache, so skip it when there is
    // nothing buffered and the published size is already current (e.g. Sync followed by Close).
    if (m_bufferStats.empty() && m_size == m_syncSize && !m_dirty) {
        return;
    }

    m_w.MarkStale();

    Flush();

    m_w.PublishSize(m_size);
    m_syncSize = m_size;
    m_dirty = false;
    BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::debug)
        << "Synced read/writeable file '" << m_w.H.Name << "' to " << m_size << " bytes";
}

// Writes the buffered chunks to the blob. Chunks that touch the same page (or sit next to each other) are merged into
// one page-aligned extent so each page is written once with the last write winning, and bytes of partially covered
// pages that no chunk supplies are read from the blob first. Extents never share a page, so their reads and uploads
// are independent and run concurrently, in bounded waves.
template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Flush() {
    if (m_bufferStats.empty()) {
        return;
    }

    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;

    // The capacity is tracked locally (set on open and by EnsureCapacity), so no round trip is needed here.
    auto maxSizeNeeded = m_size;
    for (const auto& chunk : m_bufferStats) {
        maxSizeNeeded = std::max(maxSizeNeeded, chunk.targetOffset + chunk.dataLength);
    }
    m_w.EnsureCapacity(maxSizeNeeded + pageSize);

    struct Extent {
        int64_t Start = 0;
        int64_t End = 0;
        std::vector<size_t> Chunks;              // indices into m_bufferStats, in write order
        std::shared_ptr<std::vector<char>> Data; // shared so each upload's request views it without a copy
    };

    std::vector<size_t> order(m_bufferStats.size());
    std::iota(order.begin(), order.end(), size_t{0});
    const auto rangeStart = [&](size_t i) { return m_bufferStats[i].targetOffset - m_bufferStats[i].prePadding; };
    const auto rangeEnd = [&](size_t i) {
        return m_bufferStats[i].targetOffset + m_bufferStats[i].dataLength + m_bufferStats[i].postPadding;
    };
    std::ranges::stable_sort(order, [&](size_t a, size_t b) { return rangeStart(a) < rangeStart(b); });

    std::vector<Extent> extents;
    for (const auto index : order) {
        if (extents.empty() || rangeStart(index) > extents.back().End) {
            extents.push_back(Extent{rangeStart(index), rangeEnd(index), {}, {}});
        }
        auto& extent = extents.back();
        extent.End = std::max(extent.End, rangeEnd(index));
        extent.Chunks.push_back(index);
    }
    for (auto& extent : extents) {
        std::ranges::sort(extent.Chunks); // later writes must overwrite earlier ones
    }

    const auto sizeBefore = m_size;
    m_size = maxSizeNeeded;

    using PendingRead = BlobOperations::PendingDownload<TBlob>;

    // Fills the bytes of an extent that no chunk supplies with what the blob already holds. Reads past the file's old
    // end are skipped: that region holds no data yet and stays zero.
    const auto prepare = [&](Extent& extent, std::vector<PendingRead>& reads) {
        extent.Data = std::make_shared<std::vector<char>>(static_cast<size_t>(extent.End - extent.Start), '\0');
        std::vector<std::pair<int64_t, int64_t>> covered;
        for (const auto index : extent.Chunks) {
            const auto& chunk = m_bufferStats[index];
            const auto* source = m_buffer.data() + chunk.bufferOffset + chunk.prePadding;
            std::copy(source, source + chunk.dataLength, extent.Data->begin() + (chunk.targetOffset - extent.Start));
            covered.emplace_back(chunk.targetOffset, chunk.targetOffset + chunk.dataLength);
        }
        std::ranges::sort(covered);

        auto cursor = extent.Start;
        const auto readGap = [&](int64_t gapStart, int64_t gapEnd) {
            gapEnd = std::min(gapEnd, sizeBefore);
            if (gapEnd > gapStart) {
                std::span<char> target(extent.Data->data() + (gapStart - extent.Start),
                                       static_cast<size_t>(gapEnd - gapStart));
                reads.push_back(BlobOperations::StartDownload(*m_w.H.Blob, target, gapStart, gapEnd - gapStart));
            }
        };
        for (const auto& [start, end] : covered) {
            if (start > cursor) {
                readGap(cursor, start);
            }
            cursor = std::max(cursor, end);
        }
        readGap(cursor, extent.End);
    };

    // Waits for everything before reporting the first failure: the operations write into / read from the extents'
    // buffers, which must not be released while any of them is still in flight.
    const auto finishAll = [](auto& pending, auto&& finish) {
        std::exception_ptr failure;
        for (auto& item : pending) {
            try {
                finish(item);
            } catch (...) {
                if (!failure) {
                    failure = std::current_exception();
                }
            }
        }
        pending.clear();
        if (failure) {
            std::rethrow_exception(failure);
        }
    };
    const auto finishRead = [](PendingRead& read) {
        [[maybe_unused]] const auto bytes = BlobOperations::FinishDownload(read);
    };
    const auto finishUpload = [](auto& upload) { BlobOperations::FinishUploadPages(upload); };

    for (size_t begin = 0; begin < extents.size(); begin += Configuration::Transfer::MaxConcurrentPageCopies) {
        const auto end = std::min(extents.size(), begin + Configuration::Transfer::MaxConcurrentPageCopies);

        std::vector<PendingRead> reads;
        try {
            for (auto i = begin; i < end; ++i) {
                prepare(extents[i], reads);
            }
        } catch (...) {
            try {
                finishAll(reads, finishRead);
            } catch (...) {
            }
            throw;
        }
        finishAll(reads, finishRead);

        using PendingUploadT = decltype(BlobOperations::StartUploadPages(
            *m_w.H.Runtime, *m_w.H.Blob, std::shared_ptr<const std::vector<char>>{}, int64_t{0}));
        std::vector<PendingUploadT> uploads;
        try {
            for (auto i = begin; i < end; ++i) {
                uploads.push_back(
                    BlobOperations::StartUploadPages(*m_w.H.Runtime, *m_w.H.Blob, extents[i].Data, extents[i].Start));
            }
        } catch (...) {
            try {
                finishAll(uploads, finishUpload);
            } catch (...) {
            }
            throw;
        }
        finishAll(uploads, finishUpload);

        for (auto i = begin; i < end; ++i) {
            BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::debug)
                << "Flushed " << extents[i].Data->size() << " bytes to read/writeable file '" << m_w.H.Name << "'";
        }
    }
    m_bufferStats.clear();

#if _DEBUG
    // NOTE: This is to ensure predictable output in tests.
    std::fill(m_buffer.begin(), m_buffer.end(), '\0');
#endif // _DEBUG
}

// Buffers a write at its page-aligned slot with room around it, so Flush can merge the existing bytes of partially
// covered pages and upload whole pages. Flushes early when the buffer is nearly full.
template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Write(int64_t offset, const char* data, int64_t size) {
    // This one is TRICKY. Incomplete pages must track the location within a page
    // along with length information and then fetch the relevant page before committing
    // likely to be lots of room for optimization maybe around allocation
    auto* dataPos = data;
    if (!m_dirty) {
        // The cache is invalidated at the first write, not at Sync, because Read serves unsynced bytes from the blob.
        m_w.MarkStale();
        m_dirty = true;
    }
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

// Includes the file's own unsynced writes: buffered chunks are flushed first so the blob holds them, and the limit is
// m_size rather than the published m_syncSize.
template <typename TBlob>
int64_t BasicReadWriteFileImpl<TBlob>::Read(int64_t offset, int64_t bytesRequested, char* buffer) const {
    if (!m_bufferStats.empty()) {
        // Read is logically const; flushing only moves buffered bytes to the blob.
        const_cast<BasicReadWriteFileImpl*>(this)->Flush();
    }

    if (offset >= m_size) {
        return 0;
    }

    // Cached pages may predate this file's unsynced writes.
    if (m_w.H.FileCache && !m_dirty) {
        const auto bytesRead = m_w.H.FileCache->ReadFile(m_w.H.Name, offset, bytesRequested, buffer);
        if (bytesRead) {
            return static_cast<int64_t>(*bytesRead);
        }
    }

    const auto bytesCanRead = m_size - offset;
    if (bytesCanRead < bytesRequested)
        bytesRequested = bytesCanRead;

    std::span<char> readBuffer(buffer, static_cast<size_t>(bytesRequested));
    return BlobOperations::DownloadToBuffer(*m_w.H.Blob, readBuffer, offset, bytesRequested);
}

} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
