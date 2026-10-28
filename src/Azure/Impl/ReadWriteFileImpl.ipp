// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Definitions of BasicReadWriteFileImpl; included at the end of ReadWriteFileImpl.hpp.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"
#include <algorithm>
#include <atomic>
#include <boost/container/small_vector.hpp>
#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <exception>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
// use_count() is a relaxed load; the fence orders it before our reuse of memory the last other owner just wrote, so
// the reuse does not race with that owner's release.
bool IsIdle(const std::shared_ptr<std::vector<char>>& buffer) {
    if (buffer.use_count() != 1) {
        return false;
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    return true;
}
} // namespace
template <typename TBlob>
BasicReadWriteFileImpl<TBlob>::BasicReadWriteFileImpl(
    std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_w(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger), 0), m_size(0),
      m_syncSize(0), m_buffer(Configuration::PageBlob::DefaultBufferSize) {
    // One GetProperties call yields both the size and the capacity.
    const auto info = BlobOperations::GetInfo(*m_w.H.Blob);
    m_size = info.Size;
    m_syncSize = info.Size;
    m_w.AdoptCapacity(info.Capacity);
}

template <typename TBlob> BasicReadWriteFileImpl<TBlob>::~BasicReadWriteFileImpl() {
    // Close() is the durability boundary; a destructor must not run sleeping retries on RocksDB's cleanup threads.
    // A moved-from file is closed and has lost its logger, which must not be dereferenced.
    if (!m_close.IsClosed()) {
        m_close.LogIfUnclosed(*m_w.H.Logger, m_w.H.Name, !m_bufferStats.empty() || m_size != m_syncSize || m_dirty);
    }
}

template <typename TBlob>
BasicReadWriteFileImpl<TBlob>::BasicReadWriteFileImpl(BasicReadWriteFileImpl<TBlob>&& other) noexcept
    : m_w(std::move(other.m_w)), m_size(other.m_size), m_syncSize(other.m_syncSize), m_close(std::move(other.m_close)),
      m_dirty(other.m_dirty), m_buffer(std::move(other.m_buffer)), m_bufferStats(std::move(other.m_bufferStats)),
      m_extentPool(std::move(other.m_extentPool)) {}

template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Close() {
    m_close.Close([this] { Sync(); });
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

// Merges the buffered chunks into page-aligned extents. Chunks that touch the same page (or sit next to each other)
// share an extent so each page is written once with the last write winning. Extents never share a page, so their
// reads and uploads are independent.
template <typename TBlob> auto BasicReadWriteFileImpl<TBlob>::BuildExtents() const -> std::vector<Extent> {
    std::vector<size_t> order(m_bufferStats.size());
    std::iota(order.begin(), order.end(), size_t{0});
    const auto rangeStart = [&](size_t i) { return m_bufferStats[i].targetOffset - m_bufferStats[i].prePadding; };
    const auto rangeEnd = [&](size_t i) {
        return m_bufferStats[i].targetOffset + m_bufferStats[i].dataLength + m_bufferStats[i].postPadding;
    };
    // Stability is not needed: each extent's chunks are re-sorted into write order below.
    std::ranges::sort(order, [&](size_t a, size_t b) { return rangeStart(a) < rangeStart(b); });

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
    return extents;
}

// Copies the buffered chunks of an extent into its buffer in write order, so the last write wins.
template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::OverlayChunks(const Extent& extent) {
    for (const auto index : extent.Chunks) {
        const auto& chunk = m_bufferStats[index];
        const auto* source = m_buffer.data() + chunk.bufferOffset + chunk.prePadding;
        std::copy(source, source + chunk.dataLength, extent.Data->begin() + (chunk.targetOffset - extent.Start));
    }
}

// Fills the bytes of an extent that no chunk supplies with what the blob already holds. Reads past the file's old
// end (`sizeBefore`) are skipped: that region holds no data yet and stays zero. This only plans: the zeroing happens
// here, the downloads are returned in `reads` for the caller to run.
//
// Gaps separated by no more than a page of buffered data share one request: many tiny writes would otherwise each
// cost a GET. The wider read also fetches the bytes in between, so the chunks are copied over the extent afterwards
// (OverlayChunks) rather than before.
template <typename TBlob>
void BasicReadWriteFileImpl<TBlob>::PlanGapReads(Extent& extent, int64_t sizeBefore, std::vector<GapRead>& reads) {
    constexpr int64_t maxBridgedBytes = Configuration::PageBlob::PageSize;
    // A reused buffer holds an earlier flush's bytes, so everything not downloaded or copied later is zeroed.
    extent.Data = AcquireExtentBuffer(static_cast<size_t>(extent.End - extent.Start));
    boost::container::small_vector<std::pair<int64_t, int64_t>, 8> covered;
    covered.reserve(extent.Chunks.size());
    for (const auto index : extent.Chunks) {
        const auto& chunk = m_bufferStats[index];
        covered.emplace_back(chunk.targetOffset, chunk.targetOffset + chunk.dataLength);
    }
    std::ranges::sort(covered);

    int64_t readStart = 0;
    int64_t readEnd = 0; // empty when equal to readStart
    const auto flushRead = [&] {
        if (readEnd > readStart) {
            reads.push_back(GapRead{std::span<char>(extent.Data->data() + (readStart - extent.Start),
                                                    static_cast<size_t>(readEnd - readStart)),
                                    readStart, readEnd - readStart});
        }
        readStart = readEnd = 0;
    };
    const auto addGap = [&](int64_t gapStart, int64_t gapEnd) {
        const auto zeroFrom = std::max(gapStart, sizeBefore);
        if (gapEnd > zeroFrom) {
            std::fill(extent.Data->begin() + (zeroFrom - extent.Start), extent.Data->begin() + (gapEnd - extent.Start),
                      '\0');
        }
        gapEnd = std::min(gapEnd, sizeBefore);
        if (gapEnd <= gapStart) {
            return;
        }
        if (readEnd > readStart && gapStart - readEnd <= maxBridgedBytes) {
            readEnd = gapEnd;
            return;
        }
        flushRead();
        readStart = gapStart;
        readEnd = gapEnd;
    };

    auto cursor = extent.Start;
    for (const auto& [start, end] : covered) {
        if (start > cursor) {
            addGap(cursor, start);
        }
        cursor = std::max(cursor, end);
    }
    addGap(cursor, extent.End);
    flushRead();
}

// A short gap read would upload zeros over bytes that were never fetched, so it fails the flush.
template <typename TBlob> BlobOperations::Task<void> BasicReadWriteFileImpl<TBlob>::ReadGapAsync(const GapRead read) {
    BlobHelpers::RequireFullDownload(
        m_w.H.Name, read.Offset, read.Length,
        co_await BlobOperations::DownloadAsync(*m_w.H.Blob, read.Target, read.Offset, read.Length));
}

// Reads the gaps of, then uploads, extents [begin, end). Each stage waits for all of its operations before reporting
// the first failure: they write into / read from the extents' buffers, which must not be released while any is still
// in flight.
template <typename TBlob>
BlobOperations::Task<void> BasicReadWriteFileImpl<TBlob>::RunWaveAsync(std::vector<Extent>& extents, size_t begin,
                                                                       size_t end, int64_t sizeBefore) {
    // Runs on every exit, after the reads and uploads have drained, so the wave's buffers go back to the pool for the
    // next wave instead of staying owned until the whole flush ends.
    const boost::scope::scope_exit releaseWave{[&] noexcept {
        for (auto i = begin; i < end; ++i) {
            extents[i].Data.reset();
        }
    }};

    const auto executor = co_await boost::asio::this_coro::executor;
    std::vector<GapRead> gaps;
    for (auto i = begin; i < end; ++i) {
        PlanGapReads(extents[i], sizeBefore, gaps);
    }
    // One extent can hold many separated gaps, so only a window of them is in flight.
    TaskGroup reads(executor, Configuration::Transfer::MaxConcurrentPageCopies);
    co_await reads.Scope([&](TaskGroup& running) -> BlobOperations::Task<void> {
        for (const auto& gap : gaps) {
            co_await running.Spawn(ReadGapAsync(gap));
        }
    });
    for (auto i = begin; i < end; ++i) {
        OverlayChunks(extents[i]);
    }

    TaskGroup uploads(executor, end - begin);
    co_await uploads.Scope([&](TaskGroup& running) -> BlobOperations::Task<void> {
        for (auto i = begin; i < end; ++i) {
            co_await running.Spawn(
                BlobOperations::UploadPagesAsync(*m_w.H.Runtime, *m_w.H.Blob, extents[i].Data, extents[i].Start));
        }
    });

    for (auto i = begin; i < end; ++i) {
        BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::debug)
            << "Flushed " << extents[i].Data->size() << " bytes to read/writeable file '" << m_w.H.Name << "'";
    }
}

// Writes the buffered chunks to the blob in bounded waves of concurrent extents. m_size and the buffered chunks are
// only committed once every wave has succeeded, so a failed Flush can be retried from the same state.
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
    // RangeCheck admits ends up to INT64_MAX, so the page of headroom below must not overflow.
    if (maxSizeNeeded > RangeCheck::MaxOffset - pageSize) {
        throw std::invalid_argument("Write extends past the maximum supported file size");
    }
    m_w.EnsureCapacity(maxSizeNeeded + pageSize);

    auto extents = BuildExtents();
    // Trims on failure too, once each wave has released its buffers; the buffered writes stay for a retry.
    const boost::scope::scope_exit trimOnExit{[this] { TrimExtentPool(); }};
    for (size_t begin = 0; begin < extents.size(); begin += Configuration::Transfer::MaxConcurrentPageCopies) {
        const auto end = std::min(extents.size(), begin + Configuration::Transfer::MaxConcurrentPageCopies);
        Block(*m_w.H.Blob, RunWaveAsync(extents, begin, end, m_size));
    }
    // Writes that landed after the lease lapsed must not be reported as flushed.
    m_w.H.Runtime->ThrowIfWritesFenced();

    m_size = maxSizeNeeded;
    m_bufferStats.clear();
}
// Hands out an idle pooled buffer resized to `size`, or a new one. A buffer an upload's request still references is
// not idle, so it is never rewritten while in use. The contents are unspecified: callers set every byte.
template <typename TBlob>
std::shared_ptr<std::vector<char>> BasicReadWriteFileImpl<TBlob>::AcquireExtentBuffer(size_t size) {
    if (const auto idle = std::ranges::find_if(m_extentPool, IsIdle); idle != m_extentPool.end()) {
        (*idle)->resize(size);
        return *idle;
    }
    return m_extentPool.emplace_back(std::make_shared<std::vector<char>>(size));
}

// Keeps the pool from growing past one wave of extents, so a burst of large flushes does not pin memory.
template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::TrimExtentPool() {
    for (auto it = m_extentPool.begin();
         it != m_extentPool.end() && m_extentPool.size() > Configuration::Transfer::MaxConcurrentPageCopies;) {
        it = IsIdle(*it) ? m_extentPool.erase(it) : it + 1;
    }
}
// Buffers a write at its page-aligned slot with room around it, so Flush can merge the existing bytes of partially
// covered pages and upload whole pages. Flushes early when the buffer is nearly full.
template <typename TBlob> void BasicReadWriteFileImpl<TBlob>::Write(int64_t offset, const char* data, int64_t size) {
    RangeCheck::ThrowIfInvalid(offset, size, "Write");
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

        if (bufferOffset >= static_cast<int64_t>(m_buffer.size()) - Configuration::PageBlob::PageSize) {
            Flush();
            bufferOffset = 0;
        }
    }
}

// Includes the file's own unsynced writes: buffered chunks are flushed first so the blob holds them, and the limit is
// m_size rather than the published m_syncSize.
template <typename TBlob>
int64_t BasicReadWriteFileImpl<TBlob>::Read(int64_t offset, int64_t bytesRequested, char* buffer) {
    RangeCheck::ThrowIfInvalid(offset, bytesRequested, "Read");
    if (!m_bufferStats.empty()) {
        Flush();
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
