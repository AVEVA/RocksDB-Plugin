// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <boost/log/trivial.hpp>

#include <algorithm>
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
AVEVA::RocksDB::Plugin::Azure::Impl::WriteableFileImpl::WriteableFileImpl(
    const std::string_view name, std::shared_ptr<ClientRuntime> runtime,
    std::shared_ptr<AzureClient::PageBlobClient> blob, std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    const int64_t bufferSize)
    : WriteableFileImpl(name, std::move(runtime), blob, std::move(fileCache), std::move(logger), bufferSize,
                        // Braced initialisation guarantees GetSize runs before GetCapacity.
                        BlobState{BlobOperations::GetSize(*blob), BlobOperations::GetCapacity(*blob)}) {}

WriteableFileImpl::WriteableFileImpl(
    const std::string_view name, std::shared_ptr<ClientRuntime> runtime,
    std::shared_ptr<AzureClient::PageBlobClient> blob, std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    const int64_t bufferSize, const BlobState knownState)
    : m_name(name), m_bufferSize(bufferSize), m_runtime(std::move(runtime)), m_blob(std::move(blob)),
      m_fileCache(std::move(fileCache)), m_logger(std::move(logger)), m_lastPageOffset(0), m_size(knownState.Size),
      m_capacity(knownState.Capacity), m_bufferOffset(0), m_closed(false), m_flushed(true) {
    if (m_bufferSize < Configuration::PageBlob::PageSize) {
        throw std::invalid_argument("Buffer size cannot be smaller than a page");
    }

    // Asynchronous flushes upload whole pages only, so a page-aligned buffer guarantees a full page is always
    // available whenever the buffer runs out of space.
    m_bufferSize = (m_bufferSize / Configuration::PageBlob::PageSize) * Configuration::PageBlob::PageSize;

    assert(m_bufferSize > 0);
    m_buffer.resize(static_cast<size_t>(m_bufferSize));
    if (m_size > 0) // Existing file with data
    {
        int64_t lastPageBytes;
        int64_t lastPageOffset;
        std::tie(lastPageBytes, lastPageOffset) = BlobHelpers::RoundToBeginningOfNearestPage(m_size);
        m_lastPageOffset = lastPageOffset;
        if (lastPageBytes > 0) // There is a partially filled page
        {
            [[maybe_unused]] const auto bytesDownloaded =
                BlobOperations::DownloadToBuffer(*m_blob, m_buffer, m_lastPageOffset, lastPageBytes);
            assert(bytesDownloaded == lastPageBytes);
            m_bufferOffset = lastPageBytes;
            m_flushed = false; // We have existing partial page data in buffer
        }
    }
}

WriteableFileImpl::~WriteableFileImpl() {
    for (int i = 0; i < 5; i++) {
        if (i > 0) {
            BOOST_LOG_SEV(*m_logger, debug) << "Retrying to close file '" << m_name << "'. Attempt " << i << " of 5";
        }
        try {
            Close();
            break;
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_logger, warning) << "Failed to close file '" << m_name << "' on attempt " << i;
            BOOST_LOG_SEV(*m_logger, warning) << "Exception details: " << e.what();
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, warning) << "Failed to close file '" << m_name << "' on attempt " << i;
        }
        // A failed upload is sticky, so retrying can only fail the same way.
        if (HasUploadError()) {
            break;
        }
    }

    // Outstanding upload completions use the blob client and logger, which are released below.
    DrainUploads();
}

bool WriteableFileImpl::HasUploadError() const noexcept {
    if (!m_uploads) {
        return false;
    }
    std::scoped_lock lock(m_uploads->Mutex);
    return static_cast<bool>(m_uploads->Error);
}

void WriteableFileImpl::DrainUploads() noexcept {
    if (!m_uploads) {
        return;
    }
    std::unique_lock lock(m_uploads->Mutex);
    m_uploads->Done.wait(lock, [&] { return m_uploads->InFlight == 0; });
}

WriteableFileImpl::WriteableFileImpl(WriteableFileImpl&& other) noexcept
    : m_name(std::move(other.m_name)), m_bufferSize(other.m_bufferSize), m_runtime(std::move(other.m_runtime)),
      m_blob(std::move(other.m_blob)), m_fileCache(std::move(other.m_fileCache)), m_logger(std::move(other.m_logger)),
      m_lastPageOffset(other.m_lastPageOffset), m_size(other.m_size), m_capacity(other.m_capacity),
      m_bufferOffset(other.m_bufferOffset), m_closed(std::exchange(other.m_closed, true)), m_flushed(other.m_flushed),
      m_unsyncedSize(other.m_unsyncedSize), m_buffer(std::move(other.m_buffer)), m_uploads(std::move(other.m_uploads)) {
}

WriteableFileImpl& WriteableFileImpl::operator=(WriteableFileImpl&& other) noexcept {
    m_name = std::move(other.m_name);
    m_bufferSize = other.m_bufferSize;
    m_blob = std::move(other.m_blob);
    m_runtime = std::move(other.m_runtime);
    m_fileCache = std::move(other.m_fileCache);
    m_logger = std::move(other.m_logger);
    m_lastPageOffset = other.m_lastPageOffset;
    m_size = other.m_size;
    m_capacity = other.m_capacity;
    m_bufferOffset = other.m_bufferOffset;
    m_closed = std::exchange(other.m_closed, true);
    m_flushed = other.m_flushed;
    m_unsyncedSize = other.m_unsyncedSize;
    m_buffer = std::move(other.m_buffer);
    m_uploads = std::move(other.m_uploads);
    return *this;
}

void WriteableFileImpl::Close() {
    if (!m_closed) {
        Sync();
        m_closed = true;
    }
}

void WriteableFileImpl::Append(const std::span<const char> data) {
    const char* dataPos = data.data();
    auto dataSize = static_cast<int64_t>(data.size());
    while (dataSize > 0) {
        const auto spaceLeft = m_bufferSize - m_bufferOffset;
        if (spaceLeft < Configuration::PageBlob::PageSize) {
            StartFlush(false);
            continue;
        }

        auto bufPos = &m_buffer[static_cast<size_t>(m_bufferOffset)];
        const auto bytesToCopy = std::min(spaceLeft, dataSize);
        std::copy(dataPos, dataPos + bytesToCopy, bufPos);

        dataSize -= bytesToCopy;
        m_bufferOffset += bytesToCopy;
        dataPos += bytesToCopy;
        m_size += bytesToCopy;
        m_flushed = false; // Mark as not flushed since we added new data
        m_unsyncedSize = true;
    }
}

void WriteableFileImpl::Flush() {
    StartFlush(true);
    WaitForUploads(0);
}

void WriteableFileImpl::RangeSync() { StartFlush(false); }

// Uploads the buffered data without waiting for it; Flush, Sync and Close wait. Asynchronous flushes send complete
// pages only and keep the trailing partial page buffered: uploading it zero-padded and again later with more data
// would put two overlapping requests in flight, and Azure does not order them. Only the final flush, which is
// always followed by WaitForUploads(0), uploads the padded tail.
void WriteableFileImpl::StartFlush(const bool includePartialPage) {
    if (m_bufferOffset == 0 || m_flushed) {
        return;
    }

    constexpr auto pageSize = Configuration::PageBlob::PageSize;
    const auto fullBytes = (m_bufferOffset / pageSize) * pageSize;
    const auto tail = m_bufferOffset - fullBytes;
    const auto bytesToWrite = (includePartialPage && tail != 0) ? fullBytes + pageSize : fullBytes;
    if (bytesToWrite == 0) {
        return;
    }

    if ((m_lastPageOffset + bytesToWrite) > m_capacity) {
        // Resizing the blob while uploads are in flight would race with them.
        WaitForUploads(0);
        Expand(m_lastPageOffset + bytesToWrite);
    }

    // Back-pressure: also surfaces an earlier upload failure before more data is accepted.
    WaitForUploads(MaxInFlightUploads - 1);
    if (includePartialPage && tail != 0) {
        // The zero padding past the tail is part of the page being uploaded.
        std::fill(m_buffer.begin() + m_bufferOffset, m_buffer.begin() + bytesToWrite, '\0');
    }
    // The full buffer becomes the upload payload as-is and a recycled buffer takes its place; only the partial-page
    // tail (under one page) is copied. A pooled buffer returns to the pool when the last reference to the payload,
    // held by the in-flight request, is released.
    std::vector<char> next = RentBuffer();
    std::copy(m_buffer.begin() + fullBytes, m_buffer.begin() + m_bufferOffset, next.begin());
    std::swap(m_buffer, next);
    next.resize(static_cast<size_t>(bytesToWrite));

    // Owns an upload payload and hands its storage back to the pool when the last reference is released.
    struct PooledPayload {
        std::vector<char> Data;
        std::shared_ptr<UploadTracker> Pool;

        PooledPayload(std::vector<char> data, std::shared_ptr<UploadTracker> pool)
            : Data(std::move(data)), Pool(std::move(pool)) {}
        PooledPayload(const PooledPayload&) = delete;
        PooledPayload& operator=(const PooledPayload&) = delete;
        ~PooledPayload() {
            if (!Pool) {
                return;
            }
            std::scoped_lock lock(Pool->Mutex);
            if (Pool->Free.size() < MaxInFlightUploads) {
                Pool->Free.push_back(std::move(Data));
            }
        }
    };
    const auto owner = std::make_shared<PooledPayload>(std::move(next), m_uploads);
    try {
        StartUpload(std::shared_ptr<const std::vector<char>>(owner, &owner->Data), m_lastPageOffset);
    } catch (...) {
        // Nothing was accepted, so the data stays buffered for a retry.
        owner->Data.resize(static_cast<size_t>(m_bufferSize));
        std::swap(m_buffer, owner->Data);
        owner->Pool.reset();
        throw;
    }
    BOOST_LOG_SEV(*m_logger, debug) << "Flushed " << bytesToWrite << " bytes to writeable file '" << m_name << "'.";
    m_lastPageOffset += fullBytes;
    m_bufferOffset = tail;
    // The tail only counts as flushed once it has been uploaded.
    m_flushed = tail == 0 || includePartialPage;
}

void WriteableFileImpl::Sync() {
    // Every metadata write changes the blob's ETag and invalidates open readers, so a Sync with nothing new to
    // publish (e.g. Sync followed by Close) must not write the size again.
    if (m_unsyncedSize && m_fileCache) {
        m_fileCache->MarkFileAsStaleIfExists(m_name);
    }

    StartFlush(true);
    WaitForUploads(0);
    if (m_unsyncedSize) {
        BlobOperations::SetSize(*m_runtime, *m_blob, m_size);
        m_unsyncedSize = false;
    }
    BOOST_LOG_SEV(*m_logger, debug) << "Synced writeable file '" << m_name << "' to " << m_size << " bytes";
}

// Uploads of distinct page ranges are independent, so up to MaxInFlightUploads overlap. The payload is a buffer the
// caller no longer touches, so it is sent without a copy.
void WriteableFileImpl::StartUpload(std::shared_ptr<const std::vector<char>> data, const int64_t offset) {
    {
        std::scoped_lock lock(m_uploads->Mutex);
        ++m_uploads->InFlight;
    }

    auto tracker = m_uploads;
    try {
        BlobOperations::UploadPagesAsync(*m_runtime, *m_blob, std::move(data), offset,
                                         [tracker](std::exception_ptr error) {
                                             {
                                                 std::scoped_lock lock(tracker->Mutex);
                                                 --tracker->InFlight;
                                                 if (error && !tracker->Error) {
                                                     tracker->Error = error;
                                                 }
                                             }
                                             tracker->Done.notify_all();
                                         });
    } catch (...) {
        std::scoped_lock lock(tracker->Mutex);
        --tracker->InFlight;
        throw;
    }
}

std::vector<char> WriteableFileImpl::RentBuffer() const {
    std::vector<char> buffer;
    {
        std::scoped_lock lock(m_uploads->Mutex);
        if (!m_uploads->Free.empty()) {
            buffer = std::move(m_uploads->Free.back());
            m_uploads->Free.pop_back();
        }
    }
    // A buffer that carried a short final page was trimmed; growing it zero-fills only the missing part.
    buffer.resize(static_cast<size_t>(m_bufferSize));
    return buffer;
}

// Blocks the calling (RocksDB) thread, never an io_context thread. The first upload failure is sticky: the data of a
// failed upload is gone, so every later Sync/Close must keep reporting it rather than claim durability.
void WriteableFileImpl::WaitForUploads(const size_t maxRemaining) {
    std::unique_lock lock(m_uploads->Mutex);
    m_uploads->Done.wait(lock, [&] { return m_uploads->InFlight <= maxRemaining; });
    if (m_uploads->Error) {
        std::rethrow_exception(m_uploads->Error);
    }
}

void WriteableFileImpl::Truncate(int64_t size) {
    // Truncate only allows shrinking, not expanding
    if (size > m_size) {
        throw std::invalid_argument("Truncate can only shrink the file. Cannot expand from " + std::to_string(m_size) +
                                    " to " + std::to_string(size) + " bytes.");
    }

    // Ensure all data is written to blob before modifications are made
    Sync();

    const auto [partialPageSize, totalPageOffset] = BlobHelpers::RoundToBeginningOfNearestPage(size);
    m_bufferOffset = 0;
    m_lastPageOffset = totalPageOffset;
    m_flushed = true; // Buffer is empty after truncate

    if (partialPageSize != 0) {
        // Read the partial page into memory for further appends
        [[maybe_unused]] const auto bytesDownloaded =
            BlobOperations::DownloadToBuffer(*m_blob, m_buffer, totalPageOffset, partialPageSize);
        assert(bytesDownloaded == partialPageSize);
        m_bufferOffset = partialPageSize;
        m_flushed = false; // We have data in buffer now
    }

    m_size = size;
    BlobOperations::SetSize(*m_runtime, *m_blob, m_size);

    // Calculate new capacity rounded up to page size
    const auto [_, newCapacity] = BlobHelpers::RoundToEndOfNearestPage(size);
    m_capacity = newCapacity;
    BlobOperations::SetCapacity(*m_runtime, *m_blob, newCapacity);
}

int64_t WriteableFileImpl::GetFileSize() const noexcept { return m_size; }

int64_t WriteableFileImpl::GetUniqueId(char* id, const int64_t maxIdSize) const noexcept {
    const auto length = std::min(static_cast<int64_t>(m_name.size()), maxIdSize);
    std::copy_n(m_name.begin(), length, id);
    return length;
}

// Doubling alone under-sizes the blob when the capacity is zero (after Truncate(0)) or smaller than the pending
// write, so the pending write itself must always fit.
void WriteableFileImpl::Expand(const int64_t requiredCapacity) {
    // TODO: Consider expanding by less for large files.
    const auto wanted = std::max({requiredCapacity, m_capacity * 2, Configuration::PageBlob::DefaultSize});
    const auto [_, desiredSize] = BlobHelpers::RoundToEndOfNearestPage(wanted);

    BOOST_LOG_SEV(*m_logger, debug) << "Expanding writeable file '" << m_name << "' to " << desiredSize << " bytes";

    BlobOperations::SetCapacity(*m_runtime, *m_blob, desiredSize);
    m_capacity = desiredSize;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
