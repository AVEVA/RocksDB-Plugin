// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure {
using Impl::AsyncReadHandle;
using Impl::AsyncReadRequest;
using Impl::DeleteAsyncReadHandle;
namespace {
rocksdb::IOStatus StatusFromException(const std::exception_ptr& error) {
    try {
        std::rethrow_exception(error);
    } catch (const RequestFailedException& e) {
        return AzureErrorTranslator::IOStatusFromError(e);
    } catch (const std::exception& e) {
        return rocksdb::IOStatus::IOError(e.what());
    } catch (...) {
        return rocksdb::IOStatus::IOError("Failed to Read from file");
    }
}
} // namespace

ReadableFile::ReadableFile(Impl::ReadableFileImpl&& file)
    : m_file(std::make_shared<Impl::ReadableFileImpl>(std::move(file))) {}

ReadableFile::~ReadableFile() = default;

rocksdb::IOStatus ReadableFile::Read(const size_t n, const rocksdb::IOOptions&, rocksdb::Slice* result, char* scratch,
                                     rocksdb::IODebugContext*) {
    try {
        assert(n <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
               "size_t value exceeds int64_t max value");
        const auto bytesRead = m_file->SequentialRead(static_cast<int64_t>(n), scratch);
        assert(bytesRead >= 0 && "SequentialRead should not return negative values");
        assert(static_cast<size_t>(bytesRead) <= std::numeric_limits<size_t>::max() &&
               "bytesRead exceeds size_t max value");
        *result = rocksdb::Slice(scratch, static_cast<size_t>(bytesRead));
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}

rocksdb::IOStatus ReadableFile::Read(const uint64_t offset, const size_t n, const rocksdb::IOOptions&,
                                     rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext*) const {
    try {
        assert(offset <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
               "offset exceeds int64_t max value");
        assert(n <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
               "size_t value exceeds int64_t max value");
        const auto bytesRead = m_file->RandomRead(static_cast<int64_t>(offset), static_cast<int64_t>(n), scratch);
        assert(bytesRead >= 0 && "RandomRead should not return negative values");
        assert(static_cast<uint64_t>(bytesRead) <= std::numeric_limits<size_t>::max() &&
               "bytesRead exceeds size_t max value");
        *result = rocksdb::Slice(scratch, static_cast<size_t>(bytesRead));
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}

// Cache hits are served inline; misses start a non-blocking blob download on the host io_context. Either way
// RocksDB's callback is deferred to FileSystem::Poll/AbortIO so it always runs on a RocksDB thread.
rocksdb::IOStatus ReadableFile::ReadAsync(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                          std::function<void(rocksdb::FSReadRequest&, void*)> cb, void* cb_arg,
                                          void** io_handle, rocksdb::IOHandleDeleter* del_fn,
                                          rocksdb::IODebugContext* dbg) {
    if (io_handle == nullptr || del_fn == nullptr) {
        return FSRandomAccessFile::ReadAsync(req, opts, std::move(cb), cb_arg, io_handle, del_fn, dbg);
    }

    assert(req.offset <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
           "offset exceeds int64_t max value");
    assert(req.len <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
           "size_t value exceeds int64_t max value");
    try {
        auto request = std::make_shared<AsyncReadRequest>(req, std::move(cb), cb_arg);
        auto handle = std::make_unique<AsyncReadHandle>(AsyncReadHandle{request});
        const auto offset = static_cast<int64_t>(req.offset);
        const auto length = static_cast<int64_t>(req.len);
        // IOOptions::timeout is in microseconds; zero means "no deadline". Round up so a tiny deadline is not lost.
        const auto timeout = std::chrono::ceil<std::chrono::milliseconds>(opts.timeout);

        if (const auto cached = m_file->TryReadFromCache(offset, length, req.scratch)) {
            // The cache wrote straight into scratch; Complete only records the length.
            request->Complete(rocksdb::IOStatus::OK(), std::string_view(req.scratch, *cached));
        } else {
            auto download = [file = m_file, offset, length, timeout, request] {
                Impl::ReadableFileImpl::ReadAsync(
                    file, offset, length,
                    [request](std::exception_ptr error, std::string data) {
                        request->Complete(error ? StatusFromException(error) : rocksdb::IOStatus::OK(), data);
                    },
                    Impl::ReadableFileImpl::kMaxStaleReadRetries, timeout);
            };

            // A prefetch of this range may still be downloading; waiting for it beats downloading it twice.
            std::move_only_function<void()> resume = [file = m_file, offset, length, scratch = req.scratch, request,
                                                      download] {
                if (const auto cached = file->TryReadFromCache(offset, length, scratch)) {
                    request->Complete(rocksdb::IOStatus::OK(), std::string_view(scratch, *cached));
                } else {
                    download();
                }
            };
            if (!m_file->ChainOntoPendingPrefetch(offset, length, resume)) {
                download();
            }
        }

        *io_handle = handle.release();
        *del_fn = DeleteAsyncReadHandle;
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}

// The default MultiRead issues one blocking round trip per request. Starting every download before waiting lets them
// overlap on the host io_context, so a batch costs roughly one round trip. Per-request failures are reported in
// each request's status, as RocksDB expects; the returned status only covers batch-level problems.
rocksdb::IOStatus ReadableFile::MultiRead(rocksdb::FSReadRequest* reqs, const size_t num_reqs,
                                          const rocksdb::IOOptions& opts, rocksdb::IODebugContext* dbg) {
    // A large batch must not open an unbounded number of connections, so requests run in bounded waves.
    constexpr size_t maxInFlight = 32;
    rocksdb::IOStatus batchStatus = rocksdb::IOStatus::OK();

    for (size_t begin = 0; begin < num_reqs; begin += maxInFlight) {
        const size_t end = std::min(num_reqs, begin + maxInFlight);
        std::vector<void*> handles;
        handles.reserve(end - begin);
        // Deleting a handle aborts a request still in flight, so the handles are released even on an early exit.
        struct HandleCleanup {
            std::vector<void*>& Handles;
            ~HandleCleanup() {
                for (auto* handle : Handles) {
                    DeleteAsyncReadHandle(handle);
                }
            }
        } cleanup{handles};

        for (size_t i = begin; i < end; ++i) {
            auto* target = &reqs[i];
            void* handle = nullptr;
            rocksdb::IOHandleDeleter deleter = nullptr;
            auto started = ReadAsync(
                *target, opts,
                [target](rocksdb::FSReadRequest& done, void*) {
                    target->status = done.status;
                    target->result = done.result;
                },
                nullptr, &handle, &deleter, dbg);
            if (!started.ok()) {
                target->status = started;
                target->result = rocksdb::Slice();
                continue;
            }
            handles.push_back(handle);
        }

        if (auto waited = Impl::PollAsyncReads(handles); !waited.ok() && batchStatus.ok()) {
            batchStatus = waited;
        }
    }

    return batchStatus;
}

// A hint: starts a background download into a per-file buffer that later reads of that range are served from.
rocksdb::IOStatus ReadableFile::Prefetch(const uint64_t offset, const size_t n, const rocksdb::IOOptions&,
                                         rocksdb::IODebugContext*) {
    if (offset > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        n > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        return rocksdb::IOStatus::InvalidArgument("Prefetch range exceeds int64_t");
    }

    try {
        if (!Impl::ReadableFileImpl::Prefetch(m_file, static_cast<int64_t>(offset), static_cast<int64_t>(n))) {
            return rocksdb::IOStatus::NotSupported("Prefetch declined; another prefetch is pending or over budget");
        }
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}

rocksdb::IOStatus ReadableFile::Skip(const uint64_t n) {
    try {
        assert(n <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
               "skip value exceeds int64_t max value");
        m_file->Skip(static_cast<int64_t>(n));
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
