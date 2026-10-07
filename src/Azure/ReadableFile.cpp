// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <chrono>
#include <cassert>
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
            Impl::ReadableFileImpl::ReadAsync(
                m_file, offset, length, [request](std::exception_ptr error, std::string data) {
                    request->Complete(error ? StatusFromException(error) : rocksdb::IOStatus::OK(), data);
                },
                Impl::ReadableFileImpl::kMaxStaleReadRetries, timeout);
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
    std::vector<void*> handles;
    handles.reserve(num_reqs);
    // Deleting a handle aborts a request still in flight, so the handles are released even on an early exit.
    struct HandleCleanup {
        std::vector<void*>& Handles;
        ~HandleCleanup() {
            for (auto* handle : Handles) {
                DeleteAsyncReadHandle(handle);
            }
        }
    } cleanup{handles};

    for (size_t i = 0; i < num_reqs; ++i) {
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

    return Impl::PollAsyncReads(handles);
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
