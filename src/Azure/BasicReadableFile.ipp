// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "BasicReadableFile.hpp"

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

template <class TImpl>
BasicReadableFile<TImpl>::BasicReadableFile(
    TImpl&& file, std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_file(std::make_shared<TImpl>(std::move(file))),
      m_logger(logger
                   ? std::move(logger)
                   : std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>()) {
}

template <class TImpl> BasicReadableFile<TImpl>::~BasicReadableFile() = default;

template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::Read(const uint64_t offset, const size_t n, const rocksdb::IOOptions&,
                                                 rocksdb::Slice* result, char* scratch,
                                                 rocksdb::IODebugContext*) const {
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
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Read");
    }
}

// Cache hits are served inline; misses start a non-blocking blob download on the host io_context. Either way
// RocksDB's callback is deferred to FileSystem::Poll/AbortIO so it always runs on a RocksDB thread.
template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::ReadAsync(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                                      std::function<void(rocksdb::FSReadRequest&, void*)> cb,
                                                      void* cb_arg, void** io_handle, rocksdb::IOHandleDeleter* del_fn,
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
            auto download = [file = m_file, offset, length, timeout, request, logger = m_logger] {
                TImpl::ReadAsync(
                    file, offset, length,
                    [request, logger](std::exception_ptr error, std::string_view data) {
                        request->Complete(error ? AzureErrorTranslator::FromException(*logger, error, "ReadAsync")
                                                : rocksdb::IOStatus::OK(),
                                          data);
                    },
                    TImpl::kMaxStaleReadRetries, timeout);
            };

            // A prefetch of this range may still be downloading; waiting for it beats downloading it twice. The
            // waiter runs on the host io_context, so it only looks at the in-memory prefetch: the file cache was
            // already consulted above and reading it again would put blocking disk I/O on that thread.
            std::move_only_function<void()> resume = [file = m_file, offset, length, scratch = req.scratch, request,
                                                      download] {
                if (const auto prefetched = file->TryReadFromCompletedPrefetch(offset, length, scratch)) {
                    request->Complete(rocksdb::IOStatus::OK(), std::string_view(scratch, *prefetched));
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
        return AzureErrorTranslator::FromCurrentException(*m_logger, "ReadAsync");
    }
}

// The default MultiRead issues one blocking round trip per request. Starting every download before waiting lets them
// overlap on the host io_context, so a batch costs roughly one round trip. Per-request failures are reported in
// each request's status, as RocksDB expects; the returned status only covers batch-level problems.
template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::MultiRead(rocksdb::FSReadRequest* reqs, const size_t num_reqs,
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
template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::Prefetch(const uint64_t offset, const size_t n, const rocksdb::IOOptions&,
                                                     rocksdb::IODebugContext*) {
    if (offset > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        n > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        return rocksdb::IOStatus::InvalidArgument("Prefetch range exceeds int64_t");
    }

    try {
        if (!TImpl::Prefetch(m_file, static_cast<int64_t>(offset), static_cast<int64_t>(n))) {
            return rocksdb::IOStatus::NotSupported("Prefetch declined; another prefetch is pending or over budget");
        }
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Prefetch");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
