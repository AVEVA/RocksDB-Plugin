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
#include <cstring>
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
    return StartRead(req, opts, std::move(cb), cb_arg, io_handle, del_fn, nullptr);
}

template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::StartRead(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                                      std::function<void(rocksdb::FSReadRequest&, void*)> cb,
                                                      void* cb_arg, void** io_handle, rocksdb::IOHandleDeleter* del_fn,
                                                      std::shared_ptr<Impl::AsyncReadSignal> signal) {
    assert(req.offset <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
           "offset exceeds int64_t max value");
    assert(req.len <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
           "size_t value exceeds int64_t max value");
    try {
        auto request = std::make_shared<AsyncReadRequest>(req, std::move(cb), cb_arg, std::move(signal));
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

// The default MultiRead issues one blocking round trip per request. Starting the downloads before waiting lets them
// overlap on the host io_context, so a batch costs roughly one round trip. Requests that touch or overlap, or sit
// within kMultiReadMaxGap of each other, are fetched with one GET and split back afterwards; at most
// kMultiReadMaxInFlight GETs are outstanding, and a new one starts as soon as any finishes. Per-request failures are
// reported in each request's status, as RocksDB expects; the returned status only covers batch-level problems.
template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::MultiRead(rocksdb::FSReadRequest* reqs, const size_t num_reqs,
                                                      const rocksdb::IOOptions& opts, rocksdb::IODebugContext*) {
    // One merged download: the member requests it serves (indexes into reqs, ordered by offset) and, when it serves
    // more than one, the buffer the merged range is fetched into.
    struct Group {
        std::vector<size_t> Members;
        uint64_t Start = 0;
        uint64_t End = 0;
        std::vector<char> Buffer;
    };

    std::vector<size_t> order(num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
        order[i] = i;
    }
    std::ranges::sort(order, [&](const size_t a, const size_t b) { return reqs[a].offset < reqs[b].offset; });

    std::vector<Group> groups;
    for (const auto index : order) {
        const auto& req = reqs[index];
        const uint64_t end = req.offset + req.len;
        if (!groups.empty()) {
            auto& last = groups.back();
            if (req.offset <= last.End + kMultiReadMaxGap &&
                std::max(last.End, end) - last.Start <= kMultiReadMaxMergedBytes) {
                last.Members.push_back(index);
                last.End = std::max(last.End, end);
                continue;
            }
        }
        groups.push_back(Group{.Members = {index}, .Start = req.offset, .End = end, .Buffer = {}});
    }

    struct Active {
        void* Handle;
    };
    std::vector<Active> active;
    // Deleting a handle aborts a request still in flight, so the handles are released even on an early exit. Declared
    // after `groups` so it runs first: an aborted request never touches its group's buffer afterwards.
    struct HandleCleanup {
        std::vector<Active>& Handles;
        ~HandleCleanup() {
            for (auto& entry : Handles) {
                DeleteAsyncReadHandle(entry.Handle);
            }
        }
    } cleanup{active};

    auto signal = std::make_shared<Impl::AsyncReadSignal>();
    rocksdb::IOStatus batchStatus = rocksdb::IOStatus::OK();
    size_t nextGroup = 0;

    auto start = [&](Group& group) {
        const bool single = group.Members.size() == 1;
        // A merged group is fetched into its own buffer; the callback then copies each member's slice out.
        rocksdb::FSReadRequest merged;
        rocksdb::FSReadRequest* target = &reqs[group.Members.front()];
        if (!single) {
            group.Buffer.resize(group.End - group.Start);
            merged.offset = group.Start;
            merged.len = group.Buffer.size();
            merged.scratch = group.Buffer.data();
            target = &merged;
        }

        std::function<void(rocksdb::FSReadRequest&, void*)> done;
        if (single) {
            done = [target](rocksdb::FSReadRequest& finished, void*) {
                target->status = finished.status;
                target->result = finished.result;
            };
        } else {
            done = [reqs, &group](rocksdb::FSReadRequest& finished, void*) {
                for (const auto index : group.Members) {
                    auto& member = reqs[index];
                    const uint64_t relative = member.offset - group.Start;
                    size_t available = 0;
                    if (finished.status.ok() && finished.result.size() > relative) {
                        available = std::min<size_t>(member.len, finished.result.size() - relative);
                        if (available > 0) {
                            std::memcpy(member.scratch, finished.result.data() + relative, available);
                        }
                    }
                    member.status = finished.status;
                    member.result = rocksdb::Slice(member.scratch, available);
                }
            };
        }

        void* handle = nullptr;
        rocksdb::IOHandleDeleter deleter = nullptr;
        auto started = StartRead(*target, opts, std::move(done), nullptr, &handle, &deleter, signal);
        if (!started.ok()) {
            for (const auto index : group.Members) {
                reqs[index].status = started;
                reqs[index].result = rocksdb::Slice();
            }
            return;
        }
        active.push_back(Active{handle});
    };

    while (true) {
        while (active.size() < kMultiReadMaxInFlight && nextGroup < groups.size()) {
            start(groups[nextGroup++]);
        }
        if (active.empty()) {
            break;
        }

        // Read the count before scanning so a read that finishes during the scan is not missed.
        const auto seen = signal->Count();
        bool progressed = false;
        for (auto it = active.begin(); it != active.end();) {
            auto* handle = static_cast<AsyncReadHandle*>(it->Handle);
            if (!handle->Request->Finished()) {
                ++it;
                continue;
            }
            // Finished, so this delivers the callback without blocking.
            if (auto delivered = Impl::PollAsyncReads({it->Handle}); !delivered.ok() && batchStatus.ok()) {
                batchStatus = delivered;
            }
            DeleteAsyncReadHandle(it->Handle);
            it = active.erase(it);
            progressed = true;
        }
        if (!progressed) {
            signal->WaitPast(seen);
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
            // NotSupported would make RocksDB stop prefetching this file for good; a declined hint is not an error.
            return rocksdb::IOStatus::OK();
        }
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Prefetch");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
