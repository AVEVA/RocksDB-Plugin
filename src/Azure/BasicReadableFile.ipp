// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "BasicReadableFile.hpp"

#include "AVEVA/AzureClient/BlockOn.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure {
using Impl::AsyncReadHandle;
using Impl::AsyncReadRequest;
using Impl::DeleteAsyncReadHandle;

template <class TImpl>
BasicReadableFile<TImpl>::BasicReadableFile(
    std::shared_ptr<TImpl> file,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_file(std::move(file)),
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
        if (!Impl::RangeCheck::IsValid(offset, n)) {
            return rocksdb::IOStatus::InvalidArgument("Read offset or size is out of range");
        }
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
    if (!cb) {
        return rocksdb::IOStatus::InvalidArgument("ReadAsync requires a callback");
    }
    return StartRead(req, opts, std::move(cb), cb_arg, io_handle, del_fn, nullptr);
}

template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::StartRead(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                                      std::function<void(rocksdb::FSReadRequest&, void*)> cb,
                                                      void* cb_arg, void** io_handle, rocksdb::IOHandleDeleter* del_fn,
                                                      std::shared_ptr<Impl::AsyncReadSignal> signal) {
    if (!Impl::RangeCheck::IsValid(req.offset, req.len)) {
        return rocksdb::IOStatus::InvalidArgument("ReadAsync offset or size is out of range");
    }
    try {
        auto request =
            std::make_shared<AsyncReadRequest>(req, std::move(cb), cb_arg, std::move(signal), m_file->ClientExecutor());
        auto handle = std::make_unique<AsyncReadHandle>(AsyncReadHandle{request});
        const auto offset = static_cast<int64_t>(req.offset);
        const auto length = static_cast<int64_t>(req.len);
        // IOOptions::timeout is in microseconds; zero means "no deadline". Round up so a tiny deadline is not lost.
        const auto timeout = std::chrono::ceil<std::chrono::milliseconds>(opts.timeout);

        if (const auto cached = m_file->TryReadFromCache(offset, length, req.scratch)) {
            // The cache wrote straight into scratch; Complete only records the length.
            request->Complete(rocksdb::IOStatus::OK(), std::string_view(req.scratch, *cached));
        } else {
            auto makeDownload = [&] {
                return [file = m_file, offset, length, timeout, request, logger = m_logger] {
                    TImpl::ReadAsync(
                        file, offset, length,
                        [request, logger](std::exception_ptr error, std::string_view data,
                                          const std::shared_ptr<const void>&) {
                            request->Complete(error ? AzureErrorTranslator::FromException(*logger, error, "ReadAsync")
                                                    : rocksdb::IOStatus::OK(),
                                              data);
                        },
                        TImpl::kMaxStaleReadRetries, timeout);
                };
            };

            // A prefetch of this range may still be downloading; waiting for it beats downloading it twice. The
            // waiter runs on the host io_context, so it only looks at the in-memory prefetch: the file cache was
            // already consulted above and reading it again would put blocking disk I/O on that thread.
            // The prefetch is copied into scratch only through TryCompleteInline, which does so under the request
            // lock: RocksDB may free scratch as soon as the handle is deleted, and an aborted request must not
            // touch it. A throw here would escape the prefetch completion into the host io_context and strand the
            // request, so it is turned into the request's error status instead.
            // The pending check is advisory, so the common case builds no waiter; if the prefetch settles in
            // between, ChainOntoPendingPrefetch declines and the download starts directly.
            bool chained = false;
            if (m_file->HasPendingPrefetch(offset, length)) {
                std::move_only_function<void()> resume = [file = m_file, offset, length, request,
                                                          download = makeDownload(), logger = m_logger]() mutable {
                    try {
                        if (!request->TryCompleteInline([&](char* scratch) {
                                return file->TryReadFromCompletedPrefetch(offset, length, scratch);
                            })) {
                            download();
                        }
                    } catch (...) {
                        request->Complete(AzureErrorTranslator::FromCurrentException(*logger, "ReadAsync"), {});
                    }
                };
                chained = m_file->ChainOntoPendingPrefetch(offset, length, resume);
            }
            if (!chained) {
                makeDownload()();
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
                                                      const rocksdb::IOOptions& opts, rocksdb::IODebugContext*) try {
    // The waits below need completions that run on the host io_context, so they would hang on one of its threads.
    try {
        AzureClient::ThrowIfRunningOn(m_file->ClientExecutor());
    } catch (const std::logic_error& error) {
        return rocksdb::IOStatus::InvalidArgument(error.what());
    }

    // One merged download: the member requests it serves (the run [FirstMember, EndMember) of `order`, which is
    // sorted by offset) and, when it serves more than one, the buffer the merged range is fetched into. The buffer is
    // left uninitialised because the download overwrites it.
    struct Group {
        size_t FirstMember = 0;
        size_t EndMember = 0;
        uint64_t Start = 0;
        uint64_t End = 0;
        std::unique_ptr<char[]> Buffer;
    };

    // Marks requests that have no outcome yet, so the batch-level handler can tell them from per-request results.
    for (size_t i = 0; i < num_reqs; ++i) {
        reqs[i].status = rocksdb::IOStatus::IOError(kMultiReadPendingMessage);
        reqs[i].result = rocksdb::Slice();
    }

    // A request whose range overflows cannot be merged or read; it fails on its own like any per-request error.
    std::vector<size_t> order;
    order.reserve(num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
        if (Impl::RangeCheck::IsValid(reqs[i].offset, reqs[i].len)) {
            order.push_back(i);
        } else {
            reqs[i].status = rocksdb::IOStatus::InvalidArgument("MultiRead offset or size is out of range");
            reqs[i].result = rocksdb::Slice();
        }
    }
    std::ranges::sort(order, [&](const size_t a, const size_t b) { return reqs[a].offset < reqs[b].offset; });

    std::vector<Group> groups;
    for (size_t position = 0; position < order.size(); ++position) {
        const auto& req = reqs[order[position]];
        const uint64_t end = req.offset + req.len;
        if (!groups.empty()) {
            auto& last = groups.back();
            if (req.offset <= last.End + kMultiReadMaxGap &&
                std::max(last.End, end) - last.Start <= kMultiReadMaxMergedBytes) {
                last.EndMember = position + 1;
                last.End = std::max(last.End, end);
                continue;
            }
        }
        groups.push_back(Group{.FirstMember = position, .EndMember = position + 1, .Start = req.offset, .End = end});
    }

    struct Active {
        void* Handle;
        Group* Owner;
    };
    std::vector<Active> active;
    // Reserved up front so push_back cannot throw after a read has started and orphan its handle.
    active.reserve(std::min<size_t>(kMultiReadMaxInFlight, groups.size()));
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
    size_t nextGroup = 0;

    auto start = [&](Group& group) {
        const bool single = group.EndMember - group.FirstMember == 1;
        // A merged group is fetched into its own buffer; the callback then copies each member's slice out.
        rocksdb::FSReadRequest merged;
        rocksdb::FSReadRequest* target = &reqs[order[group.FirstMember]];
        if (!single) {
            merged.offset = group.Start;
            merged.len = group.End - group.Start;
            group.Buffer = std::make_unique_for_overwrite<char[]>(merged.len);
            merged.scratch = group.Buffer.get();
            target = &merged;
        }

        void* handle = nullptr;
        rocksdb::IOHandleDeleter deleter = nullptr;
        // No callback: MultiRead reads the outcome straight from the finished request.
        auto started = StartRead(*target, opts, nullptr, nullptr, &handle, &deleter, signal);
        if (!started.ok()) {
            for (size_t position = group.FirstMember; position < group.EndMember; ++position) {
                reqs[order[position]].status = started;
                reqs[order[position]].result = rocksdb::Slice();
            }
            group.Buffer.reset();
            return;
        }
        active.push_back(Active{handle, &group});
    };

    // A single-member group read straight into its request's scratch; a merged one is split back out of its buffer.
    auto applyOutcome = [&](const Group& group, const rocksdb::FSReadRequest& finished) {
        if (group.EndMember - group.FirstMember == 1) {
            auto& target = reqs[order[group.FirstMember]];
            target.status = finished.status;
            target.result = finished.result;
            return;
        }
        for (size_t position = group.FirstMember; position < group.EndMember; ++position) {
            auto& member = reqs[order[position]];
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
            // Finished, so reading the outcome does not block.
            rocksdb::FSReadRequest finished;
            if (handle->Request->TryGetOutcome(finished)) {
                applyOutcome(*it->Owner, finished);
            }
            DeleteAsyncReadHandle(it->Handle);
            // The handle is gone, so nothing can write the buffer; the slices are already copied out.
            it->Owner->Buffer.reset();
            it = active.erase(it);
            progressed = true;
        }
        if (!progressed) {
            signal->WaitPast(seen);
        }
    }

    return rocksdb::IOStatus::OK();
} catch (...) {
    // RocksDB does not expect exceptions from a file. Locals (and so the in-flight handles) are already released here.
    auto status = AzureErrorTranslator::FromCurrentException(*m_logger, "MultiRead");
    // Requests without an outcome must not keep the caller's stale status or result.
    for (size_t i = 0; i < num_reqs; ++i) {
        if (reqs[i].status.IsIOError() && reqs[i].status.getState() != nullptr &&
            std::string_view(reqs[i].status.getState()) == kMultiReadPendingMessage) {
            reqs[i].status = status;
            reqs[i].result = rocksdb::Slice();
        }
    }
    return status;
}

// A hint: starts a background download into a per-file buffer that later reads of that range are served from.
template <class TImpl>
rocksdb::IOStatus BasicReadableFile<TImpl>::Prefetch(const uint64_t offset, const size_t n, const rocksdb::IOOptions&,
                                                     rocksdb::IODebugContext*) {
    if (!Impl::RangeCheck::IsValid(offset, n)) {
        return rocksdb::IOStatus::InvalidArgument("Prefetch range exceeds int64_t");
    }

    try {
        // NotSupported would make RocksDB stop prefetching this file for good; a declined hint is not an error.
        std::ignore = TImpl::Prefetch(m_file, static_cast<int64_t>(offset), static_cast<int64_t>(n));
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Prefetch");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
