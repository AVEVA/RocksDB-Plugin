// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <functional>
#include <memory>
#include <string_view>

namespace AVEVA::RocksDB::Plugin::Azure {
// Adapts a readable file implementation to RocksDB. A template only so the tests can run it over a fake blob client;
// the production instantiation is wrapped by the non-template ReadableFile.
template <class TImpl> class BasicReadableFile final : public rocksdb::FSRandomAccessFile {
    // Shared with in-flight async reads so the file (and its blob client) outlives their completions.
    std::shared_ptr<TImpl> m_file;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    // ReadAsync's body; `signal`, when set, is notified as the read finishes (used by MultiRead's sliding window).
    // `deadline` is the absolute time the read must finish by, so a batch can share one budget across its reads.
    rocksdb::IOStatus StartRead(rocksdb::FSReadRequest& req, std::function<void(rocksdb::FSReadRequest&, void*)> cb,
                                void* cb_arg, void** io_handle, rocksdb::IOHandleDeleter* del_fn,
                                std::shared_ptr<Impl::AsyncReadSignal> signal, Impl::ReadDeadline deadline);

  public:
    // MultiRead keeps at most this many blob downloads outstanding, so a large batch cannot open unbounded
    // connections.
    static constexpr size_t kMultiReadMaxInFlight = 32;
    // Also bounds the backing memory of the outstanding downloads (each group's download lease plus, when merged, its
    // merge buffer), so large groups cannot multiply into hundreds of MiB. A group is always admitted when nothing
    // else is outstanding, so one large request still makes progress.
    static constexpr uint64_t kMultiReadMaxInFlightBytes = 64 * 1024 * 1024;
    // Requests separated by at most this many bytes are fetched with one GET; the few wasted bytes cost less than
    // another round trip.
    static constexpr uint64_t kMultiReadMaxGap = 4 * 1024;
    // Upper bound of one merged GET, so coalescing cannot build an arbitrarily large buffer.
    static constexpr uint64_t kMultiReadMaxMergedBytes = 8 * 1024 * 1024;
    explicit BasicReadableFile(
        std::shared_ptr<TImpl> file,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger = nullptr);
    ~BasicReadableFile() override;

    // Synchronous reads do not honour IOOptions::timeout (a hint in RocksDB): they block until the read completes or
    // fails, bounded only by the blob client's own request timeouts and retries. ReadAsync and MultiRead are
    // deadline-aware.
    rocksdb::IOStatus Read(uint64_t offset, size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result,
                           char* scratch, rocksdb::IODebugContext* dbg) const override;
    rocksdb::IOStatus ReadAsync(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                std::function<void(rocksdb::FSReadRequest&, void*)> cb, void* cb_arg, void** io_handle,
                                rocksdb::IOHandleDeleter* del_fn, rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus MultiRead(rocksdb::FSReadRequest* reqs, size_t num_reqs, const rocksdb::IOOptions& options,
                                rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus Prefetch(uint64_t offset, size_t n, const rocksdb::IOOptions& options,
                               rocksdb::IODebugContext* dbg) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
