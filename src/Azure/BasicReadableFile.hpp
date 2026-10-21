// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
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
    rocksdb::IOStatus StartRead(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                std::function<void(rocksdb::FSReadRequest&, void*)> cb, void* cb_arg, void** io_handle,
                                rocksdb::IOHandleDeleter* del_fn, std::shared_ptr<Impl::AsyncReadSignal> signal);

  public:
    // MultiRead keeps at most this many blob downloads outstanding, so a large batch cannot open unbounded
    // connections.
    static constexpr size_t kMultiReadMaxInFlight = 32;
    static constexpr std::string_view kMultiReadPendingMessage = "MultiRead request has no outcome yet";
    // Requests separated by at most this many bytes are fetched with one GET; the few wasted bytes cost less than
    // another round trip.
    static constexpr uint64_t kMultiReadMaxGap = 4 * 1024;
    // Upper bound of one merged GET, so coalescing cannot build an arbitrarily large buffer.
    static constexpr uint64_t kMultiReadMaxMergedBytes = 8 * 1024 * 1024;
    explicit BasicReadableFile(
        std::shared_ptr<TImpl> file,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger = nullptr);
    ~BasicReadableFile() override;

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
