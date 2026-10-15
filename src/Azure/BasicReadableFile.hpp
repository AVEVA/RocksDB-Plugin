// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <functional>
#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure {
// Adapts a readable file implementation to RocksDB. A template only so the tests can run it over a fake blob client;
// the production instantiation is wrapped by the non-template ReadableFile.
template <class TImpl> class BasicReadableFile final : public rocksdb::FSRandomAccessFile {
    // Shared with in-flight async reads so the file (and its blob client) outlives their completions.
    std::shared_ptr<TImpl> m_file;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

  public:
    explicit BasicReadableFile(
        TImpl&& file,
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
