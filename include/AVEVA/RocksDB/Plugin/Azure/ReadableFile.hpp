// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <functional>
#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class ReadableFileImpl;
}

namespace AVEVA::RocksDB::Plugin::Azure {
class BlobFilesystem;

/// <summary>
/// A random access file over a page blob. The implementation (and with it Boost.Asio and the Azure client) stays
/// behind a pimpl, so this header pulls in nothing beyond RocksDB and Boost.Log.
/// </summary>
class ReadableFile final : public rocksdb::FSRandomAccessFile {
    friend class BlobFilesystem; // the only creator; instances come from BlobFilesystem::NewRandomAccessFile
    std::unique_ptr<rocksdb::FSRandomAccessFile> m_delegate;

    explicit ReadableFile(
        std::shared_ptr<Impl::ReadableFileImpl> file,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger = nullptr);

  public:
    ~ReadableFile() override;

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
