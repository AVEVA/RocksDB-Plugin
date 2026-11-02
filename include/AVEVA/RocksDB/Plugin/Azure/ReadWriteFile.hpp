// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class ReadWriteFileImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure {
class BlobFilesystem;

class ReadWriteFile final : public rocksdb::FSRandomRWFile {
    friend class BlobFilesystem; // the only creator
    std::unique_ptr<Impl::ReadWriteFileImpl> m_file;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    ReadWriteFile(Impl::ReadWriteFileImpl&& file,
                  std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);

  public:
    // Close() is the only durability boundary. The destructor does not flush or retry: it logs an error if buffered
    // data was never persisted.
    ~ReadWriteFile() override;
    virtual rocksdb::IOStatus Write(uint64_t offset, const rocksdb::Slice& data, const rocksdb::IOOptions& options,
                                    rocksdb::IODebugContext* dbg) override;
    virtual rocksdb::IOStatus Read(uint64_t offset, size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result,
                                   char* scratch, rocksdb::IODebugContext* dbg) const override;
    virtual rocksdb::IOStatus Flush(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
    virtual rocksdb::IOStatus Sync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
    virtual rocksdb::IOStatus Close(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
