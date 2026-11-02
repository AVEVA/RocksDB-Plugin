// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class DirectoryImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure {
class Directory final : public rocksdb::FSDirectory {
    std::unique_ptr<Impl::DirectoryImpl> m_directory;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

  public:
    Directory(Impl::DirectoryImpl&& directory,
              std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
    ~Directory() override;
    virtual rocksdb::IOStatus Fsync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
    virtual size_t GetUniqueId(char* id, size_t max_size) const override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
