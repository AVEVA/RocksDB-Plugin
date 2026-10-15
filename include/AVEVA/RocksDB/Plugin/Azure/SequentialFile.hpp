// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class SequentialFileImpl;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

namespace AVEVA::RocksDB::Plugin::Azure {
class SequentialFile final : public rocksdb::FSSequentialFile {
    std::unique_ptr<Impl::SequentialFileImpl> m_file;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

  public:
    SequentialFile(
        Impl::SequentialFileImpl&& file,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
    ~SequentialFile() override;

    rocksdb::IOStatus Read(size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result, char* scratch,
                           rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus Skip(uint64_t n) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
