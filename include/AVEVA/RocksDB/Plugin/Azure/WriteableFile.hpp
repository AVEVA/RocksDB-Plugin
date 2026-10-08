// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/log/trivial.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <rocksdb/file_system.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class WriteableFileImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure
{
    class WriteableFile final : public rocksdb::FSWritableFile
    {
        std::unique_ptr<Impl::WriteableFileImpl> m_file;
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    public:
        WriteableFile(Impl::WriteableFileImpl&& file, std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
        ~WriteableFile() override;
        virtual rocksdb::IOStatus Append(const rocksdb::Slice& data, const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Close(const rocksdb::IOOptions&, rocksdb::IODebugContext*) override;
        virtual rocksdb::IOStatus Flush(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Sync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const rocksdb::IOOptions& options,
                                            rocksdb::IODebugContext* dbg) override;
        virtual uint64_t GetFileSize(const rocksdb::IOOptions&, rocksdb::IODebugContext*) override;
    };
}
