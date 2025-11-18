// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"

#include <boost/log/sources/logger.hpp>
#include <rocksdb/file_system.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class WriteableFile final : public rocksdb::FSWritableFile
    {
        Impl::WriteableFileImpl m_file;
        std::shared_ptr<boost::log::sources::logger_mt> m_logger;

    public:
        WriteableFile(Impl::WriteableFileImpl file, std::shared_ptr<boost::log::sources::logger_mt> logger);
        virtual rocksdb::IOStatus Append(const rocksdb::Slice& data, const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Close(const rocksdb::IOOptions&, rocksdb::IODebugContext*) override;
        virtual rocksdb::IOStatus Flush(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Sync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual uint64_t GetFileSize(const rocksdb::IOOptions&, rocksdb::IODebugContext*) override;
    };
}
