#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"

#include <boost/log/sources/logger.hpp>
#include <rocksdb/file_system.h>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class ReadWriteFile final : public rocksdb::FSRandomRWFile
    {
        Impl::ReadWriteFileImpl m_file;
        std::shared_ptr<boost::log::sources::logger_mt> m_logger;

    public:
        ReadWriteFile(Impl::ReadWriteFileImpl file, std::shared_ptr<boost::log::sources::logger_mt> logger);
        virtual rocksdb::IOStatus Write(uint64_t offset, const rocksdb::Slice& data, const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Read(uint64_t offset, size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext* dbg) const override;
        virtual rocksdb::IOStatus Flush(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Sync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Close(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
    };
}
