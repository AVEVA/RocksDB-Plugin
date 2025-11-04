#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"

#include <rocksdb/file_system.h>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class Directory final : public rocksdb::FSDirectory
    {
        Impl::DirectoryImpl m_directory;
    public:
        Directory(Impl::DirectoryImpl directory);
        virtual rocksdb::IOStatus Fsync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual size_t GetUniqueId(char* id, size_t max_size) const override;
    };
}
