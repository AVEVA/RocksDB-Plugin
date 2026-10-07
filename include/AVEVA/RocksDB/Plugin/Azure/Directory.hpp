// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <rocksdb/file_system.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class DirectoryImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure
{
    class Directory final : public rocksdb::FSDirectory
    {
        std::unique_ptr<Impl::DirectoryImpl> m_directory;
    public:
        explicit Directory(Impl::DirectoryImpl&& directory);
        ~Directory() override;
        virtual rocksdb::IOStatus Fsync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override;
        virtual size_t GetUniqueId(char* id, size_t max_size) const override;
    };
}
