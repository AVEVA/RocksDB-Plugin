// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include <rocksdb/file_system.h>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class ReadableFile final : public rocksdb::FSSequentialFile, public rocksdb::FSRandomAccessFile
    {
        Impl::ReadableFileImpl m_file;
    public:
        explicit ReadableFile(Impl::ReadableFileImpl file);

        virtual rocksdb::IOStatus Read(size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Read(uint64_t offset, size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext* dbg) const override;
        virtual rocksdb::IOStatus Skip(uint64_t n) override;
    };
}
