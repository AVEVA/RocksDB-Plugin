// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <rocksdb/file_system.h>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class SequentialFileImpl;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

namespace AVEVA::RocksDB::Plugin::Azure {
class SequentialFile final : public rocksdb::FSSequentialFile {
    std::unique_ptr<Impl::SequentialFileImpl> m_file;

  public:
    explicit SequentialFile(Impl::SequentialFileImpl&& file);
    ~SequentialFile() override;

    rocksdb::IOStatus Read(size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result, char* scratch,
                           rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus Skip(uint64_t n) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
