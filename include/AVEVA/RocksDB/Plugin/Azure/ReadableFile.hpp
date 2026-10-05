// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include <rocksdb/file_system.h>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure {
class ReadableFile final : public rocksdb::FSSequentialFile, public rocksdb::FSRandomAccessFile {
    // Shared with in-flight async reads so the file (and its blob client) outlives their completions.
    std::shared_ptr<Impl::ReadableFileImpl> m_file;

  public:
    explicit ReadableFile(Impl::ReadableFileImpl file);

    virtual rocksdb::IOStatus Read(size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result, char* scratch,
                                   rocksdb::IODebugContext* dbg) override;
    virtual rocksdb::IOStatus Read(uint64_t offset, size_t n, const rocksdb::IOOptions& options, rocksdb::Slice* result,
                                   char* scratch, rocksdb::IODebugContext* dbg) const override;
    virtual rocksdb::IOStatus ReadAsync(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                        std::function<void(rocksdb::FSReadRequest&, void*)> cb, void* cb_arg,
                                        void** io_handle, rocksdb::IOHandleDeleter* del_fn,
                                        rocksdb::IODebugContext* dbg) override;
    virtual rocksdb::IOStatus Skip(uint64_t n) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
