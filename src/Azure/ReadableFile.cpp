// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "BasicReadableFile.ipp"

namespace AVEVA::RocksDB::Plugin::Azure {
template class BasicReadableFile<Impl::ReadableFileImpl>;

ReadableFile::ReadableFile(
    Impl::ReadableFileImpl&& file,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_delegate(std::make_unique<BasicReadableFile<Impl::ReadableFileImpl>>(std::move(file), std::move(logger))) {}

ReadableFile::~ReadableFile() = default;

rocksdb::IOStatus ReadableFile::Read(const uint64_t offset, const size_t n, const rocksdb::IOOptions& options,
                                     rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext* dbg) const {
    return m_delegate->Read(offset, n, options, result, scratch, dbg);
}

rocksdb::IOStatus ReadableFile::ReadAsync(rocksdb::FSReadRequest& req, const rocksdb::IOOptions& opts,
                                          std::function<void(rocksdb::FSReadRequest&, void*)> cb, void* cb_arg,
                                          void** io_handle, rocksdb::IOHandleDeleter* del_fn,
                                          rocksdb::IODebugContext* dbg) {
    return m_delegate->ReadAsync(req, opts, std::move(cb), cb_arg, io_handle, del_fn, dbg);
}

rocksdb::IOStatus ReadableFile::MultiRead(rocksdb::FSReadRequest* reqs, const size_t num_reqs,
                                          const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) {
    return m_delegate->MultiRead(reqs, num_reqs, options, dbg);
}

rocksdb::IOStatus ReadableFile::Prefetch(const uint64_t offset, const size_t n, const rocksdb::IOOptions& options,
                                         rocksdb::IODebugContext* dbg) {
    return m_delegate->Prefetch(offset, n, options, dbg);
}
} // namespace AVEVA::RocksDB::Plugin::Azure
