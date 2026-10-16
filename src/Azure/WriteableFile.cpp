// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"

#include <boost/log/trivial.hpp>
#include <cassert>
#include <limits>
namespace AVEVA::RocksDB::Plugin::Azure {
using namespace boost::log::trivial;

WriteableFile::WriteableFile(
    Impl::WriteableFileImpl&& file,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_file(std::make_unique<Impl::WriteableFileImpl>(std::move(file))), m_logger(std::move(logger)) {}

WriteableFile::~WriteableFile() = default;

rocksdb::IOStatus WriteableFile::Append(const rocksdb::Slice& data, const rocksdb::IOOptions&,
                                        rocksdb::IODebugContext*) {
    try {
        m_file->Append(std::span(data.data(), data.size()));
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Append");
    }

    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus WriteableFile::Close(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    try {
        m_file->Close();
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Close");
    }

    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus WriteableFile::Flush(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    try {
        m_file->Flush();
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Flush");
    }

    return rocksdb::IOStatus::OK();
}

// RocksDB calls this every `bytes_per_sync` bytes while writing an SST. Starting the page uploads now (without waiting
// for them) spreads the transfer across the write instead of bursting it at the final Sync. Durability is still only
// established by Sync/Close, so strict mode defers to Sync.
rocksdb::IOStatus WriteableFile::RangeSync(uint64_t, uint64_t, const rocksdb::IOOptions& options,
                                           rocksdb::IODebugContext* dbg) {
    if (strict_bytes_per_sync_) {
        return Sync(options, dbg);
    }

    try {
        m_file->RangeSync();
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "RangeSync");
    }

    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus WriteableFile::Sync(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    try {
        m_file->Sync();
    } catch (...) {
        return AzureErrorTranslator::FromCurrentException(*m_logger, "Sync");
    }

    return rocksdb::IOStatus::OK();
}

uint64_t WriteableFile::GetFileSize(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    // The writer tracks its own size, so this never needs a round-trip to Azure and cannot fail: a transient service
    // error must not make RocksDB believe the file is empty.
    static_assert(noexcept(std::declval<const Impl::WriteableFileImpl&>().GetFileSize()));
    const auto fileSize = m_file->GetFileSize();
    assert(fileSize >= 0 && "File size should not be negative");
    return static_cast<uint64_t>(fileSize);
}
} // namespace AVEVA::RocksDB::Plugin::Azure
