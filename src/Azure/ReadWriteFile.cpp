// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <boost/log/trivial.hpp>
#include <cassert>
#include <limits>
namespace AVEVA::RocksDB::Plugin::Azure {
using namespace boost::log::trivial;
ReadWriteFile::ReadWriteFile(
    Impl::ReadWriteFileImpl&& file,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_file(std::make_unique<Impl::ReadWriteFileImpl>(std::move(file))), m_logger(std::move(logger)) {}

ReadWriteFile::~ReadWriteFile() = default;

rocksdb::IOStatus ReadWriteFile::Write(uint64_t offset, const rocksdb::Slice& data, const rocksdb::IOOptions&,
                                       rocksdb::IODebugContext*) {
    return AzureErrorTranslator::Invoke(*m_logger, "Write", [&] {
        if (!Impl::RangeCheck::IsValid(offset, data.size())) {
            return rocksdb::IOStatus::InvalidArgument("Write offset or size is out of range");
        }

        m_file->Write(static_cast<int64_t>(offset), data.data(), static_cast<int64_t>(data.size()));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus ReadWriteFile::Read(uint64_t offset, size_t n, const rocksdb::IOOptions&, rocksdb::Slice* result,
                                      char* scratch, rocksdb::IODebugContext*) const {
    return AzureErrorTranslator::Invoke(*m_logger, "Read", [&] {
        if (!Impl::RangeCheck::IsValid(offset, n)) {
            return rocksdb::IOStatus::InvalidArgument("Read offset or size is out of range");
        }

        const auto bytesRead = m_file->Read(static_cast<int64_t>(offset), static_cast<int64_t>(n), scratch);

        assert(bytesRead >= 0 && "Bytes read should not be negative");
        assert(bytesRead <= static_cast<int64_t>(n) && "Bytes read should not exceed requested amount");

        result->data_ = scratch;
        result->size_ = static_cast<size_t>(bytesRead);
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus ReadWriteFile::Flush(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return AzureErrorTranslator::Invoke(*m_logger, "Flush", [&] {
        m_file->Flush();
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus ReadWriteFile::Sync(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return AzureErrorTranslator::Invoke(*m_logger, "Sync", [&] {
        m_file->Sync();
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus ReadWriteFile::Close(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return AzureErrorTranslator::Invoke(*m_logger, "Close", [&] {
        m_file->Close();
        return rocksdb::IOStatus::OK();
    });
}
} // namespace AVEVA::RocksDB::Plugin::Azure
