// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/SequentialFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <cassert>
#include <exception>
#include <limits>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure {
SequentialFile::SequentialFile(
    Impl::SequentialFileImpl&& file,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_file(std::make_unique<Impl::SequentialFileImpl>(std::move(file))), m_logger(std::move(logger)) {}

SequentialFile::~SequentialFile() = default;

rocksdb::IOStatus SequentialFile::Read(const size_t n, const rocksdb::IOOptions&, rocksdb::Slice* result, char* scratch,
                                       rocksdb::IODebugContext*) {
    return AzureErrorTranslator::Invoke(*m_logger, "Read", [&] {
        if (n > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            return rocksdb::IOStatus::InvalidArgument("Read size is out of range");
        }
        const auto bytesRead = m_file->SequentialRead(static_cast<int64_t>(n), scratch);
        assert(bytesRead >= 0 && "SequentialRead should not return negative values");
        *result = rocksdb::Slice(scratch, static_cast<size_t>(bytesRead));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus SequentialFile::Skip(const uint64_t n) {
    return AzureErrorTranslator::Invoke(*m_logger, "Skip", [&] {
        if (n > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return rocksdb::IOStatus::InvalidArgument("Skip size is out of range");
        }
        m_file->Skip(static_cast<int64_t>(n));
        return rocksdb::IOStatus::OK();
    });
}
} // namespace AVEVA::RocksDB::Plugin::Azure
