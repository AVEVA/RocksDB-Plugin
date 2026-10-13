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
namespace {
rocksdb::IOStatus StatusFromException(const std::exception_ptr& error) {
    return AzureErrorTranslator::FromException(error);
}
} // namespace

SequentialFile::SequentialFile(Impl::SequentialFileImpl&& file)
    : m_file(std::make_unique<Impl::SequentialFileImpl>(std::move(file))) {}

SequentialFile::~SequentialFile() = default;

rocksdb::IOStatus SequentialFile::Read(const size_t n, const rocksdb::IOOptions&, rocksdb::Slice* result, char* scratch,
                                       rocksdb::IODebugContext*) {
    try {
        if (n > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            return rocksdb::IOStatus::InvalidArgument("Read size is out of range");
        }
        const auto bytesRead = m_file->SequentialRead(static_cast<int64_t>(n), scratch);
        assert(bytesRead >= 0 && "SequentialRead should not return negative values");
        *result = rocksdb::Slice(scratch, static_cast<size_t>(bytesRead));
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}

rocksdb::IOStatus SequentialFile::Skip(const uint64_t n) {
    try {
        if (n > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return rocksdb::IOStatus::InvalidArgument("Skip size is out of range");
        }
        m_file->Skip(static_cast<int64_t>(n));
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
