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
    try {
        std::rethrow_exception(error);
    } catch (const RequestFailedException& e) {
        return AzureErrorTranslator::IOStatusFromError(e);
    } catch (const std::exception& e) {
        return rocksdb::IOStatus::IOError(e.what());
    } catch (...) {
        return rocksdb::IOStatus::IOError("Failed to Read from file");
    }
}
} // namespace

SequentialFile::SequentialFile(Impl::SequentialFileImpl&& file)
    : m_file(std::make_unique<Impl::SequentialFileImpl>(std::move(file))) {}

SequentialFile::~SequentialFile() = default;

rocksdb::IOStatus SequentialFile::Read(const size_t n, const rocksdb::IOOptions&, rocksdb::Slice* result,
                                       char* scratch, rocksdb::IODebugContext*) {
    try {
        assert(n <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
               "size_t value exceeds int64_t max value");
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
        assert(n <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
               "skip value exceeds int64_t max value");
        m_file->Skip(static_cast<int64_t>(n));
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return StatusFromException(std::current_exception());
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
