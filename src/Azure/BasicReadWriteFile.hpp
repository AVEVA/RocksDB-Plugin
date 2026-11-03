// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <cassert>
#include <limits>
#include <memory>
#include <span>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure {
// Adapts a read/write file implementation to RocksDB. A template so the implementation is injected rather than named.
template <class TImpl> class BasicReadWriteFile final : public rocksdb::FSRandomRWFile {
    // Mutable because RocksDB's Read is const while the implementation's Read may fill its buffers.
    mutable TImpl m_file;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

  public:
    BasicReadWriteFile(
        TImpl&& file,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
        : m_file(std::move(file)), m_logger(std::move(logger)) {}

    rocksdb::IOStatus Write(uint64_t offset, const rocksdb::Slice& data, const rocksdb::IOOptions&,
                            rocksdb::IODebugContext*) override {
        return AzureErrorTranslator::Invoke(*m_logger, "Write", [&] {
            if (Impl::RangeCheck::Check(offset, data.size()) == Impl::RangeCheck::Bounds::OutOfRange) {
                return rocksdb::IOStatus::InvalidArgument("Write offset or size is out of range");
            }

            m_file.Write(Core::ByteOffset{offset}, std::span<const char>(data.data(), data.size()));
            return rocksdb::IOStatus::OK();
        });
    }

    rocksdb::IOStatus Read(uint64_t offset, size_t n, const rocksdb::IOOptions&, rocksdb::Slice* result,
                           char* scratch, rocksdb::IODebugContext*) const override {
        return AzureErrorTranslator::Invoke(*m_logger, "Read", [&] {
            if (Impl::RangeCheck::Check(offset, n) == Impl::RangeCheck::Bounds::OutOfRange) {
                return rocksdb::IOStatus::InvalidArgument("Read offset or size is out of range");
            }

            const auto bytesRead = m_file.Read(Core::ByteOffset{offset}, std::span<char>(scratch, n));
            assert(bytesRead.Value() <= n && "Bytes read should not exceed requested amount");

            result->data_ = scratch;
            result->size_ = static_cast<size_t>(bytesRead.Value());
            return rocksdb::IOStatus::OK();
        });
    }

    rocksdb::IOStatus Flush(const rocksdb::IOOptions&, rocksdb::IODebugContext*) override {
        return AzureErrorTranslator::Invoke(*m_logger, "Flush", [&] {
            m_file.Flush();
            return rocksdb::IOStatus::OK();
        });
    }

    rocksdb::IOStatus Sync(const rocksdb::IOOptions&, rocksdb::IODebugContext*) override {
        return AzureErrorTranslator::Invoke(*m_logger, "Sync", [&] {
            m_file.Sync();
            return rocksdb::IOStatus::OK();
        });
    }

    // Close() is the only durability boundary: the destructor does not flush or retry.
    rocksdb::IOStatus Close(const rocksdb::IOOptions&, rocksdb::IODebugContext*) override {
        return AzureErrorTranslator::Invoke(*m_logger, "Close", [&] {
            m_file.Close();
            return rocksdb::IOStatus::OK();
        });
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure
