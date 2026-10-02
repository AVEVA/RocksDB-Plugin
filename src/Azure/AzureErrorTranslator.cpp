// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
namespace AVEVA::RocksDB::Plugin::Azure {
rocksdb::IOStatus AzureErrorTranslator::IOStatusFromError(const std::string& context, unsigned int statusCode) {
    using rocksdb::IOStatus;

    rocksdb::IOStatus status;
    switch (statusCode) {
    case HttpStatus::BadRequest:
        return IOStatus::InvalidArgument(context);
    case HttpStatus::NotFound:
        return IOStatus::NotFound(context);
    case HttpStatus::RequestTimeout:
        status.SetRetryable(true);
        return IOStatus::TimedOut(context);
    case HttpStatus::ServiceUnavailable:
        status = IOStatus::Busy(context);
        status.SetRetryable(true);
        return status;
    default:
        return IOStatus::IOError(context);
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
