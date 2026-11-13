// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
namespace AVEVA::RocksDB::Plugin::Azure {
rocksdb::IOStatus AzureErrorTranslator::PathNotFound(const std::string& context) {
    // IOStatus::PathNotFound() has code kIOError, so IsNotFound() would be false. Keep the kNotFound code and add the
    // kPathNotFound subcode so both IsNotFound() and IsPathNotFound() hold.
    return rocksdb::status_to_io_status(rocksdb::Status::NotFound(rocksdb::Status::SubCode::kPathNotFound, context));
}

rocksdb::IOStatus AzureErrorTranslator::IOStatusFromError(const std::string& context,
                                                          const ::Azure::Core::Http::HttpStatusCode& statusCode) {
    using ::Azure::Core::Http::HttpStatusCode;
    using rocksdb::IOStatus;

    rocksdb::IOStatus status;
    switch (statusCode) {
    case HttpStatusCode::BadRequest:
        return IOStatus::InvalidArgument(context);
    case HttpStatusCode::NotFound:
        return PathNotFound(context);
    case HttpStatusCode::RequestTimeout:
        status.SetRetryable(true);
        return IOStatus::TimedOut(context);
    case HttpStatusCode::ServiceUnavailable:
        status = IOStatus::Busy(context);
        status.SetRetryable(true);
        return status;
    default:
        return IOStatus::IOError(context);
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
