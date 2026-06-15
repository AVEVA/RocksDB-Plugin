// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
namespace AVEVA::RocksDB::Plugin::Azure
{
    rocksdb::IOStatus AzureErrorTranslator::IOStatusFromError(const std::string& context, const ::Azure::Core::Http::HttpStatusCode& statusCode)
    {
        using ::Azure::Core::Http::HttpStatusCode;
        using rocksdb::IOStatus;

        rocksdb::IOStatus status;
        switch (statusCode)
        {
        case HttpStatusCode::BadRequest:
            return IOStatus::InvalidArgument(context);
        case HttpStatusCode::NotFound:
            return IOStatus::NotFound(context);
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
}
