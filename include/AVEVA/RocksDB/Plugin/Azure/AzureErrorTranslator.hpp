// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <rocksdb/io_status.h>
namespace AVEVA::RocksDB::Plugin::Azure {
struct AzureErrorTranslator {
    static rocksdb::IOStatus IOStatusFromError(const std::string& context, unsigned int statusCode);

    /// <summary>
    /// Translates a failed request, using the HTTP status, the Azure error code and the transport error
    /// (status code 0) to pick the status and its retryable flag. The error code is always in the message.
    /// </summary>
    static rocksdb::IOStatus IOStatusFromError(const RequestFailedException& error);

    /// <summary>
    /// True when retrying the same request may succeed: 408, 429, 5xx, and transport failures.
    /// </summary>
    static bool IsTransient(const RequestFailedException& error);

    /// <summary>
    /// Same classification as above, for callers that only have the status and transport error.
    /// </summary>
    static bool IsTransient(unsigned int statusCode, const std::error_code& code = {});
};
} // namespace AVEVA::RocksDB::Plugin::Azure
