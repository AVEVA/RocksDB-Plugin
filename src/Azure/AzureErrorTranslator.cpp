// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include <system_error>
namespace AVEVA::RocksDB::Plugin::Azure {
namespace {
rocksdb::IOStatus Retryable(rocksdb::IOStatus status) {
    status.SetRetryable(true);
    return status;
}

bool IsTransportTimeout(const std::error_code& code) {
    return code == std::errc::timed_out || code == std::errc::operation_canceled;
}

// The Azure error code and message are always part of the text so that logs identify the failure.
std::string DescribeFailure(const RequestFailedException& error) {
    std::string text = error.Message.empty() ? error.Code.message() : error.Message;
    if (!error.ErrorCode.empty()) {
        text = error.ErrorCode + ": " + text;
    }

    if (error.StatusCode != 0) {
        text = "HTTP " + std::to_string(error.StatusCode) + " " + text;
    }

    return text;
}
} // namespace

rocksdb::IOStatus AzureErrorTranslator::IOStatusFromError(const std::string& context, unsigned int statusCode) {
    using rocksdb::IOStatus;

    switch (statusCode) {
    case HttpStatus::BadRequest:
        return IOStatus::InvalidArgument(context);
    case HttpStatus::NotFound:
        return IOStatus::NotFound(context);
    case HttpStatus::RequestTimeout:
        return Retryable(IOStatus::TimedOut(context));
    case HttpStatus::TooManyRequests:
    case HttpStatus::ServiceUnavailable:
        return Retryable(IOStatus::Busy(context));
    case HttpStatus::InternalServerError:
    case HttpStatus::BadGateway:
    case HttpStatus::GatewayTimeout:
        return Retryable(IOStatus::IOError(context));
    default:
        return IOStatus::IOError(context);
    }
}

rocksdb::IOStatus AzureErrorTranslator::IOStatusFromError(const RequestFailedException& error) {
    using rocksdb::IOStatus;

    const std::string text = DescribeFailure(error);
    const std::error_code& code = error.Code;
    switch (error.StatusCode) {
    case 0:
        if (IsTransportTimeout(code)) {
            return Retryable(IOStatus::TimedOut(text));
        }
        if (code == std::errc::invalid_argument) {
            return IOStatus::InvalidArgument("Invalid request or configuration: " + text);
        }
        if (code == std::errc::permission_denied) {
            return IOStatus::IOError("Authentication failed: " + text);
        }
        if (!IsTransient(error.StatusCode, code)) {
            return IOStatus::IOError("Request failed: " + text);
        }
        return Retryable(IOStatus::IOError("Connection failure: " + text));
    case HttpStatus::Forbidden:
        return IOStatus::IOError("Authorization failed: " + text);
    case HttpStatus::Conflict:
    case HttpStatus::PreconditionFailed:
        return IOStatus::IOError(text);
    default:
        return IOStatusFromError(text, error.StatusCode);
    }
}

bool AzureErrorTranslator::IsTransient(const RequestFailedException& error) {
    return IsTransient(error.StatusCode, error.Code);
}

bool AzureErrorTranslator::IsTransient(unsigned int statusCode, const std::error_code& code) {
    switch (statusCode) {
    case 0:
        // Status 0 also covers requests rejected client-side (bad arguments, unparsable responses) and
        // credential failures (a rejected secret maps to permission_denied), which would fail the same way again.
        return code != std::errc::invalid_argument && code != std::errc::bad_message &&
               code != std::errc::value_too_large && code != std::errc::permission_denied;
    case HttpStatus::RequestTimeout:
    case HttpStatus::TooManyRequests:
        return true;
    default:
        return statusCode >= 500 && statusCode <= 599;
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
