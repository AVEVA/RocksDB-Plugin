// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <system_error>
namespace AVEVA::RocksDB::Plugin::Azure {
namespace {
rocksdb::IOStatus Retryable(rocksdb::IOStatus status) {
    status.SetRetryable(true);
    return status;
}

bool IsTransportTimeout(const std::error_code& code) {
    return code == std::errc::timed_out || code == HttpClientError::TimedOut;
}

// These fail identically on every attempt, so retrying (or reporting them as retryable) only delays the error.
bool IsPermanentHttpClientError(const std::error_code& code) {
    return code == HttpClientError::InvalidUrl || code == HttpClientError::InvalidRequest ||
           code == HttpClientError::TlsFailed || code == HttpClientError::ResponseTooLarge ||
           code == HttpClientError::ProtocolError;
}

// The Azure error code, message and request id are always part of the text so that logs identify the failure.
std::string DescribeFailure(const RequestFailedException& error) {
    return error.StatusCode != 0 ? "HTTP " + error.Describe() : error.Describe();
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
        // Matches IsTransient: every 5xx is retried internally, so it is reported as retryable too.
        if (statusCode >= 500 && statusCode <= 599) {
            return Retryable(IOStatus::IOError(context));
        }
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
        // Raised by AbortIO or shutdown, so retrying would only fight the cancellation.
        if (code == std::errc::operation_canceled) {
            return IOStatus::Aborted(text);
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
    case HttpStatus::Unauthorized:
        return IOStatus::IOError("Authentication failed: " + text);
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
        return code != std::errc::operation_canceled && code != std::errc::state_not_recoverable &&
               code != std::errc::invalid_argument && code != std::errc::bad_message &&
               code != std::errc::value_too_large && code != std::errc::permission_denied &&
               !IsPermanentHttpClientError(code);
    case HttpStatus::RequestTimeout:
    case HttpStatus::TooManyRequests:
        return true;
    default:
        return statusCode >= 500 && statusCode <= 599;
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
