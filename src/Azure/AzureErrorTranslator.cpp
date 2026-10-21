// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
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

// The Azure error code, message and request id are always part of the text so that logs identify the failure.
std::string DescribeFailure(const RequestFailedException& error) {
    return error.StatusCode != 0 ? "HTTP " + error.Describe() : error.Describe();
}

// The single exception-to-status ladder, so a mapping change is made once. `log(kind, detail)` is a lambda (not an
// erased callable) that the logging overload uses to report and the plain overload leaves empty. An empty
// `operation` selects the plain overload's wording for unknown exceptions.
template <class TLog> rocksdb::IOStatus ClassifyCurrentException(TLog&& log, std::string_view operation) {
    try {
        throw;
    } catch (const RequestFailedException& ex) {
        // Describe() carries the status, Azure error code, message and request id. AuthenticationDetail may echo the
        // string-to-sign, so it is deliberately never logged.
        log("failed", ex.Describe());
        return AzureErrorTranslator::IOStatusFromError(ex);
    } catch (const WritesFencedException& ex) {
        log("failed", ex.what());
        return rocksdb::IOStatus::IOFenced(ex.what());
    } catch (const std::invalid_argument& ex) {
        log("rejected", ex.what());
        return rocksdb::IOStatus::InvalidArgument(ex.what());
    } catch (const std::system_error& ex) {
        log("failed", ex.what());
        // Cancellation comes from AbortIO or shutdown, so it is not a storage failure.
        if (ex.code() == std::errc::operation_canceled) {
            return rocksdb::IOStatus::Aborted(ex.what());
        }
        return rocksdb::IOStatus::IOError(ex.what());
    } catch (const std::exception& ex) {
        log("failed", ex.what());
        return rocksdb::IOStatus::IOError(ex.what());
    } catch (...) {
        log("failed with an unknown error", std::string_view{});
        return rocksdb::IOStatus::IOError(operation.empty() ? "Unknown error"
                                                            : "Unknown error when calling " + std::string(operation));
    }
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

    // The library's portable conditions are more precise than the HTTP status alone (e.g. a 409 can mean a lost lease).
    if (code == std::errc::no_such_file_or_directory) {
        return IOStatus::NotFound(text);
    }
    if (code == AzureClient::BlobStorageErrorCode::InvalidResponse) {
        return IOStatus::Corruption("Invalid service response: " + text);
    }
    // Another writer may own the blob now, so retrying could corrupt data; IOFenced is non-retryable.
    // Lock-file acquisition inspects the raw client error instead, so contention there stays "locked", not fenced.
    if (IsLeaseLost(code)) {
        return IOStatus::IOFenced("Lease lost: " + text);
    }
    if (code == AzureClient::BlobStorageErrorCode::InvalidPageRange) {
        return IOStatus::InvalidArgument("Invalid page range: " + text);
    }
    // The blob changed underneath us. Nothing on disk is corrupt, so this is a non-retryable IOError rather than
    // Corruption; the prefix lets callers and logs tell it from a generic failure.
    if (code == AzureClient::BlobStorageErrorCode::SequenceNumberConditionNotMet ||
        code == AzureClient::BlobStorageErrorCode::ConditionNotMet ||
        code == AzureClient::BlobStorageErrorCode::TargetConditionNotMet) {
        return IOStatus::IOError("Blob changed concurrently: " + text);
    }
    if (code == AzureClient::BlobStorageErrorCode::AuthorizationPermissionMismatch) {
        return IOStatus::IOError("Authorization failed: " + text);
    }

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

bool AzureErrorTranslator::IsLeaseLost(const std::error_code& code) {
    using AzureClient::BlobStorageErrorCode;
    for (const auto lost :
         {BlobStorageErrorCode::LeaseLost, BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation,
          BlobStorageErrorCode::LeaseNotPresentWithBlobOperation, BlobStorageErrorCode::LeaseIdMissing,
          BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation,
          BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed, BlobStorageErrorCode::LeaseAlreadyBroken,
          BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired, BlobStorageErrorCode::LeaseAlreadyPresent}) {
        if (code == AzureClient::make_error_code(lost)) {
            return true;
        }
    }
    return false;
}

bool AzureErrorTranslator::IsTransient(const RequestFailedException& error) {
    return IsTransient(error.StatusCode, error.Code);
}

rocksdb::IOStatus AzureErrorTranslator::FromException(const std::exception_ptr& error) {
    try {
        std::rethrow_exception(error);
    } catch (...) {
        return ClassifyCurrentException([](std::string_view, std::string_view) {}, {});
    }
}

rocksdb::IOStatus AzureErrorTranslator::FromCurrentException(
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger, std::string_view operation) {
    return ClassifyCurrentException(
        [&](std::string_view kind, std::string_view detail) {
            if (detail.empty()) {
                BOOST_LOG_SEV(logger, boost::log::trivial::error) << operation << ' ' << kind;
            } else {
                BOOST_LOG_SEV(logger, boost::log::trivial::error) << operation << ' ' << kind << ": " << detail;
            }
        },
        operation);
}

rocksdb::IOStatus AzureErrorTranslator::FromException(
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger,
    const std::exception_ptr& error, std::string_view operation) {
    try {
        std::rethrow_exception(error);
    } catch (...) {
        return FromCurrentException(logger, operation);
    }
}

bool AzureErrorTranslator::IsTransient(unsigned int statusCode, const std::error_code& code) {
    // The client's own retry policy is the single definition of "transient", so the plugin never disagrees with it.
    AzureClient::BlobStorageError error;
    error.StatusCode = statusCode;
    error.Code = code;
    return AzureClient::IsTransient(error);
}
} // namespace AVEVA::RocksDB::Plugin::Azure
