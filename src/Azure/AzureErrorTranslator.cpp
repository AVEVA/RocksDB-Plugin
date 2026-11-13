// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ErrorTransience.hpp"
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <algorithm>
#include <array>
#include <system_error>
namespace AVEVA::RocksDB::Plugin::Azure {
namespace {
rocksdb::IOStatus Retryable(rocksdb::IOStatus status) {
    status.SetRetryable(true);
    return status;
}

enum class TransportFault : std::uint8_t { Other, Timeout, InvalidInput };

TransportFault ClassifyTransportFault(const std::error_code& code) {
    if (code == std::errc::timed_out || code == HttpClientError::TimedOut) {
        return TransportFault::Timeout;
    }
    // The HTTP client's category has no portable invalid_argument condition, so its input-rejection codes are named
    // here.
    if (code == std::errc::invalid_argument || code == HttpClientError::InvalidUrl ||
        code == HttpClientError::InvalidRequest) {
        return TransportFault::InvalidInput;
    }
    return TransportFault::Other;
}

// The single exception-to-status ladder, so a mapping change is made once. `log(kind, detail)` is a lambda (not an
// erased callable) that the logging overload uses to report and the plain overload leaves empty. An empty
// `operation` selects the plain overload's wording for unknown exceptions.
template <class TLog> rocksdb::IOStatus ClassifyCurrentException(TLog&& log, std::string_view operation) {
    try {
        throw;
    } catch (const RequestFailedException& ex) {
        // what() carries the status, Azure error code, message and request id. AuthenticationDetail may echo the
        // string-to-sign, so it is deliberately never logged.
        log("failed", ex.what());
        return AzureErrorTranslator::IOStatusFromError(ex);
    } catch (const BlockingOnIoThreadException& ex) {
        // The caller broke the threading contract; nothing was started, so this is not a storage failure.
        log("rejected", ex.what());
        return rocksdb::IOStatus::InvalidArgument(ex.what());
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
        // Same conditions as the RequestFailedException path, so wrapped and raw errors classify identically.
        const auto fault = ClassifyTransportFault(ex.code());
        if (fault == TransportFault::Timeout) {
            return Retryable(rocksdb::IOStatus::TimedOut(ex.what()));
        }
        if (fault == TransportFault::InvalidInput) {
            return rocksdb::IOStatus::InvalidArgument(ex.what());
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

rocksdb::IOStatus AzureErrorTranslator::IOStatusFromError(const std::string& context, const HttpStatusCode statusCode) {
    using rocksdb::IOStatus;

    switch (statusCode.Value()) {
    case HttpStatus::BadRequest.Value():
        return IOStatus::InvalidArgument(context);
    case HttpStatus::NotFound.Value():
        return IOStatus::NotFound(context);
    case HttpStatus::RequestTimeout.Value():
        return Retryable(IOStatus::TimedOut(context));
    case HttpStatus::TooManyRequests.Value():
    case HttpStatus::ServiceUnavailable.Value():
        return Retryable(IOStatus::Busy(context));
    case HttpStatus::InternalServerError.Value():
    case HttpStatus::BadGateway.Value():
    case HttpStatus::GatewayTimeout.Value():
        return Retryable(IOStatus::IOError(context));
    default:
        return IOStatus::IOError(context);
    }
}

rocksdb::IOStatus AzureErrorTranslator::IOStatusFromError(const RequestFailedException& error) {
    using rocksdb::IOStatus;

    // what() carries the status, Azure error code, message and request id so that logs identify the failure.
    const std::string text = error.what();
    const std::error_code code = error.Code();

    // The library's portable conditions are more precise than the HTTP status alone (e.g. a 409 can mean a lost lease).
    if (code == std::errc::no_such_file_or_directory) {
        return IOStatus::NotFound(text);
    }
    if (code == AzureClient::BlobStorageErrorCode::InvalidResponse) {
        return IOStatus::Corruption("Invalid service response: " + text);
    }
    // Another writer may own the blob now, so retrying could corrupt data; IOFenced is non-retryable.
    // Lock-file acquisition inspects the raw client error instead, so contention there stays "locked", not fenced.
    if (LeaseLossOf(code) == LeaseLoss::Lost) {
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

    // A service-side OperationTimedOut arrives as HTTP 500 but is still a timeout, so it is classified before the
    // status.
    const auto fault = ClassifyTransportFault(code);
    if (fault == TransportFault::Timeout) {
        return Retryable(IOStatus::TimedOut(text));
    }

    switch (error.StatusCode().Value()) {
    case HttpStatus::NoResponse.Value():
        // Raised by AbortIO or shutdown, so retrying would only fight the cancellation.
        if (code == std::errc::operation_canceled) {
            return IOStatus::Aborted(text);
        }
        if (fault == TransportFault::InvalidInput) {
            return IOStatus::InvalidArgument("Invalid request or configuration: " + text);
        }
        if (code == std::errc::permission_denied) {
            return IOStatus::IOError("Authentication failed: " + text);
        }
        if (Impl::TransienceOf(error) == AzureClient::Transience::Permanent) {
            return IOStatus::IOError("Request failed: " + text);
        }
        return Retryable(IOStatus::IOError("Connection failure: " + text));
    case HttpStatus::Unauthorized.Value():
        return IOStatus::IOError("Authentication failed: " + text);
    case HttpStatus::Forbidden.Value():
        return IOStatus::IOError("Authorization failed: " + text);
    case HttpStatus::Conflict.Value():
    case HttpStatus::PreconditionFailed.Value():
        return IOStatus::IOError(text);
    default:
        return IOStatusFromError(text, error.StatusCode());
    }
}

LeaseLoss AzureErrorTranslator::LeaseLossOf(const std::error_code& code) {
    using AzureClient::BlobStorageErrorCode;
    static constexpr std::array leaseErrors{BlobStorageErrorCode::LeaseLost,
                                            BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation,
                                            BlobStorageErrorCode::LeaseNotPresentWithBlobOperation,
                                            BlobStorageErrorCode::LeaseIdMissing,
                                            BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation,
                                            BlobStorageErrorCode::LeaseNotPresentWithLeaseOperation,
                                            BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed,
                                            BlobStorageErrorCode::LeaseAlreadyBroken,
                                            BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired,
                                            BlobStorageErrorCode::LeaseAlreadyPresent};
    // Compare full error codes (value and category), not just the numeric value.
    const auto lost = std::ranges::any_of(
        leaseErrors, [&](BlobStorageErrorCode lease) { return code == AzureClient::make_error_code(lease); });
    return lost ? LeaseLoss::Lost : LeaseLoss::NotLost;
}

rocksdb::IOStatus AzureErrorTranslator::FromException(
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger,
    const std::exception_ptr& error, std::string_view operation,
    boost::log::trivial::severity_level severity) noexcept {
    if (!error) {
        return rocksdb::IOStatus::IOError();
    }
    // Logging and message building can throw (allocation, a failing log sink). The status is what matters, so a
    // second, silent classification and finally a fixed status keep this from ever escaping.
    try {
        try {
            std::rethrow_exception(error);
        } catch (...) {
            return ClassifyCurrentException(
                [&](std::string_view kind, std::string_view detail) {
                    if (detail.empty()) {
                        BOOST_LOG_SEV(logger, severity) << operation << ' ' << kind;
                    } else {
                        BOOST_LOG_SEV(logger, severity) << operation << ' ' << kind << ": " << detail;
                    }
                },
                operation);
        }
    } catch (...) {
    }
    try {
        try {
            std::rethrow_exception(error);
        } catch (...) {
            return ClassifyCurrentException([](std::string_view, std::string_view) {}, operation);
        }
    } catch (...) {
        return rocksdb::IOStatus::IOError();
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
AzureClient::Transience TransienceOf(const RequestFailedException& error) {
    // The client's own retry policy is the single definition of "transient", so the plugin never disagrees with it.
    AzureClient::BlobStorageError clientError;
    clientError.StatusCode = AVEVA::HttpStatusCode{error.StatusCode().Value()};
    clientError.Code = error.Code();
    return AzureClient::TransienceOf(clientError);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl