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
    if (code == AzureClient::BlobStorageErrorCode::LeaseLost ||
        code == AzureClient::BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation ||
        code == AzureClient::BlobStorageErrorCode::LeaseNotPresentWithBlobOperation ||
        code == AzureClient::BlobStorageErrorCode::LeaseIdMissing ||
        code == AzureClient::BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation ||
        code == AzureClient::BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed ||
        code == AzureClient::BlobStorageErrorCode::LeaseAlreadyBroken ||
        code == AzureClient::BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired) {
        return IOStatus::IOFenced("Lease lost: " + text);
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

bool AzureErrorTranslator::IsTransient(const RequestFailedException& error) {
    return IsTransient(error.StatusCode, error.Code);
}

rocksdb::IOStatus AzureErrorTranslator::FromException(const std::exception_ptr& error) {
    try {
        std::rethrow_exception(error);
    } catch (const RequestFailedException& ex) {
        return IOStatusFromError(ex);
    } catch (const WritesFencedException& ex) {
        return rocksdb::IOStatus::IOFenced(ex.what());
    } catch (const std::invalid_argument& ex) {
        return rocksdb::IOStatus::InvalidArgument(ex.what());
    } catch (const std::system_error& ex) {
        if (ex.code() == std::errc::operation_canceled) {
            return rocksdb::IOStatus::Aborted(ex.what());
        }
        return rocksdb::IOStatus::IOError(ex.what());
    } catch (const std::exception& ex) {
        return rocksdb::IOStatus::IOError(ex.what());
    } catch (...) {
        return rocksdb::IOStatus::IOError("Unknown error");
    }
}

rocksdb::IOStatus AzureErrorTranslator::FromCurrentException(
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger, std::string_view operation) {
    using boost::log::trivial::error;
    try {
        throw;
    } catch (const RequestFailedException& ex) {
        // Describe() carries the status, Azure error code, message and request id.
        BOOST_LOG_SEV(logger, error) << operation << " failed: " << ex.Describe();
        // AuthenticationDetail may echo the string-to-sign, so it is deliberately never logged.
        return IOStatusFromError(ex);
    } catch (const WritesFencedException& ex) {
        BOOST_LOG_SEV(logger, error) << operation << " failed: " << ex.what();
        return rocksdb::IOStatus::IOFenced(ex.what());
    } catch (const std::invalid_argument& ex) {
        BOOST_LOG_SEV(logger, error) << operation << " rejected: " << ex.what();
        return rocksdb::IOStatus::InvalidArgument(ex.what());
    } catch (const std::system_error& ex) {
        BOOST_LOG_SEV(logger, error) << operation << " failed: " << ex.what();
        // Cancellation comes from AbortIO or shutdown, so it is not a storage failure.
        if (ex.code() == std::errc::operation_canceled) {
            return rocksdb::IOStatus::Aborted(ex.what());
        }
        return rocksdb::IOStatus::IOError(ex.what());
    } catch (const std::exception& ex) {
        BOOST_LOG_SEV(logger, error) << operation << " failed: " << ex.what();
        return rocksdb::IOStatus::IOError(ex.what());
    } catch (...) {
        BOOST_LOG_SEV(logger, error) << operation << " failed with an unknown error";
        return rocksdb::IOStatus::IOError("Unknown error when calling " + std::string(operation));
    }
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
