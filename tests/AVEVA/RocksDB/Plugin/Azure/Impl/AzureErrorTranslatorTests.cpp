// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <boost/log/core.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <sstream>
#include <system_error>

using namespace AVEVA::RocksDB::Plugin::Azure;

namespace {
RequestFailedException Failure(unsigned int status, std::string errorCode = "SomeCode", std::error_code code = {}) {
    return RequestFailedException(status, std::move(errorCode), "boom", "req-1", code);
}
} // namespace

TEST(AzureErrorTranslatorTests, BadRequestIsInvalidArgument) {
    EXPECT_TRUE(AzureErrorTranslator::IOStatusFromError(Failure(400)).IsInvalidArgument());
}

TEST(AzureErrorTranslatorTests, NotFoundIsNotFound) {
    EXPECT_TRUE(AzureErrorTranslator::IOStatusFromError(Failure(404)).IsNotFound());
}

TEST(AzureErrorTranslatorTests, RequestTimeoutIsRetryableTimedOut) {
    auto status = AzureErrorTranslator::IOStatusFromError(Failure(408));
    EXPECT_TRUE(status.IsTimedOut());
    EXPECT_TRUE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, ForbiddenIsNonRetryableAuthorizationFailure) {
    auto status = AzureErrorTranslator::IOStatusFromError(Failure(403, "AuthorizationFailure"));
    EXPECT_TRUE(status.IsIOError());
    EXPECT_FALSE(status.GetRetryable());
    EXPECT_NE(status.ToString().find("Authorization failed"), std::string::npos);
    EXPECT_NE(status.ToString().find("AuthorizationFailure"), std::string::npos);
}

TEST(AzureErrorTranslatorTests, UnauthorizedIsNonRetryableAuthenticationFailure) {
    auto status = AzureErrorTranslator::IOStatusFromError(Failure(401, "InvalidAuthenticationInfo"));
    EXPECT_TRUE(status.IsIOError());
    EXPECT_FALSE(status.GetRetryable());
    EXPECT_NE(status.ToString().find("Authentication failed"), std::string::npos);
}

TEST(AzureErrorTranslatorTests, ConflictAndPreconditionFailedPreserveErrorCode) {
    for (unsigned int code : {409u, 412u}) {
        auto status = AzureErrorTranslator::IOStatusFromError(Failure(code, "LeaseAlreadyPresent"));
        EXPECT_TRUE(status.IsIOError());
        EXPECT_FALSE(status.GetRetryable());
        EXPECT_NE(status.ToString().find("LeaseAlreadyPresent"), std::string::npos);
        EXPECT_NE(status.ToString().find("boom"), std::string::npos);
    }
}

TEST(AzureErrorTranslatorTests, ThrottlingAndUnavailableAreRetryableBusy) {
    for (unsigned int code : {429u, 503u}) {
        auto status = AzureErrorTranslator::IOStatusFromError(Failure(code, "ServerBusy"));
        EXPECT_TRUE(status.IsBusy());
        EXPECT_TRUE(status.GetRetryable());
    }
}

TEST(AzureErrorTranslatorTests, ServerErrorsAreRetryableIOError) {
    for (unsigned int code : {500u, 502u, 504u}) {
        auto status = AzureErrorTranslator::IOStatusFromError(Failure(code));
        EXPECT_TRUE(status.IsIOError());
        EXPECT_TRUE(status.GetRetryable());
    }
}

TEST(AzureErrorTranslatorTests, UnknownStatusIsNonRetryableIOError) {
    auto status = AzureErrorTranslator::IOStatusFromError(Failure(418));
    EXPECT_TRUE(status.IsIOError());
    EXPECT_FALSE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, TransportTimeoutIsRetryableTimedOut) {
    auto status = AzureErrorTranslator::IOStatusFromError(Failure(0, "", std::make_error_code(std::errc::timed_out)));
    EXPECT_TRUE(status.IsTimedOut());
    EXPECT_TRUE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, HttpClientTimeoutIsRetryableTimedOut) {
    const std::error_code code = AVEVA::HttpClientError::TimedOut;
    auto status = AzureErrorTranslator::IOStatusFromError(Failure(0, "", code));
    EXPECT_TRUE(status.IsTimedOut());
    EXPECT_TRUE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, PermanentHttpClientErrorsAreNonRetryable) {
    for (auto error :
         {AVEVA::HttpClientError::InvalidUrl, AVEVA::HttpClientError::InvalidRequest, AVEVA::HttpClientError::TlsFailed,
          AVEVA::HttpClientError::ResponseTooLarge, AVEVA::HttpClientError::ProtocolError}) {
        const std::error_code code = error;
        EXPECT_FALSE(AzureErrorTranslator::IsTransient(0, code)) << code.message();
        EXPECT_FALSE(AzureErrorTranslator::IOStatusFromError(Failure(0, "", code)).GetRetryable()) << code.message();
    }
}

TEST(AzureErrorTranslatorTests, TransientHttpClientErrorsAreRetryable) {
    for (auto error : {AVEVA::HttpClientError::ResolveFailed, AVEVA::HttpClientError::ConnectFailed,
                       AVEVA::HttpClientError::WriteFailed, AVEVA::HttpClientError::ReadFailed}) {
        const std::error_code code = error;
        EXPECT_TRUE(AzureErrorTranslator::IsTransient(0, code)) << code.message();
    }
}

TEST(AzureErrorTranslatorTests, TransportCancellationIsNonRetryableAborted) {
    const auto code = std::make_error_code(std::errc::operation_canceled);
    auto status = AzureErrorTranslator::IOStatusFromError(Failure(0, "", code));
    EXPECT_TRUE(status.IsAborted());
    EXPECT_FALSE(status.GetRetryable());
    EXPECT_FALSE(AzureErrorTranslator::IsTransient(0, code));
}

TEST(AzureErrorTranslatorTests, ConnectionFailureIsRetryableIOError) {
    auto status =
        AzureErrorTranslator::IOStatusFromError(Failure(0, "", std::make_error_code(std::errc::connection_reset)));
    EXPECT_TRUE(status.IsIOError());
    EXPECT_TRUE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, InvalidArgumentWithoutStatusIsNonRetryableInvalidArgument) {
    auto status =
        AzureErrorTranslator::IOStatusFromError(Failure(0, "", std::make_error_code(std::errc::invalid_argument)));
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_FALSE(status.GetRetryable());
    EXPECT_EQ(status.ToString().find("Connection failure"), std::string::npos);
}

TEST(AzureErrorTranslatorTests, PermanentClientSideFailuresWithoutStatusAreNonRetryable) {
    for (auto errc : {std::errc::bad_message, std::errc::value_too_large}) {
        auto status = AzureErrorTranslator::IOStatusFromError(Failure(0, "", std::make_error_code(errc)));
        EXPECT_TRUE(status.IsIOError());
        EXPECT_FALSE(status.GetRetryable());
        EXPECT_EQ(status.ToString().find("Connection failure"), std::string::npos);
    }
}

TEST(AzureErrorTranslatorTests, RejectedCredentialsWithoutStatusAreNonRetryable) {
    auto status =
        AzureErrorTranslator::IOStatusFromError(Failure(0, "", std::make_error_code(std::errc::permission_denied)));
    EXPECT_TRUE(status.IsIOError());
    EXPECT_FALSE(status.GetRetryable());
    EXPECT_NE(status.ToString().find("Authentication failed"), std::string::npos);
    EXPECT_FALSE(AzureErrorTranslator::IsTransient(0, std::make_error_code(std::errc::permission_denied)));
}

TEST(AzureErrorTranslatorTests, StatusTextIncludesCodeAndMessage) {
    auto text = AzureErrorTranslator::IOStatusFromError(Failure(500, "InternalError")).ToString();
    EXPECT_NE(text.find("InternalError"), std::string::npos);
    EXPECT_NE(text.find("boom"), std::string::npos);
}

TEST(AzureErrorTranslatorTests, TransientClassification) {
    for (unsigned int code : {408u, 429u, 500u, 502u, 503u, 504u}) {
        EXPECT_TRUE(AzureErrorTranslator::IsTransient(Failure(code))) << code;
    }

    for (unsigned int code : {0u, 400u, 401u, 403u, 404u, 409u, 412u, 501u, 505u, 507u}) {
        EXPECT_FALSE(AzureErrorTranslator::IsTransient(Failure(code))) << code;
    }
}

TEST(AzureErrorTranslatorTests, PortableNotFoundConditionWinsOverTheStatus) {
    const auto code = std::make_error_code(std::errc::no_such_file_or_directory);
    EXPECT_TRUE(AzureErrorTranslator::IOStatusFromError(Failure(409, "SomeCode", code)).IsNotFound());
}

TEST(AzureErrorTranslatorTests, InvalidResponseIsCorruption) {
    const auto code =
        ::AVEVA::AzureClient::make_error_code(::AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
    EXPECT_TRUE(AzureErrorTranslator::IOStatusFromError(Failure(200, "", code)).IsCorruption());
}

TEST(AzureErrorTranslatorTests, LeaseLostIsIOFencedNamingTheLease) {
    const auto code = ::AVEVA::AzureClient::make_error_code(::AVEVA::AzureClient::BlobStorageErrorCode::LeaseLost);
    const auto status = AzureErrorTranslator::IOStatusFromError(Failure(409, "LeaseLost", code));
    EXPECT_TRUE(status.IsIOFenced());
    EXPECT_FALSE(status.GetRetryable());
    EXPECT_NE(status.ToString().find("Lease lost"), std::string::npos);
}

TEST(AzureErrorTranslatorTests, LeaseMismatchAndMissingLeaseAreIOFenced) {
    using ::AVEVA::AzureClient::BlobStorageErrorCode;
    EXPECT_TRUE(
        AzureErrorTranslator::IOStatusFromError(
            Failure(412, "LeaseIdMismatchWithBlobOperation",
                    ::AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation)))
            .IsIOFenced());
    EXPECT_TRUE(
        AzureErrorTranslator::IOStatusFromError(
            Failure(409, "LeaseNotPresentWithBlobOperation",
                    ::AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::LeaseNotPresentWithBlobOperation)))
            .IsIOFenced());
    EXPECT_TRUE(
        AzureErrorTranslator::IOStatusFromError(
            Failure(400, "LeaseIdMissing", ::AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::LeaseIdMissing)))
            .IsIOFenced());
}

TEST(AzureErrorTranslatorTests, NewLeaseCodesAreIOFencedAndPermissionMismatchIsNotRetryable) {
    using ::AVEVA::AzureClient::BlobStorageErrorCode;
    for (const auto lease :
         {BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation,
          BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed, BlobStorageErrorCode::LeaseAlreadyBroken,
          BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired}) {
        const auto status = AzureErrorTranslator::IOStatusFromError(
            Failure(409, "Lease", ::AVEVA::AzureClient::make_error_code(lease)));
        EXPECT_TRUE(status.IsIOFenced()) << static_cast<int>(lease);
        EXPECT_FALSE(status.GetRetryable()) << static_cast<int>(lease);
    }

    const auto denied = AzureErrorTranslator::IOStatusFromError(
        Failure(403, "AuthorizationPermissionMismatch",
                ::AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::AuthorizationPermissionMismatch)));
    EXPECT_TRUE(denied.IsIOError());
    EXPECT_FALSE(denied.GetRetryable());
    EXPECT_NE(denied.ToString().find("Authorization failed"), std::string::npos);
}

TEST(AzureErrorTranslatorTests, InvalidPageRangeIsNonRetryableInvalidArgument) {
    const auto code =
        ::AVEVA::AzureClient::make_error_code(::AVEVA::AzureClient::BlobStorageErrorCode::InvalidPageRange);
    const auto status = AzureErrorTranslator::IOStatusFromError(Failure(416, "InvalidPageRange", code));
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_FALSE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, ConditionNotMetCodesAreNonRetryableConcurrentChangeIOErrors) {
    using ::AVEVA::AzureClient::BlobStorageErrorCode;
    for (const auto condition : {BlobStorageErrorCode::SequenceNumberConditionNotMet,
                                 BlobStorageErrorCode::ConditionNotMet, BlobStorageErrorCode::TargetConditionNotMet}) {
        const auto status = AzureErrorTranslator::IOStatusFromError(
            Failure(412, "Condition", ::AVEVA::AzureClient::make_error_code(condition)));
        EXPECT_TRUE(status.IsIOError()) << static_cast<int>(condition);
        EXPECT_FALSE(status.GetRetryable()) << static_cast<int>(condition);
        EXPECT_NE(status.ToString().find("Blob changed concurrently"), std::string::npos);
    }
}

TEST(AzureErrorTranslatorTests, LeaseAlreadyPresentIsIOFencedAndLeaseLost) {
    const auto code =
        ::AVEVA::AzureClient::make_error_code(::AVEVA::AzureClient::BlobStorageErrorCode::LeaseAlreadyPresent);
    const auto status = AzureErrorTranslator::IOStatusFromError(Failure(409, "LeaseAlreadyPresent", code));
    EXPECT_TRUE(status.IsIOFenced());
    EXPECT_FALSE(status.GetRetryable());
    EXPECT_TRUE(AzureErrorTranslator::IsLeaseLost(code));
    EXPECT_FALSE(AzureErrorTranslator::IsLeaseLost(std::make_error_code(std::errc::timed_out)));
}

TEST(AzureErrorTranslatorTests, IsLeaseLost_MatchesEveryLeaseCodeAndNoOtherCategory) {
    using ::AVEVA::AzureClient::BlobStorageErrorCode;
    for (const auto lost :
         {BlobStorageErrorCode::LeaseLost, BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation,
          BlobStorageErrorCode::LeaseNotPresentWithBlobOperation, BlobStorageErrorCode::LeaseIdMissing,
          BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation,
          BlobStorageErrorCode::LeaseNotPresentWithLeaseOperation,
          BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed, BlobStorageErrorCode::LeaseAlreadyBroken,
          BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired, BlobStorageErrorCode::LeaseAlreadyPresent}) {
        EXPECT_TRUE(AzureErrorTranslator::IsLeaseLost(::AVEVA::AzureClient::make_error_code(lost)));
        // Same numeric value in a different category must not match.
        EXPECT_FALSE(
            AzureErrorTranslator::IsLeaseLost(std::error_code(static_cast<int>(lost), std::generic_category())));
    }
    EXPECT_FALSE(
        AzureErrorTranslator::IsLeaseLost(::AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::BlobNotFound)));
}

TEST(AzureErrorTranslatorTests, WritesFencedExceptionIsIOFenced) {
    const auto status = AzureErrorTranslator::FromException(std::make_exception_ptr(WritesFencedException("fenced")));
    EXPECT_TRUE(status.IsIOFenced());
    EXPECT_FALSE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, FencedExceptionRaisedFromAFilesystemOperationIsIOFenced) {
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level> logger;
    // BlobFilesystem::Guard delegates here from its catch-all, so fencing must not degrade to a generic IOError.
    const auto status = [&] {
        try {
            throw WritesFencedException("Unable to ensure safe database access");
        } catch (...) {
            return AzureErrorTranslator::FromCurrentException(logger, "NewWritableFile");
        }
    }();
    EXPECT_TRUE(status.IsIOFenced());
}

TEST(AzureErrorTranslatorTests, InvalidArgumentExceptionIsInvalidArgument) {
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level> logger;
    const auto status = [&] {
        try {
            throw std::invalid_argument("range is out of bounds");
        } catch (...) {
            return AzureErrorTranslator::FromCurrentException(logger, "Read");
        }
    }();
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_TRUE(
        AzureErrorTranslator::FromException(std::make_exception_ptr(std::invalid_argument("x"))).IsInvalidArgument());
}

TEST(AzureErrorTranslatorTests, SystemErrorCancellationIsAborted) {
    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level> logger;
    const auto cancelled =
        std::make_exception_ptr(std::system_error(std::make_error_code(std::errc::operation_canceled)));
    EXPECT_TRUE(AzureErrorTranslator::FromException(logger, cancelled, "Read").IsAborted());
    EXPECT_TRUE(AzureErrorTranslator::FromException(cancelled).IsAborted());
    const auto other = std::make_exception_ptr(std::system_error(std::make_error_code(std::errc::io_error)));
    EXPECT_TRUE(AzureErrorTranslator::FromException(logger, other, "Read").IsIOError());
}

namespace {
enum class Expected : std::uint8_t {
    NotFound,
    Corruption,
    Fenced,
    InvalidArgument,
    IOError,
    RetryableBusy,
    RetryableTimedOut,
    RetryableIOError
};

struct PolicyEntry {
    ::AVEVA::AzureClient::BlobStorageErrorCode Code;
    unsigned int Status;
    Expected Result;
};

void ExpectPolicy(const rocksdb::IOStatus& status, Expected expected, const std::string& label) {
    switch (expected) {
    case Expected::NotFound:
        EXPECT_TRUE(status.IsNotFound()) << label;
        break;
    case Expected::Corruption:
        EXPECT_TRUE(status.IsCorruption()) << label;
        break;
    case Expected::Fenced:
        EXPECT_TRUE(status.IsIOFenced()) << label;
        break;
    case Expected::InvalidArgument:
        EXPECT_TRUE(status.IsInvalidArgument()) << label;
        break;
    case Expected::IOError:
    case Expected::RetryableIOError:
        EXPECT_TRUE(status.IsIOError()) << label;
        break;
    case Expected::RetryableBusy:
        EXPECT_TRUE(status.IsBusy()) << label;
        break;
    case Expected::RetryableTimedOut:
        EXPECT_TRUE(status.IsTimedOut()) << label;
        break;
    }
    const bool retryable = expected == Expected::RetryableBusy || expected == Expected::RetryableTimedOut ||
                           expected == Expected::RetryableIOError;
    EXPECT_EQ(status.GetRetryable(), retryable) << label;
}
} // namespace

// One row per client enumerator; the size assertion forces a new enumerator to be given a deliberate mapping.
TEST(AzureErrorTranslatorTests, EveryBlobStorageErrorCodeHasADeliberateMapping) {
    using ::AVEVA::AzureClient::BlobStorageErrorCode;
    static constexpr std::array table{
        PolicyEntry{BlobStorageErrorCode::ContainerAlreadyExists, 409, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::ContainerNotFound, 404, Expected::NotFound},
        PolicyEntry{BlobStorageErrorCode::ContainerBeingDeleted, 409, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::ContainerDisabled, 403, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::InvalidResourceName, 400, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::ResourceNotFound, 404, Expected::NotFound},
        PolicyEntry{BlobStorageErrorCode::AuthenticationFailed, 403, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::AuthorizationFailure, 403, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::InvalidAuthenticationInfo, 401, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::ConditionNotMet, 412, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::BlobNotFound, 404, Expected::NotFound},
        PolicyEntry{BlobStorageErrorCode::BlobAlreadyExists, 409, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::InvalidBlobOrBlock, 400, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::InvalidBlockList, 400, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::ServiceError, 418, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::LeaseIdMissing, 412, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseAlreadyPresent, 409, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation, 412, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseNotPresentWithBlobOperation, 409, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseLost, 412, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::ServerBusy, 503, Expected::RetryableBusy},
        PolicyEntry{BlobStorageErrorCode::OperationTimedOut, 500, Expected::RetryableTimedOut},
        PolicyEntry{BlobStorageErrorCode::InternalError, 500, Expected::RetryableIOError},
        PolicyEntry{BlobStorageErrorCode::InvalidRange, 416, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::TargetConditionNotMet, 412, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::AppendPositionConditionNotMet, 412, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::MaxBlobSizeConditionNotMet, 412, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::BlobArchived, 409, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::Md5Mismatch, 400, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::InvalidHeaderValue, 400, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::InvalidQueryParameterValue, 400, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::RequestBodyTooLarge, 413, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::BlobTierInadequateForContentLength, 409, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::InvalidMetadata, 0, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::InvalidResponse, 200, Expected::Corruption},
        PolicyEntry{BlobStorageErrorCode::LeaseNotPresentWithLeaseOperation, 409, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation, 409, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed, 409, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseAlreadyBroken, 409, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired, 409, Expected::Fenced},
        PolicyEntry{BlobStorageErrorCode::SequenceNumberConditionNotMet, 412, Expected::IOError},
        PolicyEntry{BlobStorageErrorCode::InvalidPageRange, 416, Expected::InvalidArgument},
        PolicyEntry{BlobStorageErrorCode::AuthorizationPermissionMismatch, 403, Expected::IOError}};
    static_assert(table.size() == static_cast<std::size_t>(BlobStorageErrorCode::AuthorizationPermissionMismatch));

    for (std::size_t i = 0; i < table.size(); ++i) {
        const auto& entry = table[i];
        EXPECT_EQ(static_cast<std::size_t>(entry.Code), i + 1);
        const auto code = ::AVEVA::AzureClient::make_error_code(entry.Code);
        const auto status = AzureErrorTranslator::IOStatusFromError(Failure(entry.Status, "Azure", code));
        ExpectPolicy(status, entry.Result, code.message());
        EXPECT_NE(status.ToString().find("req-1"), std::string::npos) << code.message();
        EXPECT_NE(status.ToString().find("boom"), std::string::npos) << code.message();
        EXPECT_EQ(AzureErrorTranslator::IsLeaseLost(code), entry.Result == Expected::Fenced) << code.message();
    }
}

TEST(AzureErrorTranslatorTests, EveryHttpClientErrorHasADeliberateMapping) {
    using ::AVEVA::HttpClientError;
    struct Row {
        HttpClientError Error;
        Expected Result;
    };
    static constexpr std::array table{Row{HttpClientError::InvalidUrl, Expected::InvalidArgument},
                                      Row{HttpClientError::InvalidRequest, Expected::InvalidArgument},
                                      Row{HttpClientError::ResolveFailed, Expected::RetryableIOError},
                                      Row{HttpClientError::ConnectFailed, Expected::RetryableIOError},
                                      Row{HttpClientError::TlsFailed, Expected::IOError},
                                      Row{HttpClientError::WriteFailed, Expected::RetryableIOError},
                                      Row{HttpClientError::ReadFailed, Expected::RetryableIOError},
                                      Row{HttpClientError::TimedOut, Expected::RetryableTimedOut},
                                      Row{HttpClientError::ResponseTooLarge, Expected::IOError},
                                      Row{HttpClientError::ProtocolError, Expected::IOError}};
    static_assert(table.size() == static_cast<std::size_t>(HttpClientError::ProtocolError));

    for (const auto& row : table) {
        const std::error_code code = row.Error;
        ExpectPolicy(AzureErrorTranslator::IOStatusFromError(Failure(0, "", code)), row.Result, code.message());
    }
}

TEST(AzureErrorTranslatorTests, RawSystemErrorsMatchTheWrappedClassification) {
    using ::AVEVA::HttpClientError;
    for (const std::error_code code :
         {std::error_code{std::make_error_code(std::errc::timed_out)}, std::error_code{HttpClientError::TimedOut}}) {
        const auto wrapped = AzureErrorTranslator::IOStatusFromError(Failure(0, "", code));
        const auto raw = AzureErrorTranslator::FromException(std::make_exception_ptr(std::system_error(code, "ctx")));
        EXPECT_TRUE(raw.IsTimedOut()) << code.message();
        EXPECT_TRUE(raw.GetRetryable()) << code.message();
        EXPECT_EQ(raw.IsTimedOut(), wrapped.IsTimedOut());
        EXPECT_EQ(raw.GetRetryable(), wrapped.GetRetryable());
        EXPECT_NE(raw.ToString().find("ctx"), std::string::npos);
    }

    for (const std::error_code code :
         {std::error_code{HttpClientError::InvalidUrl}, std::error_code{HttpClientError::InvalidRequest},
          std::error_code{std::make_error_code(std::errc::invalid_argument)}}) {
        const auto raw = AzureErrorTranslator::FromException(std::make_exception_ptr(std::system_error(code, "ctx")));
        EXPECT_TRUE(raw.IsInvalidArgument()) << code.message();
        EXPECT_FALSE(raw.GetRetryable()) << code.message();
    }

    const auto cancelled = AzureErrorTranslator::FromException(
        std::make_exception_ptr(std::system_error(std::make_error_code(std::errc::operation_canceled), "ctx")));
    EXPECT_TRUE(cancelled.IsAborted());
    EXPECT_FALSE(cancelled.GetRetryable());
}

TEST(AzureErrorTranslatorTests, CapturedExceptionIsLoggedWithTheRequestIdAndTranslated) {
    namespace sinks = boost::log::sinks;
    auto stream = std::make_shared<std::ostringstream>();
    auto backend = boost::make_shared<sinks::text_ostream_backend>();
    backend->add_stream(boost::shared_ptr<std::ostream>(stream.get(), [](std::ostream*) {}));
    backend->auto_flush(true);
    auto sink = boost::make_shared<sinks::synchronous_sink<sinks::text_ostream_backend>>(backend);
    boost::log::core::get()->add_sink(sink);

    boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level> logger;
    const auto status = AzureErrorTranslator::FromException(logger, std::make_exception_ptr(Failure(404)), "ReadAsync");
    boost::log::core::get()->remove_sink(sink);

    EXPECT_TRUE(status.IsNotFound());
    EXPECT_NE(stream->str().find("ReadAsync failed"), std::string::npos) << stream->str();
    EXPECT_NE(stream->str().find("req-1"), std::string::npos) << stream->str();
}