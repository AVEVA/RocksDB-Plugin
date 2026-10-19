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

#include <cerrno>
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