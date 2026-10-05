// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"

#include <gtest/gtest.h>

#include <cerrno>
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

TEST(AzureErrorTranslatorTests, TransportCancellationIsRetryableTimedOut) {
    auto status =
        AzureErrorTranslator::IOStatusFromError(Failure(0, "", std::make_error_code(std::errc::operation_canceled)));
    EXPECT_TRUE(status.IsTimedOut());
    EXPECT_TRUE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, ConnectionFailureIsRetryableIOError) {
    auto status =
        AzureErrorTranslator::IOStatusFromError(Failure(0, "", std::make_error_code(std::errc::connection_reset)));
    EXPECT_TRUE(status.IsIOError());
    EXPECT_TRUE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, StatusTextIncludesCodeAndMessage) {
    auto text = AzureErrorTranslator::IOStatusFromError(Failure(500, "InternalError")).ToString();
    EXPECT_NE(text.find("InternalError"), std::string::npos);
    EXPECT_NE(text.find("boom"), std::string::npos);
}

TEST(AzureErrorTranslatorTests, LegacyOverloadKeepsRetryableTimeout) {
    auto status = AzureErrorTranslator::IOStatusFromError("ctx", HttpStatus::RequestTimeout);
    EXPECT_TRUE(status.IsTimedOut());
    EXPECT_TRUE(status.GetRetryable());
}

TEST(AzureErrorTranslatorTests, TransientClassification) {
    for (unsigned int code : {0u, 408u, 429u, 500u, 502u, 503u, 504u}) {
        EXPECT_TRUE(AzureErrorTranslator::IsTransient(Failure(code))) << code;
    }

    for (unsigned int code : {400u, 403u, 404u, 409u, 412u}) {
        EXPECT_FALSE(AzureErrorTranslator::IsTransient(Failure(code))) << code;
    }
}
