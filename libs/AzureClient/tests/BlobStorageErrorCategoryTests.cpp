// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "BlobRequestHelpers.hpp"
#include "BlobStorageErrorCategory.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/HttpClient/HttpClientError.hpp>

#include <cstddef>
#include <gtest/gtest.h>

#include <array>
#include <initializer_list>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::IsTransient;
    using AVEVA::AzureClient::make_error_code;
    using AVEVA::AzureClient::Private::BlobStorageErrorCategory;
    using AVEVA::AzureClient::Private::ParseBlobStorageErrorCode;

    void ExpectEnumeratorHasMessageAndRoundTrips(const BlobStorageErrorCategory& category,
        const std::string& generic,
        BlobStorageErrorCode code,
        std::string_view serviceName)
    {
        SCOPED_TRACE(static_cast<int>(code));
        const std::string message = category.message(static_cast<int>(code));
        EXPECT_FALSE(message.empty());
        if (!serviceName.empty())
        {
            EXPECT_NE(message, generic) << serviceName << " needs a specific message";
            EXPECT_EQ(ParseBlobStorageErrorCode(serviceName), code) << serviceName;
        }
    }

    void ExpectStatusesAreTransient(std::initializer_list<unsigned int> statuses)
    {
        for (const unsigned int status : statuses)
        {
            EXPECT_TRUE(IsTransient(
                BlobStorageError{.StatusCode = status, .Code = make_error_code(BlobStorageErrorCode::ServiceError)}))
                << status;
        }
    }

    void ExpectStatusesAreNotTransient(std::initializer_list<unsigned int> statuses)
    {
        for (const unsigned int status : statuses)
        {
            EXPECT_FALSE(IsTransient(
                BlobStorageError{.StatusCode = status, .Code = make_error_code(BlobStorageErrorCode::ServiceError)}))
                << status;
        }
    }
} // namespace

TEST(BlobStorageErrorCategoryTests, ParseBlobStorageErrorCode_MapsEveryKnownCode)
{
    static constexpr std::array<std::pair<std::string_view, BlobStorageErrorCode>, 14> KnownCodes{{
        {"ContainerAlreadyExists", BlobStorageErrorCode::ContainerAlreadyExists},
        {"ContainerNotFound", BlobStorageErrorCode::ContainerNotFound},
        {"ContainerBeingDeleted", BlobStorageErrorCode::ContainerBeingDeleted},
        {"ContainerDisabled", BlobStorageErrorCode::ContainerDisabled},
        {"InvalidResourceName", BlobStorageErrorCode::InvalidResourceName},
        {"ResourceNotFound", BlobStorageErrorCode::ResourceNotFound},
        {"AuthenticationFailed", BlobStorageErrorCode::AuthenticationFailed},
        {"AuthorizationFailure", BlobStorageErrorCode::AuthorizationFailure},
        {"InvalidAuthenticationInfo", BlobStorageErrorCode::InvalidAuthenticationInfo},
        {"ConditionNotMet", BlobStorageErrorCode::ConditionNotMet},
        {"BlobNotFound", BlobStorageErrorCode::BlobNotFound},
        {"BlobAlreadyExists", BlobStorageErrorCode::BlobAlreadyExists},
        {"InvalidBlobOrBlock", BlobStorageErrorCode::InvalidBlobOrBlock},
        {"InvalidBlockList", BlobStorageErrorCode::InvalidBlockList},
    }};

    for (const auto& [name, expected] : KnownCodes)
    {
        EXPECT_EQ(ParseBlobStorageErrorCode(name), expected) << name;
    }
}

TEST(BlobStorageErrorCategoryTests, ParseBlobStorageErrorCode_FallsBackToServiceErrorForUnknownCode)
{
    EXPECT_EQ(ParseBlobStorageErrorCode("SomethingUnexpected"), BlobStorageErrorCode::ServiceError);
    EXPECT_EQ(ParseBlobStorageErrorCode(""), BlobStorageErrorCode::ServiceError);
}

TEST(BlobStorageErrorCategoryTests, CategoryNameAndMessages_AreStable)
{
    BlobStorageErrorCategory category;
    EXPECT_STREQ(category.name(), "aveva.azure-client.blob-storage");

    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::ContainerAlreadyExists)),
        "The specified container already exists");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::ContainerNotFound)),
        "The specified container does not exist");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::ContainerBeingDeleted)),
        "The specified container is being deleted");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::ContainerDisabled)),
        "The specified container has been disabled");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::InvalidResourceName)),
        "The specified resource name is invalid");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::ResourceNotFound)),
        "The specified resource does not exist");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::AuthenticationFailed)),
        "Server failed to authenticate the request");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::AuthorizationFailure)),
        "The caller is not authorized to perform this operation");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::InvalidAuthenticationInfo)),
        "The authentication information was not provided in the correct format");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::ConditionNotMet)),
        "The condition specified in the request was not met");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::BlobNotFound)),
        "The specified blob does not exist");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::BlobAlreadyExists)),
        "The specified blob already exists");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::InvalidBlobOrBlock)),
        "The specified blob or block content is invalid");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::InvalidBlockList)),
        "The specified block list is invalid");
    EXPECT_EQ(category.message(static_cast<int>(BlobStorageErrorCode::ServiceError)),
        "The Blob Storage service returned an error response");
    EXPECT_EQ(category.message(9999), "The Blob Storage service returned an error response");
}

TEST(BlobStorageErrorCategoryTests, MakeErrorCode_ComparesByCategoryAndValue)
{
    const std::error_code notFound = AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::ContainerNotFound);
    const std::error_code alsoNotFound = AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::ContainerNotFound);
    const std::error_code alreadyExists =
        AVEVA::AzureClient::make_error_code(BlobStorageErrorCode::ContainerAlreadyExists);
    const std::error_code genericNotFound = std::make_error_code(std::errc::no_such_file_or_directory);

    EXPECT_EQ(notFound, BlobStorageErrorCode::ContainerNotFound);
    EXPECT_EQ(notFound, alsoNotFound);
    EXPECT_NE(notFound, alreadyExists);
    EXPECT_NE(notFound, genericNotFound);
    EXPECT_STREQ(notFound.category().name(), "aveva.azure-client.blob-storage");
}

namespace
{
    struct EnumeratorCase
    {
        BlobStorageErrorCode Code;
        std::string_view ServiceName; // empty for client-side / catch-all codes
    };

    // Every enumerator, in declaration order. Adding an enumerator without a row here fails
    // EveryEnumerator_IsCovered below.
    constexpr std::array<EnumeratorCase, 35> AllEnumerators{{
        {.Code = BlobStorageErrorCode::ContainerAlreadyExists, .ServiceName = "ContainerAlreadyExists"},
        {.Code = BlobStorageErrorCode::ContainerNotFound, .ServiceName = "ContainerNotFound"},
        {.Code = BlobStorageErrorCode::ContainerBeingDeleted, .ServiceName = "ContainerBeingDeleted"},
        {.Code = BlobStorageErrorCode::ContainerDisabled, .ServiceName = "ContainerDisabled"},
        {.Code = BlobStorageErrorCode::InvalidResourceName, .ServiceName = "InvalidResourceName"},
        {.Code = BlobStorageErrorCode::ResourceNotFound, .ServiceName = "ResourceNotFound"},
        {.Code = BlobStorageErrorCode::AuthenticationFailed, .ServiceName = "AuthenticationFailed"},
        {.Code = BlobStorageErrorCode::AuthorizationFailure, .ServiceName = "AuthorizationFailure"},
        {.Code = BlobStorageErrorCode::InvalidAuthenticationInfo, .ServiceName = "InvalidAuthenticationInfo"},
        {.Code = BlobStorageErrorCode::ConditionNotMet, .ServiceName = "ConditionNotMet"},
        {.Code = BlobStorageErrorCode::BlobNotFound, .ServiceName = "BlobNotFound"},
        {.Code = BlobStorageErrorCode::BlobAlreadyExists, .ServiceName = "BlobAlreadyExists"},
        {.Code = BlobStorageErrorCode::InvalidBlobOrBlock, .ServiceName = "InvalidBlobOrBlock"},
        {.Code = BlobStorageErrorCode::InvalidBlockList, .ServiceName = "InvalidBlockList"},
        {.Code = BlobStorageErrorCode::ServiceError, .ServiceName = ""},
        {.Code = BlobStorageErrorCode::LeaseIdMissing, .ServiceName = "LeaseIdMissing"},
        {.Code = BlobStorageErrorCode::LeaseAlreadyPresent, .ServiceName = "LeaseAlreadyPresent"},
        {.Code = BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation,
            .ServiceName = "LeaseIdMismatchWithBlobOperation"},
        {.Code = BlobStorageErrorCode::LeaseNotPresentWithBlobOperation,
            .ServiceName = "LeaseNotPresentWithBlobOperation"},
        {.Code = BlobStorageErrorCode::LeaseLost, .ServiceName = "LeaseLost"},
        {.Code = BlobStorageErrorCode::ServerBusy, .ServiceName = "ServerBusy"},
        {.Code = BlobStorageErrorCode::OperationTimedOut, .ServiceName = "OperationTimedOut"},
        {.Code = BlobStorageErrorCode::InternalError, .ServiceName = "InternalError"},
        {.Code = BlobStorageErrorCode::InvalidRange, .ServiceName = "InvalidRange"},
        {.Code = BlobStorageErrorCode::TargetConditionNotMet, .ServiceName = "TargetConditionNotMet"},
        {.Code = BlobStorageErrorCode::AppendPositionConditionNotMet, .ServiceName = "AppendPositionConditionNotMet"},
        {.Code = BlobStorageErrorCode::MaxBlobSizeConditionNotMet, .ServiceName = "MaxBlobSizeConditionNotMet"},
        {.Code = BlobStorageErrorCode::BlobArchived, .ServiceName = "BlobArchived"},
        {.Code = BlobStorageErrorCode::Md5Mismatch, .ServiceName = "Md5Mismatch"},
        {.Code = BlobStorageErrorCode::InvalidHeaderValue, .ServiceName = "InvalidHeaderValue"},
        {.Code = BlobStorageErrorCode::InvalidQueryParameterValue, .ServiceName = "InvalidQueryParameterValue"},
        {.Code = BlobStorageErrorCode::RequestBodyTooLarge, .ServiceName = "RequestBodyTooLarge"},
        {.Code = BlobStorageErrorCode::BlobTierInadequateForContentLength,
            .ServiceName = "BlobTierInadequateForContentLength"},
        {.Code = BlobStorageErrorCode::InvalidMetadata, .ServiceName = "InvalidMetadata"},
        {.Code = BlobStorageErrorCode::InvalidResponse, .ServiceName = ""},
    }};
} // namespace

TEST(BlobStorageErrorCategoryTests, EveryEnumerator_IsCovered)
{
    for (std::size_t i = 0; i < AllEnumerators.size(); ++i)
    {
        EXPECT_EQ(static_cast<std::size_t>(AllEnumerators.at(i).Code), i + 1U) << "row " << i;
    }
    EXPECT_EQ(AllEnumerators.back().Code, BlobStorageErrorCode::InvalidResponse)
        << "InvalidResponse must stay the last enumerator";
}

TEST(BlobStorageErrorCategoryTests, EveryEnumerator_HasMessageAndRoundTripsThroughParse)
{
    BlobStorageErrorCategory category;
    const std::string generic = category.message(static_cast<int>(BlobStorageErrorCode::ServiceError));
    for (const auto& [code, serviceName] : AllEnumerators)
    {
        ExpectEnumeratorHasMessageAndRoundTrips(category, generic, code, serviceName);
    }
    EXPECT_EQ(ParseBlobStorageErrorCode("InvalidResponse"), BlobStorageErrorCode::ServiceError)
        << "client-side code is not a service code";
    EXPECT_EQ(ParseBlobStorageErrorCode("blobnotfound"), BlobStorageErrorCode::ServiceError)
        << "service codes are case-sensitive";
}

TEST(BlobStorageErrorCategoryTests, ErrorCodes_MapToPortableConditions)
{
    using AVEVA::AzureClient::make_error_code;
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::BlobNotFound), std::errc::no_such_file_or_directory);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::ContainerNotFound), std::errc::no_such_file_or_directory);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::ResourceNotFound), std::errc::no_such_file_or_directory);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::BlobAlreadyExists), std::errc::file_exists);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::ContainerAlreadyExists), std::errc::file_exists);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::AuthenticationFailed), std::errc::permission_denied);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::AuthorizationFailure), std::errc::permission_denied);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::InvalidAuthenticationInfo), std::errc::permission_denied);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::OperationTimedOut), std::errc::timed_out);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::InvalidResponse), std::errc::bad_message);
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::InvalidMetadata), std::errc::invalid_argument);

    EXPECT_NE(make_error_code(BlobStorageErrorCode::ConditionNotMet), std::errc::no_such_file_or_directory);
    EXPECT_NE(make_error_code(BlobStorageErrorCode::ServiceError), std::errc::file_exists);
    // Unmapped codes keep their own category as the default condition.
    EXPECT_EQ(make_error_code(BlobStorageErrorCode::LeaseLost).default_error_condition().category().name(),
        std::string{"aveva.azure-client.blob-storage"});
}

TEST(BlobStorageErrorCategoryTests, EnumeratorValuesArePinned)
{
    using AVEVA::AzureClient::BlobStorageErrorCode;
    static_assert(static_cast<int>(BlobStorageErrorCode::ContainerAlreadyExists) == 1);
    static_assert(static_cast<int>(BlobStorageErrorCode::ContainerNotFound) == 2);
    static_assert(static_cast<int>(BlobStorageErrorCode::BlobNotFound) == 11);
    static_assert(static_cast<int>(BlobStorageErrorCode::ServiceError) == 15);
    SUCCEED();
}

TEST(BlobStorageErrorCategoryTests, TransportFailureHasZeroStatusAndNoServiceFields)
{
    const auto failure =
        AVEVA::AzureClient::Private::DetermineBlobStorageFailure(std::make_error_code(std::errc::connection_reset),
            AVEVA::HttpResponse{});
    EXPECT_EQ(failure.Error, std::make_error_code(std::errc::connection_reset));
    EXPECT_FALSE(failure.Details.has_value());
}

TEST(BlobStorageErrorCategoryTests, DetermineFailure_Maps304WithoutErrorCodeToConditionNotMet)
{
    using AVEVA::AzureClient::Private::DetermineBlobStorageFailure;
    const auto failure =
        DetermineBlobStorageFailure({}, AVEVA::HttpResponse{304, {{"x-ms-request-id", "req-304"}}, ""});
    EXPECT_EQ(failure.Error, BlobStorageErrorCode::ConditionNotMet);
    ASSERT_TRUE(failure.Details.has_value());
    EXPECT_EQ(ValueOrFail(failure.Details).StatusCode, 304U);
    EXPECT_EQ(ValueOrFail(failure.Details).RequestId, "req-304");

    const auto redirect = DetermineBlobStorageFailure({}, AVEVA::HttpResponse{302, {}, ""});
    EXPECT_EQ(redirect.Error, BlobStorageErrorCode::ServiceError);

    const auto newCode =
        DetermineBlobStorageFailure({}, AVEVA::HttpResponse{409, {{"x-ms-error-code", "LeaseAlreadyPresent"}}, ""});
    EXPECT_EQ(newCode.Error, BlobStorageErrorCode::LeaseAlreadyPresent);
}

TEST(BlobStorageErrorCategoryTests, IsTransient_ClassifiesStatusAndTransportFailures)
{
    ExpectStatusesAreTransient({408U, 429U, 500U, 502U, 503U, 504U});
    ExpectStatusesAreNotTransient({200U, 304U, 400U, 403U, 404U, 409U, 412U, 501U});

    EXPECT_TRUE(IsTransient(BlobStorageError{.Code = std::make_error_code(std::errc::connection_reset)}));
    EXPECT_TRUE(IsTransient(BlobStorageError{.Code = std::make_error_code(std::errc::timed_out)}));
    EXPECT_TRUE(IsTransient(BlobStorageError{.Code = AVEVA::HttpClientError::ConnectFailed}));
    EXPECT_TRUE(IsTransient(BlobStorageError{.Code = make_error_code(BlobStorageErrorCode::ServerBusy)}));
    EXPECT_FALSE(IsTransient(BlobStorageError{.Code = std::make_error_code(std::errc::invalid_argument)}));
    EXPECT_FALSE(IsTransient(BlobStorageError{.Code = std::make_error_code(std::errc::operation_canceled)}));
    EXPECT_FALSE(IsTransient(BlobStorageError{.Code = make_error_code(BlobStorageErrorCode::InvalidResponse)}));
}
