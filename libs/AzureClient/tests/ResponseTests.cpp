#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/Response.hpp>

#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::Response;
} // namespace

TEST(ResponseTests, DefaultConstructedResponseHasDefaultState)
{
    Response<std::string> response;

    EXPECT_TRUE(response.Value().empty());
    EXPECT_EQ(response.RawResponse().GetStatus(), 0U);
    EXPECT_FALSE(response.Error().has_value());
}

TEST(ResponseTests, MoveConstructionPreservesValueRawResponseAndError)
{
    Response<std::vector<int>> original{std::vector<int>{1, 2, 3},
        HttpResponse{206, {{"x-ms-request-id", "req-1"}}, "body"},
        BlobStorageError{.StatusCode = 409, .ErrorCode = "Conflict", .Message = "message", .RequestId = "req-1"}};

    Response<std::vector<int>> moved{std::move(original)};

    ASSERT_EQ(moved.Value().size(), 3U);
    EXPECT_EQ(moved.Value().at(0), 1);
    EXPECT_EQ(moved.RawResponse().GetStatus(), 206U);
    ASSERT_TRUE(moved.Error().has_value());
    EXPECT_EQ(ValueOrFail(moved.Error()).ErrorCode, "Conflict");
}

TEST(ResponseTests, RvalueQualifiedValueMovesOutLargePayloads)
{
    Response<std::unique_ptr<int>> response{std::make_unique<int>(42), HttpResponse{200, {}, ""}};

    std::unique_ptr<int> const value = std::move(response).Value();

    ASSERT_TRUE(value);
    EXPECT_EQ(*value, 42);
}

TEST(ResponseTests, RvalueQualifiedRawResponseAndErrorMoveOutWithoutRelyingOnSource)
{
    const auto makeResponse = []
    {
        return Response<std::string>{"value",
            HttpResponse{206, {{"x-ms-request-id", "req-1"}}, "large-body-payload"},
            BlobStorageError{.StatusCode = 409, .ErrorCode = "Conflict", .Message = "message", .RequestId = "req-1"}};
    };

    // Each rvalue accessor moves out a different member, so they are exercised on separate (identical)
    // responses rather than chained on one moved-from object.
    HttpResponse rawResponse = makeResponse().RawResponse();
    std::optional<BlobStorageError> error = makeResponse().Error();

    EXPECT_EQ(rawResponse.GetStatus(), 206U);
    EXPECT_EQ(rawResponse.GetBody(), "large-body-payload");
    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(ValueOrFail(error).ErrorCode, "Conflict");
}

TEST(ResponseTests, MutableLvalueAccessorsAllowInPlaceModificationAndMoveOut)
{
    Response<std::string> response{"value",
        HttpResponse{206, {{"x-ms-request-id", "req-1"}}, "body"},
        BlobStorageError{.StatusCode = 409, .ErrorCode = "Conflict"}};

    response.Value() = "changed";
    response.Error().reset();
    HttpResponse& raw = response.RawResponse();
    HttpResponse moved = std::move(raw);

    EXPECT_EQ(response.Value(), "changed");
    EXPECT_FALSE(response.Error().has_value());
    EXPECT_EQ(moved.GetStatus(), 206U);
    EXPECT_EQ(moved.GetBody(), "body");
}

TEST(ResponseTests, ErrorCodesCompareEqualToDocumentedPortableConditions)
{
    using AVEVA::AzureClient::BlobStorageErrorCode;

    struct Row
    {
        BlobStorageErrorCode Code;
        std::errc Condition;
    };

    const std::vector<Row> rows{
        {BlobStorageErrorCode::ContainerNotFound, std::errc::no_such_file_or_directory},
        {BlobStorageErrorCode::ResourceNotFound, std::errc::no_such_file_or_directory},
        {BlobStorageErrorCode::BlobNotFound, std::errc::no_such_file_or_directory},
        {BlobStorageErrorCode::ContainerAlreadyExists, std::errc::file_exists},
        {BlobStorageErrorCode::BlobAlreadyExists, std::errc::file_exists},
        {BlobStorageErrorCode::AuthenticationFailed, std::errc::permission_denied},
        {BlobStorageErrorCode::AuthorizationFailure, std::errc::permission_denied},
        {BlobStorageErrorCode::InvalidAuthenticationInfo, std::errc::permission_denied},
        {BlobStorageErrorCode::ServerBusy, std::errc::resource_unavailable_try_again},
        {BlobStorageErrorCode::OperationTimedOut, std::errc::timed_out},
        {BlobStorageErrorCode::RequestBodyTooLarge, std::errc::file_too_large},
        {BlobStorageErrorCode::InvalidResponse, std::errc::bad_message},
        {BlobStorageErrorCode::InvalidMetadata, std::errc::invalid_argument},
    };
    for (const Row& row : rows)
    {
        BlobStorageError error;
        error.Code = make_error_code(row.Code);
        EXPECT_EQ(error.Code, row.Condition) << static_cast<int>(row.Code);
    }
}

TEST(ResponseTests, MovingMembersOutInDifferentOrdersLeavesEachIntact)
{
    const auto makeResponse = []
    {
        return Response<std::string>{"value",
            HttpResponse{206, {{"x-ms-request-id", "req-1"}}, "body"},
            BlobStorageError{.StatusCode = 409, .ErrorCode = "Conflict"}};
    };

    Response<std::string> first = makeResponse();
    std::optional<BlobStorageError> error = std::move(first.Error());
    HttpResponse raw = std::move(first.RawResponse());
    std::string value = std::move(first.Value());
    EXPECT_EQ(value, "value");
    EXPECT_EQ(raw.GetStatus(), 206U);
    ASSERT_TRUE(error.has_value());

    Response<std::string> second = makeResponse();
    std::string value2 = std::move(second.Value());
    std::optional<BlobStorageError> error2 = std::move(second.Error());
    HttpResponse raw2 = std::move(second.RawResponse());
    EXPECT_EQ(value2, "value");
    EXPECT_EQ(raw2.GetBody(), "body");
    EXPECT_EQ(ValueOrFail(error2).ErrorCode, "Conflict");
}
