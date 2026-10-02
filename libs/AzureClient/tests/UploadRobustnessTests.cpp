#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "BlobRequestHelpers.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <expected>
#include <gtest/gtest.h>

#include <cstddef>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::BlockBlobClient;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::UploadFromOptions;
using AVEVA::AzureClient::Models::UploadBlockBlobResult;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;

namespace
{
    using UploadResult = std::expected<Response<UploadBlockBlobResult>, BlobStorageError>;

    [[nodiscard]] std::optional<UploadResult> RunUpload(FakeHttpClient& httpClient,
        BlockBlobClient& client,
        const std::string& data,
        const UploadFromOptions& options)
    {
        std::istringstream stream(data);
        std::optional<UploadResult> result;
        client.UploadFromAsync(stream,
            options,
            [&](UploadResult value)
        {
            result = std::move(value);
        });
        EXPECT_TRUE(httpClient.RunUntil([&]
        {
            return result.has_value();
        }));
        return result;
    }

} // namespace

TEST(UploadRobustnessTests, B01_ConcurrentUploadsUseDisjointBlockIds)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    UploadFromOptions options;
    options.BlockSize = 2U;

    ASSERT_TRUE(RunUpload(httpClient, client, "abcdef", options)->has_value());
    const std::size_t firstRequests = httpClient.RequestCount();
    const std::vector<std::string> first = httpClient.StagedBlockIds();
    ASSERT_TRUE(RunUpload(httpClient, client, "abcdef", options)->has_value());
    const std::vector<std::string> all = httpClient.StagedBlockIds();

    ASSERT_EQ(first.size(), 3U);
    ASSERT_EQ(all.size(), 6U);
    const std::set<std::string> firstSet(first.begin(), first.end());
    EXPECT_EQ(firstSet.size(), 3U);
    for (std::size_t i = 3U; i < all.size(); ++i)
    {
        EXPECT_FALSE(firstSet.contains(all.at(i))) << all.at(i);
    }
    for (const std::string& id : all)
    {
        EXPECT_EQ(id.size(), all.front().size());
    }
    EXPECT_GT(httpClient.RequestCount(), firstRequests);
}

TEST(UploadRobustnessTests, B01_CommitListsExactlyTheStagedIdsInOrder)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    UploadFromOptions options;
    options.BlockSize = 2U;
    options.Concurrency = 2U;

    ASSERT_TRUE(RunUpload(httpClient, client, "abcdefg", options)->has_value());
    const std::vector<std::string> ids = httpClient.StagedBlockIds();
    ASSERT_EQ(ids.size(), 4U);

    std::string expected;
    for (const std::string& id : ids)
    {
        expected += "<Latest>" + id + "</Latest>";
    }
    const std::string& body = httpClient.Requests().back().Body;
    EXPECT_NE(body.find(expected), std::string::npos) << body;
}

TEST(UploadRobustnessTests, B01_UploadBlockIdsAreValidAndEqualLength)
{
    const std::string prefix = AVEVA::AzureClient::Private::CreateUploadBlockIdPrefix();
    EXPECT_EQ(prefix.size(), 32U);
    EXPECT_NE(prefix, AVEVA::AzureClient::Private::CreateUploadBlockIdPrefix());
    const std::string a = AVEVA::AzureClient::Private::EncodeUploadBlockId(prefix, 0U);
    const std::string b = AVEVA::AzureClient::Private::EncodeUploadBlockId(prefix, 49999U);
    EXPECT_EQ(a.size(), b.size());
    EXPECT_NO_THROW(AVEVA::AzureClient::Private::ValidateBlockId(a));
}

TEST(UploadRobustnessTests, B04_BlockSizeAboveServiceLimitFailsWithoutRequest)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    UploadFromOptions options;
    options.BlockSize = (std::size_t{4000} * 1024U * 1024U) + 1U;

    const auto result = RunUpload(httpClient, client, "abc", options);
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_TRUE(httpClient.NoRequestMade());
}

TEST(UploadRobustnessTests, B04_HugeBlockSizeDoesNotThrow)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    UploadFromOptions options;
    options.BlockSize = std::numeric_limits<std::size_t>::max() / 2U;

    std::optional<UploadResult> result;
    std::istringstream stream("abc");
    ASSERT_NO_THROW(client.UploadFromAsync(stream,
        options,
        [&](UploadResult value)
    {
        result = std::move(value);
    }));
    ASSERT_TRUE(httpClient.RunUntil([&]
    {
        return result.has_value();
    }));
    ASSERT_FALSE(result->has_value());
    const std::error_code code = result->error().Code;
    EXPECT_TRUE(code == std::errc::not_enough_memory || code == std::errc::invalid_argument) << code.message();
}

TEST(UploadRobustnessTests, B04_MalformedCommitLastModifiedIsInvalidResponse)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, {{"ETag", "\"e\""}, {"Last-Modified", "not a date"}}, ""};
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    UploadFromOptions options;
    options.BlockSize = 2U;

    const auto result = RunUpload(httpClient, client, "abcd", options);
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, BlobStorageErrorCode::InvalidResponse);
}
