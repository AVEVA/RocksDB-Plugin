#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <expected>
#include <gtest/gtest.h>

#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include <sstream>
#include <string>
#include <utility>

using namespace AVEVA::AzureClient;
using namespace AVEVA::AzureClient::Tests;

namespace
{
    void StartUploadFromAsyncAndVerifySuccess(BlockBlobClient& client,
        std::istringstream& input,
        UploadFromOptions options,
        bool& callbackInvoked)
    {
        client.UploadFromAsync(input,
            std::move(options),
            [&](std::expected<Response<Models::UploadBlockBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            callbackInvoked = true;
        });
    }

    void VerifySharedKeyAuthorizationOnStageBlockRequests(const FakeHttpClient& httpClient, bool& sawStage)
    {
        for (const auto& rec : httpClient.Requests())
        {
            const auto& req = rec.Request;
            const std::string url = req.GetUrl();
            if (url.contains("comp=block&blockid="))
            {
                sawStage = true;
                const std::string auth = FakeHttpClient::FindHeaderValue(req, "Authorization");
                EXPECT_FALSE(auth.empty());
                EXPECT_TRUE(auth.rfind("SharedKey storageaccount:", 0) == 0);
            }
        }
    }
} // namespace

TEST(T08_UploadFromSharedKeyTests, UploadFromAsync_StreamStagesBlocksWithSharedKeyAuthorization)
{
    FakeHttpClient httpClient;
    // default responses (201) are fine for stage/commit
    BlobClientOptions opts = MakeBlobClientOptions();
    opts.SasToken.clear();
    opts.SharedKey = {.AccountName = "storageaccount",
        .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="}; // base64 for '0123456789abcdef'

    BlockBlobClient client{httpClient, opts};

    std::istringstream input("abcdefghijklmnopqrstuvwxyz");
    UploadFromOptions options;
    options.BlockSize = 5; // force multiple blocks

    bool callbackInvoked = false;
    StartUploadFromAsyncAndVerifySuccess(client, input, options, callbackInvoked);

    // Allow any deferred completions to run
    httpClient.Poll();

    ASSERT_TRUE(callbackInvoked);

    // Verify every StageBlock request had SharedKey Authorization header
    bool sawStage = false;
    VerifySharedKeyAuthorizationOnStageBlockRequests(httpClient, sawStage);

    EXPECT_TRUE(sawStage);
}
