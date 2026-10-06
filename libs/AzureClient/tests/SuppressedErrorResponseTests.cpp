#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>

namespace
{
    using AVEVA::AzureClient::BlobClient;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;

    struct Observed
    {
        bool Succeeded = false;
        unsigned int RawStatus = 0;
        std::string RawRequestId;
        std::string ErrorRequestId;
    };

    // Runs a suppressing operation against `status`/`code` and records what the caller sees.
    template <class TStart>
    [[nodiscard]] Observed RunSuppressed(FakeHttpClient& httpClient,
        unsigned int status,
        std::string_view code,
        TStart start)
    {
        httpClient.EnqueueResponse(MakeAzureErrorResponse(status, code, "suppressed", "req-1"));
        std::optional<Observed> observed;
        start([&](auto result)
        {
            Observed value;
            value.Succeeded = result.has_value();
            if (result.has_value())
            {
                value.RawStatus = result->RawResponse().GetStatus();
                for (const auto& header : result->RawResponse().GetHeaders())
                {
                    if (header.GetName() == "x-ms-request-id")
                    {
                        value.RawRequestId = header.GetValue();
                    }
                }
                value.ErrorRequestId = result->Error().has_value() ? result->Error()->RequestId : std::string{};
            }
            observed = value;
        });
        httpClient.Poll();
        EXPECT_TRUE(observed.has_value());
        return observed.value_or(Observed{});
    }

    void ExpectSuppressedWithRealResponse(const Observed& observed, unsigned int status)
    {
        EXPECT_TRUE(observed.Succeeded);
        EXPECT_EQ(observed.RawStatus, status);
        EXPECT_EQ(observed.RawRequestId, "req-1");
        EXPECT_EQ(observed.ErrorRequestId, "req-1");
    }
} // namespace

TEST(SuppressedErrorResponseTests, OtherFailuresAreStillReported)
{
    FakeHttpClient httpClient;
    PageBlobClient blockBlob{httpClient, MakeBlobClientOptions()};
    BlobContainerClient container{httpClient, MakeBlobContainerClientOptions()};

    EXPECT_FALSE(RunSuppressed(httpClient,
        403,
        "AuthorizationFailure",
        [&](auto completion)
    {
        blockBlob.DeleteIfExistsAsync(std::move(completion));
    }).Succeeded);
    EXPECT_FALSE(RunSuppressed(httpClient,
        409,
        "ContainerBeingDeleted",
        [&](auto completion)
    {
        container.CreateIfNotExistsAsync(std::move(completion));
    }).Succeeded);
}
