#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace
{
    using AVEVA::HttpHeader;
    using AVEVA::HttpRequest;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::AcquireLeaseOptions;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::ReleaseLeaseOptions;
    using AVEVA::AzureClient::RenewLeaseOptions;
    using AVEVA::AzureClient::Models::BlobRequestConditions;
    using AVEVA::AzureClient::Tests::DefaultETag;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
    using namespace std::chrono_literals;

    [[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name)
    {
        return FakeHttpClient::FindHeaderValue(request, name);
    }

    [[nodiscard]] HttpResponse LeaseResponse(unsigned int status, std::string leaseId)
    {
        std::vector<HttpHeader> headers = MakeCanonicalSuccessHeaders();
        headers.emplace_back("x-ms-lease-id", std::move(leaseId));
        return HttpResponse{status, std::move(headers), ""};
    }

    [[nodiscard]] BlobRequestConditions IfUnmodifiedSince()
    {
        BlobRequestConditions conditions;
        conditions.IfUnmodifiedSince = std::chrono::sys_days{std::chrono::year{2015} / 6 / 26} + 18h + 59min + 17s;
        return conditions;
    }

    const HttpRequest& RenewBlobLease(FakeHttpClient& httpClient,
        BlockBlobClient& client,
        std::string leaseId,
        BlobRequestConditions conditions,
        std::optional<std::string>& renewed)
    {
        client.RenewLeaseAsync(RenewLeaseOptions{.LeaseId = std::move(leaseId), .Conditions = std::move(conditions)},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            renewed = result->Value().LeaseId;
        });
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& ReleaseBlobLease(FakeHttpClient& httpClient,
        BlockBlobClient& client,
        std::string leaseId,
        BlobRequestConditions conditions)
    {
        client.ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = std::move(leaseId), .Conditions = std::move(conditions)},
            [](auto) {});
        httpClient.Poll();
        return httpClient.LastRequest();
    }
} // namespace

TEST(LeaseTests, BlobRenewReleaseRejectEmptyLeaseIds)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    int completed = 0;
    const auto expectInvalid = [&](auto result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
        ++completed;
    };
    client.RenewLeaseAsync(RenewLeaseOptions{}, expectInvalid);
    client.ReleaseLeaseAsync(ReleaseLeaseOptions{}, expectInvalid);
    httpClient.Poll();
    EXPECT_EQ(completed, 2);
    EXPECT_TRUE(httpClient.NoRequestMade());
}

TEST(LeaseTests, BlobRenewAndReleaseSendLeaseActionAndConditions)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(LeaseResponse(200, "lease-a"));
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    std::optional<std::string> renewed;
    const HttpRequest renew = RenewBlobLease(httpClient, client, "lease-a", {}, renewed);
    EXPECT_EQ(Header(renew, "x-ms-lease-action"), "renew");
    EXPECT_NE(renew.GetUrl().find("comp=lease"), std::string::npos);
    EXPECT_EQ(renewed, "lease-a");

    const HttpRequest release = ReleaseBlobLease(httpClient, client, "lease-a", IfUnmodifiedSince());
    EXPECT_EQ(Header(release, "x-ms-lease-action"), "release");
    EXPECT_EQ(Header(release, "If-Unmodified-Since"), "Fri, 26 Jun 2015 18:59:17 GMT");
}

TEST(LeaseTests, BlobAcquireLeaseValidatesDuration)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    std::optional<std::error_code> error;
    client.AcquireLeaseAsync(AcquireLeaseOptions{.Conditions = {}, .ProposedLeaseId = {}, .Duration = 5s},
        [&](auto result)
    {
        ASSERT_FALSE(result.has_value());
        error = result.error().Code;
    });
    httpClient.Poll();
    EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}
