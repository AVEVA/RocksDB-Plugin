#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
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
    using AVEVA::HttpMethod;
    using AVEVA::HttpRequest;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::AcquireLeaseOptions;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::BreakLeaseOptions;
    using AVEVA::AzureClient::ChangeLeaseOptions;
    using AVEVA::AzureClient::ReleaseLeaseOptions;
    using AVEVA::AzureClient::RenewLeaseOptions;
    using AVEVA::AzureClient::Models::BlobRequestConditions;
    using AVEVA::AzureClient::Tests::DefaultETag;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
    using namespace std::chrono_literals;

    [[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name)
    {
        return FakeHttpClient::FindHeaderValue(request, name);
    }

    [[nodiscard]] bool HasQuery(const HttpRequest& request, std::string_view part)
    {
        return request.GetUrl().contains(part);
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

    const HttpRequest& ChangeBlobLease(FakeHttpClient& httpClient,
        BlockBlobClient& client,
        std::string leaseId,
        std::string proposedLeaseId,
        std::optional<std::string>& changed)
    {
        client.ChangeLeaseAsync(ChangeLeaseOptions{.LeaseId = std::move(leaseId),
                                    .ProposedLeaseId = std::move(proposedLeaseId),
                                    .Conditions = {}},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            changed = result->Value().LeaseId;
        });
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& ReleaseBlobLease(FakeHttpClient& httpClient,
        BlockBlobClient& client,
        std::string leaseId,
        BlobRequestConditions conditions)
    {
        client.ReleaseLeaseAsync(
            ReleaseLeaseOptions{.LeaseId = std::move(leaseId), .Conditions = std::move(conditions)},
            [](auto) {});
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& BreakBlobLease(FakeHttpClient& httpClient,
        BlockBlobClient& client,
        BlobRequestConditions conditions,
        std::chrono::seconds breakPeriod)
    {
        client.BreakLeaseAsync(BreakLeaseOptions{.Conditions = std::move(conditions), .BreakPeriod = breakPeriod},
            [](auto) {});
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& AcquireContainerLease(FakeHttpClient& httpClient,
        BlobContainerClient& client,
        std::string proposedLeaseId,
        std::chrono::seconds duration,
        int& completed,
        std::optional<std::string>& acquired)
    {
        client.AcquireLeaseAsync(
            AcquireLeaseOptions{.Conditions = {}, .ProposedLeaseId = std::move(proposedLeaseId), .Duration = duration},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            acquired = result->Value().LeaseId;
            ++completed;
        });
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& RenewContainerLease(FakeHttpClient& httpClient,
        BlobContainerClient& client,
        std::string leaseId,
        int& completed)
    {
        client.RenewLeaseAsync(RenewLeaseOptions{.LeaseId = std::move(leaseId), .Conditions = {}},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            ++completed;
        });
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& ChangeContainerLease(FakeHttpClient& httpClient,
        BlobContainerClient& client,
        std::string leaseId,
        std::string proposedLeaseId,
        int& completed)
    {
        client.ChangeLeaseAsync(ChangeLeaseOptions{.LeaseId = std::move(leaseId),
                                    .ProposedLeaseId = std::move(proposedLeaseId),
                                    .Conditions = {}},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().LeaseId, "lease-b");
            ++completed;
        });
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& ReleaseContainerLease(FakeHttpClient& httpClient,
        BlobContainerClient& client,
        std::string leaseId,
        BlobRequestConditions conditions,
        int& completed)
    {
        client.ReleaseLeaseAsync(
            ReleaseLeaseOptions{.LeaseId = std::move(leaseId), .Conditions = std::move(conditions)},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            ++completed;
        });
        httpClient.Poll();
        return httpClient.LastRequest();
    }

    const HttpRequest& BreakContainerLease(FakeHttpClient& httpClient,
        BlobContainerClient& client,
        int& completed,
        std::optional<int>& leaseTimeSeconds)
    {
        client.BreakLeaseAsync([&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->Value().LeaseTimeSeconds.has_value());
            leaseTimeSeconds = *result->Value().LeaseTimeSeconds;
            ++completed;
        });
        httpClient.Poll();
        return httpClient.LastRequest();
    }
} // namespace

namespace
{
    template <class TClient> void ExpectEmptyLeaseIdsRejected(TClient& client, FakeHttpClient& httpClient)
    {
        int completed = 0;
        const auto expectInvalid = [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
            ++completed;
        };
        client.RenewLeaseAsync(RenewLeaseOptions{}, expectInvalid);
        client.ReleaseLeaseAsync(ReleaseLeaseOptions{}, expectInvalid);
        client.ChangeLeaseAsync(ChangeLeaseOptions{.LeaseId = "a"}, expectInvalid);
        client.ChangeLeaseAsync(ChangeLeaseOptions{.ProposedLeaseId = "b"}, expectInvalid);
        httpClient.Poll();
        EXPECT_EQ(completed, 4);
        EXPECT_TRUE(httpClient.NoRequestMade());
    }
} // namespace

TEST(LeaseTests, BlobRenewReleaseChangeRejectEmptyLeaseIds)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    ExpectEmptyLeaseIdsRejected(client, httpClient);
}

TEST(LeaseTests, ContainerRenewReleaseChangeRejectEmptyLeaseIds)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};
    ExpectEmptyLeaseIdsRejected(client, httpClient);
}

TEST(LeaseTests, BlobRenewAndChangeLeaseSendExpectedHeadersAndParseLeaseId)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(LeaseResponse(200, "lease-1"));
    httpClient.EnqueueResponse(LeaseResponse(200, "lease-2"));
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    std::optional<std::string> renewed;
    const HttpRequest& renew = RenewBlobLease(httpClient, client, "lease-1", IfUnmodifiedSince(), renewed);
    EXPECT_EQ(renew.GetMethod(), HttpMethod::Put);
    EXPECT_TRUE(HasQuery(renew, "comp=lease"));
    EXPECT_EQ(Header(renew, "x-ms-lease-action"), "renew");
    EXPECT_EQ(Header(renew, "x-ms-lease-id"), "lease-1");
    EXPECT_EQ(Header(renew, "If-Unmodified-Since"), "Fri, 26 Jun 2015 18:59:17 GMT");
    EXPECT_EQ(renewed, "lease-1");

    std::optional<std::string> changed;
    const HttpRequest& change = ChangeBlobLease(httpClient, client, "lease-1", "lease-2", changed);
    EXPECT_EQ(Header(change, "x-ms-lease-action"), "change");
    EXPECT_EQ(Header(change, "x-ms-lease-id"), "lease-1");
    EXPECT_EQ(Header(change, "x-ms-proposed-lease-id"), "lease-2");
    EXPECT_EQ(changed, "lease-2");
}

TEST(LeaseTests, BlobReleaseAndBreakHonourConditionsAndBreakSendsNoLeaseId)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    BlobRequestConditions conditions;
    conditions.IfMatch = "\"etag\"";
    conditions.LeaseId = "ignored";
    const HttpRequest& release = ReleaseBlobLease(httpClient, client, "lease-1", conditions);
    EXPECT_EQ(Header(release, "x-ms-lease-action"), "release");
    EXPECT_EQ(Header(release, "x-ms-lease-id"), "lease-1");
    EXPECT_EQ(Header(release, "If-Match"), "\"etag\"");

    const HttpRequest& breakRequest = BreakBlobLease(httpClient, client, conditions, 5s);
    EXPECT_EQ(Header(breakRequest, "x-ms-lease-action"), "break");
    EXPECT_EQ(Header(breakRequest, "x-ms-lease-id"), "");
    EXPECT_EQ(Header(breakRequest, "x-ms-lease-break-period"), "5");
    EXPECT_EQ(Header(breakRequest, "If-Match"), "\"etag\"");
}

TEST(LeaseTests, ContainerLeaseOperationsTargetTheContainerLeaseEndpoint)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(LeaseResponse(201, "lease-a"));
    httpClient.EnqueueResponse(LeaseResponse(200, "lease-a"));
    httpClient.EnqueueResponse(LeaseResponse(200, "lease-b"));
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    std::vector<HttpHeader> breakHeaders = MakeCanonicalSuccessHeaders();
    breakHeaders.emplace_back("x-ms-lease-time", "0");
    httpClient.EnqueueResponse(HttpResponse{202, std::move(breakHeaders), ""});
    BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

    int completed = 0;
    std::optional<std::string> acquired;
    const HttpRequest& acquire = AcquireContainerLease(httpClient, client, "lease-a", 30s, completed, acquired);
    EXPECT_EQ(acquire.GetMethod(), HttpMethod::Put);
    EXPECT_TRUE(HasQuery(acquire, "/images?"));
    EXPECT_TRUE(HasQuery(acquire, "restype=container"));
    EXPECT_TRUE(HasQuery(acquire, "comp=lease"));
    EXPECT_EQ(Header(acquire, "x-ms-lease-action"), "acquire");
    EXPECT_EQ(Header(acquire, "x-ms-lease-duration"), "30");
    EXPECT_EQ(Header(acquire, "x-ms-proposed-lease-id"), "lease-a");
    EXPECT_EQ(acquired, "lease-a");

    const HttpRequest& renew = RenewContainerLease(httpClient, client, "lease-a", completed);
    EXPECT_EQ(Header(renew, "x-ms-lease-action"), "renew");
    EXPECT_TRUE(HasQuery(renew, "restype=container"));

    const HttpRequest& change = ChangeContainerLease(httpClient, client, "lease-a", "lease-b", completed);
    EXPECT_EQ(Header(change, "x-ms-lease-action"), "change");

    const HttpRequest& release = ReleaseContainerLease(httpClient, client, "lease-b", IfUnmodifiedSince(), completed);
    EXPECT_EQ(Header(release, "x-ms-lease-action"), "release");
    EXPECT_EQ(Header(release, "If-Unmodified-Since"), "Fri, 26 Jun 2015 18:59:17 GMT");

    std::optional<int> leaseTimeSeconds;
    const HttpRequest& breakRequest = BreakContainerLease(httpClient, client, completed, leaseTimeSeconds);
    EXPECT_EQ(Header(breakRequest, "x-ms-lease-action"), "break");
    EXPECT_EQ(Header(breakRequest, "x-ms-lease-break-period"), "");
    EXPECT_EQ(leaseTimeSeconds, 0);
    EXPECT_EQ(completed, 5);
}

TEST(LeaseTests, ContainerAcquireLeaseValidatesDuration)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

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
