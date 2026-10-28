// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

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

namespace {
using AVEVA::HttpHeader;
using AVEVA::HttpRequest;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::AcquireLeaseOptions;
using AVEVA::AzureClient::ReleaseLeaseOptions;
using AVEVA::AzureClient::RenewLeaseOptions;
using AVEVA::AzureClient::Models::BlobRequestConditions;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::DefaultETag;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::RunAwaitable;
using namespace std::chrono_literals;

[[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name) {
    return FakeHttpClient::FindHeaderValue(request, name);
}

[[nodiscard]] HttpResponse LeaseResponse(unsigned int status, std::string leaseId) {
    std::vector<HttpHeader> headers = MakeCanonicalSuccessHeaders();
    headers.emplace_back("x-ms-lease-id", std::move(leaseId));
    return HttpResponse{status, std::move(headers), ""};
}

[[nodiscard]] BlobRequestConditions IfUnmodifiedSince() {
    BlobRequestConditions conditions;
    conditions.IfUnmodifiedSince = std::chrono::sys_days{std::chrono::year{2015} / 6 / 26} + 18h + 59min + 17s;
    return conditions;
}

const HttpRequest& RenewBlobLease(FakeHttpClient& httpClient, BlobClient& client, std::string leaseId,
                                  BlobRequestConditions conditions, std::optional<std::string>& renewed) {
    auto result = RunAwaitable(httpClient, client.RenewLeaseAsync(RenewLeaseOptions{
                                               .LeaseId = std::move(leaseId), .Conditions = std::move(conditions)}));
    EXPECT_TRUE(result.has_value());
    if (result) {
        EXPECT_EQ(result->Value().ETag, DefaultETag);
        renewed = result->Value().LeaseId;
    }
    return httpClient.LastRequest();
}

const HttpRequest& ReleaseBlobLease(FakeHttpClient& httpClient, BlobClient& client, std::string leaseId,
                                    BlobRequestConditions conditions) {
    (void)RunAwaitable(httpClient, client.ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = std::move(leaseId),
                                                                                .Conditions = std::move(conditions)}));
    return httpClient.LastRequest();
}
} // namespace

TEST(LeaseTests, BlobRenewReleaseRejectEmptyLeaseIds) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    const auto renew = RunAwaitable(httpClient, client.RenewLeaseAsync(RenewLeaseOptions{}));
    const auto release = RunAwaitable(httpClient, client.ReleaseLeaseAsync(ReleaseLeaseOptions{}));
    ASSERT_FALSE(renew.has_value());
    ASSERT_FALSE(release.has_value());
    EXPECT_EQ(renew.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(release.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_TRUE(httpClient.NoRequestMade());
}

TEST(LeaseTests, BlobRenewAndReleaseSendLeaseActionAndConditions) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(LeaseResponse(200, "lease-a"));
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    BlobClient client{httpClient, MakeBlobClientOptions()};

    std::optional<std::string> renewed;
    const HttpRequest renew = RenewBlobLease(httpClient, client, "lease-a", {}, renewed);
    EXPECT_EQ(Header(renew, "x-ms-lease-action"), "renew");
    EXPECT_NE(renew.GetUrl().find("comp=lease"), std::string::npos);
    EXPECT_EQ(renewed, "lease-a");

    const HttpRequest release = ReleaseBlobLease(httpClient, client, "lease-a", IfUnmodifiedSince());
    EXPECT_EQ(Header(release, "x-ms-lease-action"), "release");
    EXPECT_EQ(Header(release, "If-Unmodified-Since"), "Fri, 26 Jun 2015 18:59:17 GMT");
}

TEST(LeaseTests, BlobAcquireLeaseValidatesDuration) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    const auto result = RunAwaitable(httpClient, client.AcquireLeaseAsync(AcquireLeaseOptions{
                                                     .Conditions = {}, .ProposedLeaseId = {}, .Duration = 5s}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(LeaseTests, BlobAcquireLeaseRetryReusesGeneratedProposedLeaseId) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(LeaseResponse(201, "lease-a"));

    auto options = MakeBlobClientOptions();
    options.Retry.MaxRetries = 1;
    options.Retry.InitialDelay = 0ms;
    BlobClient client{httpClient, options};

    const auto result = RunAwaitable(httpClient, client.AcquireLeaseAsync());
    ASSERT_TRUE(result.has_value());

    ASSERT_EQ(httpClient.RequestCount(), 2U);
    const std::string firstLeaseId = Header(httpClient.RequestAt(0).Request, "x-ms-proposed-lease-id");
    const std::string secondLeaseId = Header(httpClient.RequestAt(1).Request, "x-ms-proposed-lease-id");
    EXPECT_FALSE(firstLeaseId.empty());
    EXPECT_EQ(firstLeaseId, secondLeaseId);
    EXPECT_EQ(result->Value().LeaseId, "lease-a");
}
