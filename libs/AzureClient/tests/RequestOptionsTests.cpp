// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <utility>

namespace {
using AVEVA::HttpRequestOptions;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::BlobContainerClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::RunAwaitable;
using namespace std::chrono_literals;

// Distinct timeouts and body limits per layer, so each assertion shows which layer's options reached the transport.
constexpr std::chrono::milliseconds ClientTimeout = 1234ms;
constexpr std::uint64_t ClientBodyLimit = 77;
constexpr std::chrono::milliseconds CallTimeout = 55ms;
constexpr std::uint64_t CallBodyLimit = 66;
constexpr std::chrono::milliseconds ContainerTimeout = 777ms;
constexpr std::uint64_t ContainerBodyLimit = 5;
constexpr std::chrono::milliseconds ListTimeout = 10ms;
constexpr std::uint64_t ListBodyLimit = 6;

[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions MakeRequestOptions(std::chrono::milliseconds timeout,
                                                                         std::uint64_t bodyLimit) {
    HttpRequestOptions options;
    options.SetTimeout(AVEVA::Timeout{timeout});
    options.SetResponseBodyLimit(AVEVA::ByteCount{bodyLimit});
    return AVEVA::AzureClient::AzureRequestOptions{options};
}
} // namespace

TEST(RequestOptionsTests, ClientDefaultRequestOptionsApplyWhenNoneAreGiven) {
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(ClientTimeout, ClientBodyLimit);
    BlobClient client{httpClient, options};

    (void)RunAwaitable(httpClient, client.DeleteAsync());

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout().Value(), ClientTimeout);
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), AVEVA::ByteCount{ClientBodyLimit});
    EXPECT_EQ(client.GetDefaultRequestOptions().GetTimeout().Value(), ClientTimeout);
}

TEST(RequestOptionsTests, WithoutClientDefaultsTheHttpRequestOptionsDefaultsApply) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    (void)RunAwaitable(httpClient, client.DeleteAsync());

    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), HttpRequestOptions{}.GetTimeout());
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), HttpRequestOptions{}.GetResponseBodyLimit());
}

TEST(RequestOptionsTests, ExplicitTrailingRequestOptionsReplaceTheClientDefault) {
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(ClientTimeout, ClientBodyLimit);
    BlobClient client{httpClient, options};

    (void)RunAwaitable(httpClient, client.DeleteAsync({}, MakeRequestOptions(CallTimeout, CallBodyLimit)));

    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout().Value(), CallTimeout);
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), AVEVA::ByteCount{CallBodyLimit});
}

TEST(RequestOptionsTests, ContainerDefaultsApplyToContainerAndChildBlobOperations) {
    FakeHttpClient httpClient;
    auto options = MakeBlobContainerClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(ContainerTimeout, ContainerBodyLimit);
    BlobContainerClient container{httpClient, options};

    (void)RunAwaitable(httpClient, container.CreateIfNotExistsAsync());
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout().Value(), ContainerTimeout);

    auto blob = container.GetPageBlobClient("child");
    (void)RunAwaitable(httpClient, blob.DeleteAsync());
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout().Value(), ContainerTimeout);
    EXPECT_EQ(blob.GetDefaultRequestOptions().GetResponseBodyLimit(), AVEVA::ByteCount{ContainerBodyLimit});

    (void)RunAwaitable(httpClient, container.ListBlobsAsync({}, MakeRequestOptions(ListTimeout, ListBodyLimit)));
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout().Value(), ListTimeout);
}
