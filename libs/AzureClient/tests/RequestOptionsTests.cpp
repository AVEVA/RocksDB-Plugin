// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

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

[[nodiscard]] HttpRequestOptions MakeRequestOptions(std::chrono::milliseconds timeout, std::uint64_t bodyLimit) {
    HttpRequestOptions options;
    options.SetTimeout(timeout);
    options.SetResponseBodyLimit(bodyLimit);
    return options;
}
} // namespace

TEST(RequestOptionsTests, ClientDefaultRequestOptionsApplyWhenNoneAreGiven) {
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(1234ms, 77);
    BlobClient client{httpClient, options};

    (void)RunAwaitable(httpClient, client.DeleteAsync());

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 1234ms);
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), 77U);
    EXPECT_EQ(client.GetDefaultRequestOptions().GetTimeout(), 1234ms);
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
    options.DefaultRequestOptions = MakeRequestOptions(1234ms, 77);
    BlobClient client{httpClient, options};

    (void)RunAwaitable(httpClient, client.DeleteAsync({}, MakeRequestOptions(55ms, 66)));

    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 55ms);
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), 66U);
}

TEST(RequestOptionsTests, ContainerDefaultsApplyToContainerAndChildBlobOperations) {
    FakeHttpClient httpClient;
    auto options = MakeBlobContainerClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(777ms, 5);
    BlobContainerClient container{httpClient, options};

    (void)RunAwaitable(httpClient, container.CreateIfNotExistsAsync());
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 777ms);

    auto blob = container.GetPageBlobClient("child");
    (void)RunAwaitable(httpClient, blob.DeleteAsync());
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 777ms);
    EXPECT_EQ(blob.GetDefaultRequestOptions().GetResponseBodyLimit(), 5U);

    (void)RunAwaitable(httpClient, container.ListBlobsAsync({}, MakeRequestOptions(10ms, 6)));
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 10ms);
}
