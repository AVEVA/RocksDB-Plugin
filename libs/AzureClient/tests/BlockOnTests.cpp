// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"

#include <AVEVA/AzureClient/BlockOn.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/post.hpp>
#include <gtest/gtest.h>

#include <future>
#include <stdexcept>

namespace {
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::block_on;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::PageBlobClient;
} // namespace

TEST(BlockOnTokenTests, ReturnsTheResultDirectly) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    httpClient.StartBackgroundPump();
    PageBlobClient client{httpClient, MakeBlobClientOptions("disks", "disk.vhd")};

    auto result = client.CreateAsync(512, block_on);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(BlockOnTokenTests, ReturnsServiceFailureAsExpectedError) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""};
    httpClient.StartBackgroundPump();
    PageBlobClient client{httpClient, MakeBlobClientOptions("disks", "disk.vhd")};

    auto result = client.GetPropertiesAsync(block_on);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobNotFound);
}

TEST(BlockOnTokenTests, ThrowsWithoutSendingWhenCalledOnTheClientsExecutor) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, MakeBlobClientOptions("disks", "disk.vhd")};

    std::promise<bool> threw;
    auto future = threw.get_future();
    boost::asio::post(httpClient.get_executor(), [&] {
        try {
            (void)client.GetPropertiesAsync(block_on);
            threw.set_value(false);
        } catch (const std::logic_error&) {
            threw.set_value(true);
        }
    });
    httpClient.Poll();

    ASSERT_TRUE(AVEVA::AzureClient::Tests::ExpectReady(future));
    EXPECT_TRUE(future.get());
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}
