// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"

#include "BlobContainerClientMock.hpp"
#include "FakeHttpClient.hpp"
#include "PageBlobClientMock.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/io_context.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::RocksDB::Plugin::Azure::Impl::AzureContainerClient;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::BlobContainerClientMock;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::PageBlobClientMock;

TEST(AzureContainerClientTests, GetBlobClientWrapsThePageBlobClientHandedOutByTheContainer) {
    boost::asio::io_context context;
    FakeHttpClient httpClient;

    auto pageBlob = std::make_unique<::testing::StrictMock<PageBlobClientMock>>(
        httpClient, MakeBlobClientOptions("files", "000001.sst"));
    auto* const pageBlobMock = pageBlob.get();
    auto container = std::make_shared<::testing::StrictMock<BlobContainerClientMock>>(
        httpClient, MakeBlobContainerClientOptions("files"));
    EXPECT_CALL(*container, GetPageBlobClient("000001.sst")).WillOnce([&pageBlob](std::string) {
        return std::unique_ptr<AVEVA::AzureClient::PageBlobClient>(std::move(pageBlob));
    });
    EXPECT_CALL(*pageBlobMock, GetPropertiesAsyncImpl).WillOnce([](const auto&, auto completion, auto) {
        AVEVA::AzureClient::Models::BlobProperties properties;
        properties.ETag = "\"from-mock\"";
        completion(AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::BlobProperties>(
            std::move(properties), AVEVA::HttpResponse{200, {}, {}}));
    });

    AzureContainerClient adapter(std::make_shared<ClientRuntime>(context), container);
    const auto blob = adapter.GetBlobClient("000001.sst");

    EXPECT_EQ("\"from-mock\"", blob->GetEtag());
    EXPECT_TRUE(httpClient.NoRequestMade());
}
