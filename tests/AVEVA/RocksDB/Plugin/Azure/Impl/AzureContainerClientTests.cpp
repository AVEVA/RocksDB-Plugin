// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"

#include "BlobContainerClientMock.hpp"
#include "FakeHttpClient.hpp"
#include "FakePageBlobClient.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/io_context.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::RocksDB::Plugin::Azure::Impl::AzureContainerClient;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::BlobContainerClientMock;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakePageBlobClient;

TEST(AzureContainerClientTests, GetBlobSizeReadsTheSizeFromThePageBlobHandedOutByTheContainer) {
    boost::asio::io_context context;
    FakeHttpClient httpClient;

    auto pageBlob = std::make_unique<FakePageBlobClient>(httpClient);
    pageBlob->Size = 1234;
    auto container = std::make_shared<::testing::StrictMock<BlobContainerClientMock>>(
        httpClient, MakeBlobContainerClientOptions("files"));
    EXPECT_CALL(*container, GetPageBlobClient("000001.sst")).WillOnce([&pageBlob](std::string) {
        return std::unique_ptr<AVEVA::AzureClient::PageBlobClient>(std::move(pageBlob));
    });

    AzureContainerClient adapter(std::make_shared<ClientRuntime>(context), container);

    EXPECT_EQ(1234, adapter.GetBlobSize("000001.sst"));
    EXPECT_TRUE(httpClient.NoRequestMade());
}
