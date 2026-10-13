// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"

#include "BlobContainerClientMock.hpp"
#include "FakeBlobEnvironment.hpp"
#include "FakeBlobServer.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/io_context.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::RocksDB::Plugin::Azure::Impl::BasicAzureContainerClient;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::BlobContainerClientMock;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobServer;

TEST(AzureContainerClientTests, GetBlobSizeReadsTheSizeFromThePageBlobHandedOutByTheContainer) {
    boost::asio::io_context context;
    FakeBlobEnvironment env;
    env.Blob->Size = 1234;

    auto pageBlob = std::make_unique<FakeBlobServer::BlobClientType>(
        env.Http, AVEVA::AzureClient::Tests::MakeBlobClientOptions("files", "test.sst"));
    auto container = std::make_shared<::testing::StrictMock<BlobContainerClientMock>>();
    EXPECT_CALL(*container, GetPageBlobClient("000001.sst")).WillOnce([&pageBlob](std::string) {
        return std::move(pageBlob);
    });

    BasicAzureContainerClient<BlobContainerClientMock> adapter(std::make_shared<ClientRuntime>(context), container);

    EXPECT_EQ(1234, adapter.GetBlobSize("000001.sst"));
    EXPECT_EQ(1, env.Blob->PropertiesRequests);
}
