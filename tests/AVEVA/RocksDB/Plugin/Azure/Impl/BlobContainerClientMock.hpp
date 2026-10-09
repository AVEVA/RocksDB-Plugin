// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/BlobContainerClient.hpp>

#include <gmock/gmock.h>

#include <memory>
#include <string>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// Mocks the container's blob-client factories so a test can hand out mock blob clients (for example a
// PageBlobClientMock). Everything else runs the real implementation against the HTTP client given at construction.
class BlobContainerClientMock : public AzureClient::BlobContainerClient {
  public:
    using AzureClient::BlobContainerClient::BlobContainerClient;

    MOCK_METHOD(std::unique_ptr<AzureClient::BlobClient>, GetBlobClient, (std::string blobName), (const, override));
    MOCK_METHOD(std::unique_ptr<AzureClient::BlockBlobClient>, GetBlockBlobClient, (std::string blobName),
                (const, override));
    MOCK_METHOD(std::unique_ptr<AzureClient::PageBlobClient>, GetPageBlobClient, (std::string blobName),
                (const, override));
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
