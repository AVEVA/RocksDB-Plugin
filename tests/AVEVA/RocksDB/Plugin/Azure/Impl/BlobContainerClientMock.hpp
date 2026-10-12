// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "FakePageBlobClient.hpp"

#include <gmock/gmock.h>

#include <memory>
#include <string>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// A standalone stand-in for the container client (the TContainer of BasicAzureContainerClient) whose page-blob factory
// is mocked, so a test can hand out FakePageBlobClient instances.
class BlobContainerClientMock {
  public:
    MOCK_METHOD(std::unique_ptr<FakePageBlobClient>, GetPageBlobClient, (std::string blobName), (const));
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests