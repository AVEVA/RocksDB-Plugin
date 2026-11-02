// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Core::Mocks {
// Tests set expectations on the strongly typed methods below; the overrides forward to them and ignore the stop token.
class ContainerClientMock : public ContainerClient {
  public:
    ContainerClientMock();
    virtual ~ContainerClientMock();

    MOCK_METHOD(ByteCount, GetBlobSize, (const std::string& path));
    MOCK_METHOD(void, DownloadBlobTo,
                (const std::string& path, const std::filesystem::path& destinationPath, ByteOffset offset,
                 ByteCount length));

    ByteCount GetBlobSize(const BlobName& name, std::stop_token) override { return GetBlobSize(name.Value()); }

    void DownloadBlobTo(const BlobName& name, const std::filesystem::path& destinationPath, ByteOffset offset,
                        ByteCount length, std::stop_token) override {
        DownloadBlobTo(name.Value(), destinationPath, offset, length);
    }
};
} // namespace AVEVA::RocksDB::Plugin::Core::Mocks
