// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Core::Mocks {
class ContainerClientMock : public ContainerClient {
  public:
    ContainerClientMock();
    virtual ~ContainerClientMock();

    MOCK_METHOD(int64_t, GetBlobSize, (const std::string& path), (override));
    MOCK_METHOD(void, DownloadBlobTo,
                (const std::string& path, const std::string& destinationPath, int64_t offset, int64_t length),
                (override));
};
} // namespace AVEVA::RocksDB::Plugin::Core::Mocks
