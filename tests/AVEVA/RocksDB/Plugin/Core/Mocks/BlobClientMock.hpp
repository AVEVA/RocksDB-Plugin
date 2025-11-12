// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include <gmock/gmock.h>
namespace AVEVA::RocksDB::Plugin::Core::Mocks
{
    class BlobClientMock : public BlobClient
    {
    public:
        BlobClientMock();
        virtual ~BlobClientMock();

        MOCK_METHOD(uint64_t, GetSize, (), (override));
        MOCK_METHOD(void, SetSize, (int64_t size), (override));
        MOCK_METHOD(uint64_t, GetCapacity, (), (override));
        MOCK_METHOD(void, SetCapacity, (int64_t capacity), (override));
        MOCK_METHOD(void, DownloadTo, (const std::string& path, uint64_t offset, uint64_t length), (override));
        MOCK_METHOD(int64_t, DownloadTo, (std::span<char> buffer, int64_t blobOffset, int64_t length), (override));
        MOCK_METHOD(void, UploadPages, (const std::span<char> buffer, int64_t blobOffset), (override));
    };
}
