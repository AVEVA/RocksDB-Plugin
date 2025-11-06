#pragma once
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Core
{
    class ContainerClientMock : public ContainerClient
    {
    public:
        ContainerClientMock();
        virtual ~ContainerClientMock();

        MOCK_METHOD(std::unique_ptr<BlobClient>, GetBlobClient, (const std::string& path), (override));
    };
}
