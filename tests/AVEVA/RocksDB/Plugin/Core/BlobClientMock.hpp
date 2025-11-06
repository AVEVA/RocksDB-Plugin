#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include <gmock/gmock.h>
namespace AVEVA::RocksDB::Plugin::Core
{
    class BlobClientMock : public BlobClient
    {
    public:
        BlobClientMock();
        virtual ~BlobClientMock();

        MOCK_METHOD(uint64_t, GetSize, (), (override));
        MOCK_METHOD(void, DownloadTo, (const std::string& path, uint64_t offset, uint64_t length), (override));
    };
}
