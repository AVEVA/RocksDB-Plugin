#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Core
{
    class FileMock : public File
    {
    public:
        FileMock();
        virtual ~FileMock();

        MOCK_METHOD(uint64_t, Read, (char* buffer, uint64_t offset, uint64_t length), (override));
    };
}
