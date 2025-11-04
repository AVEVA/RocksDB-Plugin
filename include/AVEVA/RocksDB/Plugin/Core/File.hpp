#pragma once
#include <cstdint>
namespace AVEVA::RocksDB::Plugin::Core
{
    class File
    {
    public:
        virtual ~File() = default;

        virtual uint64_t Read(char* buffer, uint64_t offset, uint64_t length) = 0;
    };
}
