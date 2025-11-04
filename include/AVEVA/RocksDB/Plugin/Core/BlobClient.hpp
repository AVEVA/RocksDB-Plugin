#pragma once
#include <cstdint>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core
{
    class BlobClient
    {
    public:
        BlobClient() = default;
        virtual ~BlobClient() = default;
        BlobClient(const BlobClient&) = default;
        BlobClient& operator=(const BlobClient&) = default;
        BlobClient(BlobClient&&) noexcept = default;
        BlobClient& operator=(BlobClient&&) noexcept = default;

        virtual uint64_t GetSize() = 0;
        virtual void DownloadTo(const std::string& path, uint64_t offset, uint64_t length) = 0;
    };
}
