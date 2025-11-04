#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include <string>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Core
{
    class ContainerClient
    {
    public:
        ContainerClient() = default;
        virtual ~ContainerClient() = default;
        ContainerClient(const ContainerClient&) = default;
        ContainerClient& operator=(const ContainerClient&) = default;
        ContainerClient(ContainerClient&&) noexcept = default;
        ContainerClient& operator=(ContainerClient&&) noexcept = default;

        virtual std::unique_ptr<BlobClient> GetBlobClient(const std::string& path) = 0;
    };
}
