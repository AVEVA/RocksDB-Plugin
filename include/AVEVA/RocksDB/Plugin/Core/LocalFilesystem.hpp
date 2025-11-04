#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
namespace AVEVA::RocksDB::Plugin::Core
{
    class LocalFilesystem final : public Filesystem
    {
    public:
        virtual std::unique_ptr<File> Open(const std::filesystem::path& path) override;
        virtual void DeleteDir(const std::filesystem::path& path) override;
        virtual void CreateDir(const std::filesystem::path& path) override;
    };
}
