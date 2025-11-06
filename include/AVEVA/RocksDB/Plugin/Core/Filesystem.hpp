#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <filesystem>
namespace AVEVA::RocksDB::Plugin::Core
{
    class Filesystem
    {
    public:
        virtual ~Filesystem() = default;

        virtual std::unique_ptr<File> Open(const std::filesystem::path& path) = 0;
        virtual bool DeleteFile(const std::filesystem::path& path) = 0;
        virtual bool DeleteDir(const std::filesystem::path& path) = 0;
        virtual bool CreateDir(const std::filesystem::path& path) = 0;
    };
}
