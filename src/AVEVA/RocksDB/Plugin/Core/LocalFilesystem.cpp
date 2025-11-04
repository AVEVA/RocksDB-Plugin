#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"
namespace AVEVA::RocksDB::Plugin::Core
{
    std::unique_ptr<File> LocalFilesystem::Open(const std::filesystem::path& path)
    {
        return std::make_unique<LocalFile>(path);
    }

    void LocalFilesystem::DeleteDir(const std::filesystem::path& path)
    {
        std::filesystem::remove_all(path);
    }

    void LocalFilesystem::CreateDir(const std::filesystem::path& path)
    {
        std::filesystem::create_directories(path);
    }
}
