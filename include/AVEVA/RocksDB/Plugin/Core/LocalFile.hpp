#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <fstream>
#include <filesystem>
namespace AVEVA::RocksDB::Plugin::Core
{
    class LocalFile final : public File
    {
        std::fstream m_file;
    public:
        explicit LocalFile(const std::filesystem::path& path);
        virtual uint64_t Read(char* buffer, uint64_t offset, uint64_t length) override;
    };
}
