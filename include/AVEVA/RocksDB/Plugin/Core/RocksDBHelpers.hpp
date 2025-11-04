#pragma once
#include <string_view>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core
{
    struct RocksDBHelpers
    {
        struct FileType
        {
            static constexpr std::string_view sst = ".sst";
            static constexpr std::string_view ldb = ".ldb";
            static constexpr std::string_view log = ".log";
        };

        enum class FileClass
        {
            Directory = 0,
            SST = 1,
            WAL = 2, // log file
            Manifest = 3, // also log file
            Identity = 4,
        };

        [[nodiscard]] static bool IsManifestFile(const std::string& pathname);
        [[nodiscard]] static bool IsIdentityFile(const std::string& pathname);
        [[nodiscard]] static bool IsLogFile(const FileClass fileType);
        [[nodiscard]] static FileClass GetFileType(const std::string& pathname);
    };
}
