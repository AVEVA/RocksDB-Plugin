// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

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

        [[nodiscard]] static bool IsManifestFile(std::string_view pathname);
        [[nodiscard]] static bool IsIdentityFile(std::string_view pathname);
        [[nodiscard]] static bool IsLogFile(const FileClass fileType);
        [[nodiscard]] static FileClass GetFileType(std::string_view pathname);
    };
}
