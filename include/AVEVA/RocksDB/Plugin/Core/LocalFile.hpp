// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

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
        virtual int64_t Read(char* buffer, int64_t offset, int64_t length) override;
    };
}
