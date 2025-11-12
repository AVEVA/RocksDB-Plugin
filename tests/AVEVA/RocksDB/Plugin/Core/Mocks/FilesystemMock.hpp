// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Core::Mocks
{
    class FilesystemMock : public Filesystem
    {
    public:
        FilesystemMock();
        virtual ~FilesystemMock();

        MOCK_METHOD(std::unique_ptr<File>, Open, (const std::filesystem::path& path), (override));
        MOCK_METHOD(bool, DeleteFile, (const std::filesystem::path& path), (override));
        MOCK_METHOD(bool, DeleteDir, (const std::filesystem::path& path), (override));
        MOCK_METHOD(bool, CreateDir, (const std::filesystem::path& path), (override));
    };
}
