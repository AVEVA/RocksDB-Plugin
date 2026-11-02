// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include <gmock/gmock.h>
#include <optional>
#include <string>

namespace AVEVA::RocksDB::Plugin::Core::Mocks {
class FilesystemMock : public Filesystem {
  public:
    FilesystemMock();
    virtual ~FilesystemMock();

    MOCK_METHOD(std::unique_ptr<File>, Open, (const std::filesystem::path& path), (override));
    MOCK_METHOD(FilesystemResult, DeleteFile, (const std::filesystem::path& path), (override));
    MOCK_METHOD(FilesystemResult, DeleteDir, (const std::filesystem::path& path), (override));
    MOCK_METHOD(FilesystemResult, CreateDir, (const std::filesystem::path& path), (override));
    MOCK_METHOD(std::optional<std::string>, ReadFileContents, (const std::filesystem::path& path),
                (noexcept, override));
    MOCK_METHOD(FilesystemResult, WriteFileAtomic,
                (const std::filesystem::path& finalPath, std::span<const char> contents), (noexcept, override));
    MOCK_METHOD(FilesystemResult, RenameFile, (const RenameSource& from, const RenameDestination& to),
                (noexcept, override));
};
} // namespace AVEVA::RocksDB::Plugin::Core::Mocks
