// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>Whether a filesystem operation did what was asked.</summary>
enum class FilesystemResult : std::uint8_t {
    Succeeded,
    Failed,
};

class Filesystem {
  public:
    virtual ~Filesystem() = default;

    virtual std::unique_ptr<File> Open(const std::filesystem::path& path) = 0;
    virtual FilesystemResult DeleteFile(const std::filesystem::path& path) = 0;
    virtual FilesystemResult DeleteDir(const std::filesystem::path& path) = 0;
    virtual FilesystemResult CreateDir(const std::filesystem::path& path) = 0;

    /// <summary>
    /// Reads the entire contents of the file at <paramref name="path"/> into a string.
    /// Returns <c>std::nullopt</c> if the file does not exist, cannot be opened, or a
    /// read error occurs.
    /// </summary>
    virtual std::optional<std::string> ReadFileContents(const std::filesystem::path& path) noexcept = 0;

    /// <summary>
    /// Atomically writes <paramref name="contents"/> to <paramref name="finalPath"/> using an internal staging file
    /// and rename.
    /// The operation is atomic with respect to concurrent readers — they observe
    /// either the prior version of the file or the complete new contents — but does
    /// not guarantee durability across power loss.
    /// </summary>
    virtual FilesystemResult WriteFileAtomic(const std::filesystem::path& finalPath,
                                             std::span<const char> contents) noexcept = 0;

    /// <summary>
    /// Renames <paramref name="from"/> to <paramref name="to"/>.
    /// </summary>
    virtual FilesystemResult RenameFile(const std::filesystem::path& from,
                                        const std::filesystem::path& to) noexcept = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Core
