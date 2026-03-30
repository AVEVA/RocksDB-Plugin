// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <filesystem>
#include <memory>
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

        /// <summary>
        /// Returns a read-only memory-mapped view of the file at <paramref name="path"/>.
        /// Returns <c>nullptr</c> if the file does not exist or cannot be mapped.
        /// </summary>
        virtual std::unique_ptr<MappedFileView> MapReadOnly(const std::filesystem::path& path) noexcept = 0;

        /// <summary>
        /// Atomically writes <paramref name="size"/> bytes from <paramref name="data"/> to
        /// <paramref name="finalPath"/> using an internal staging file.  The write is
        /// crash-safe: either the complete data appears at <paramref name="finalPath"/> or
        /// the file is unchanged.  Returns <c>true</c> on success.
        /// </summary>
        virtual bool WriteFileAtomic(const std::filesystem::path& finalPath,
                                     const char* data, size_t size) noexcept = 0;

        /// <summary>
        /// Renames <paramref name="from"/> to <paramref name="to"/>.
        /// Returns <c>true</c> on success.
        /// </summary>
        virtual bool RenameFile(const std::filesystem::path& from,
                                const std::filesystem::path& to) noexcept = 0;
    };
}
