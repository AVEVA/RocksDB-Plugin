// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"

#include <filesystem>
#include <memory>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>
    /// Internal helpers for OS-native file I/O.  All platform-specific headers
    /// (windows.h, boost::interprocess, POSIX) are confined to LocalIoUtil.cpp
    /// so they never pollute translation units that define Filesystem virtual methods.
    /// </summary>
    struct LocalIoUtil
    {
        /// <summary>
        /// Writes all <paramref name="size"/> bytes from <paramref name="data"/> to
        /// <paramref name="path"/>, creating or truncating the file.  Uses OS-native
        /// unbuffered I/O.  Returns <c>true</c> on success.
        /// </summary>
        static bool WriteAllBytesToFile(const std::filesystem::path& path,
                                        const char* data, size_t size) noexcept;

        /// <summary>
        /// Returns a read-only memory-mapped view of <paramref name="path"/>, or
        /// <c>nullptr</c> on failure.
        /// </summary>
        static std::unique_ptr<MappedFileView> MapFileReadOnly(
            const std::filesystem::path& path) noexcept;
    };
}
