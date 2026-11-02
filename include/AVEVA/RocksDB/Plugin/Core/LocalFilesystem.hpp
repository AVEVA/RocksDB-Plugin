// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"

#include <boost/log/trivial.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <span>
namespace AVEVA::RocksDB::Plugin::Core {
class LocalFilesystem final : public Filesystem {
  public:
    /// <summary>Constructs a <c>LocalFilesystem</c> without logging.
    /// Suitable for tests and contexts that do not require error diagnostics.</summary>
    LocalFilesystem() = default;

    /// <summary>Constructs a <c>LocalFilesystem</c> that logs errors via
    /// <paramref name="logger"/>.</summary>
    explicit LocalFilesystem(
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);

    std::unique_ptr<File> Open(const std::filesystem::path& path) override;
    FilesystemResult DeleteFile(const std::filesystem::path& path) override;
    FilesystemResult DeleteDir(const std::filesystem::path& path) override;
    FilesystemResult CreateDir(const std::filesystem::path& path) override;

    /// <summary>Reads the entire contents of <paramref name="path"/> into a string using
    /// standard buffered I/O.  Returns <c>std::nullopt</c> on any failure.</summary>
    std::optional<std::string> ReadFileContents(const std::filesystem::path& path) noexcept override;

    /// <summary>Writes <paramref name="contents"/> to <paramref name="finalPath"/>
    /// atomically via a staging file and rename.  Uses standard buffered OS writes;
    /// observers see either the prior version or the complete new contents.</summary>
    FilesystemResult WriteFileAtomic(const std::filesystem::path& finalPath,
                                     std::span<const char> contents) noexcept override;

    /// <summary>Renames <paramref name="from"/> to <paramref name="to"/>.</summary>
    FilesystemResult RenameFile(const std::filesystem::path& from, const std::filesystem::path& to) noexcept override;

  private:
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    /// <summary>Monotonically increasing counter for unique staging file names.</summary>
    std::atomic<uint64_t> m_seq{0};

    /// <summary>Writes all bytes to <paramref name="path"/> using standard buffered I/O.</summary>
    static FilesystemResult WriteAllBytesToFile(const std::filesystem::path& path,
                                                std::span<const char> contents) noexcept;
};
} // namespace AVEVA::RocksDB::Plugin::Core
