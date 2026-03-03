// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"

#include <boost/log/trivial.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Core
{
    class LocalFilesystem final : public Filesystem
    {
    public:
        /// <summary>Constructs a <c>LocalFilesystem</c> without logging.
        /// Suitable for tests and contexts that do not require error diagnostics.</summary>
        LocalFilesystem() = default;

        /// <summary>Constructs a <c>LocalFilesystem</c> that logs errors via
        /// <paramref name="logger"/>.</summary>
        explicit LocalFilesystem(
            std::shared_ptr<boost::log::sources::severity_logger_mt<
                boost::log::trivial::severity_level>> logger);

        std::unique_ptr<File> Open(const std::filesystem::path& path) override;
        bool DeleteFile(const std::filesystem::path& path) override;
        bool DeleteDir(const std::filesystem::path& path) override;
        bool CreateDir(const std::filesystem::path& path) override;

        /// <summary>Memory-maps <paramref name="path"/> read-only using
        /// <c>boost::interprocess</c>.  Returns <c>nullptr</c> on failure.</summary>
        std::unique_ptr<MappedFileView> MapReadOnly(
            const std::filesystem::path& path) noexcept override;

        /// <summary>Writes <paramref name="size"/> bytes to <paramref name="finalPath"/>
        /// atomically via an internal staging file.  Uses unbuffered direct I/O so the
        /// data goes to the OS in a single write call with no extra copy.</summary>
        bool WriteFileAtomic(const std::filesystem::path& finalPath,
                             const char* data, size_t size) noexcept override;

        /// <summary>Renames <paramref name="from"/> to <paramref name="to"/>.</summary>
        bool RenameFile(const std::filesystem::path& from,
                        const std::filesystem::path& to) noexcept override;

    private:
        std::shared_ptr<boost::log::sources::severity_logger_mt<
            boost::log::trivial::severity_level>> m_logger;

        /// <summary>Monotonically increasing counter for unique staging file names.</summary>
        std::atomic<uint64_t> m_seq{0};
    };
}
