// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"

#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <chrono>
#include <fstream>
#include <thread>

namespace AVEVA::RocksDB::Plugin::Core {
using namespace boost::log::trivial;

// -----------------------------------------------------------------------
// LocalFilesystem
// -----------------------------------------------------------------------
LocalFilesystem::LocalFilesystem(
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_logger(std::move(logger)) {}

std::unique_ptr<File> LocalFilesystem::Open(const std::filesystem::path& path) {
    return std::make_unique<LocalFile>(path);
}

FilesystemResult LocalFilesystem::DeleteFile(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error)
                << "Failed to remove file '" << path.string() << "'. Error: " << ec.message();
        return FilesystemResult::Failed;
    }
    return FilesystemResult::Succeeded;
}

FilesystemResult LocalFilesystem::DeleteDir(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error)
                << "Failed to remove directories '" << path.string() << "'. Error: " << ec.message();
        return FilesystemResult::Failed;
    }
    return FilesystemResult::Succeeded;
}

FilesystemResult LocalFilesystem::CreateDir(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error)
                << "Failed to create directories '" << path.string() << "'. Error: " << ec.message();
        return FilesystemResult::Failed;
    }
    return FilesystemResult::Succeeded;
}

std::optional<std::string> LocalFilesystem::ReadFileContents(const std::filesystem::path& path) noexcept {
    try {
        std::ifstream f(path, std::ios::in | std::ios::binary | std::ios::ate);
        if (!f.is_open())
            return std::nullopt;
        const auto size = f.tellg();
        if (size < 0)
            return std::nullopt;
        std::string contents;
        f.seekg(0);
        contents.resize_and_overwrite(static_cast<size_t>(size), [&](char* p, size_t n) {
            f.read(p, static_cast<std::streamsize>(n));
            return f ? n : size_t{0};
        });
        if (contents.size() != static_cast<size_t>(size))
            return std::nullopt;
        return contents;
    } catch (...) {
        return std::nullopt;
    }
}

FilesystemResult LocalFilesystem::WriteFileAtomic(const std::filesystem::path& finalPath,
                                                  const std::span<const char> contents) noexcept {
    try {
        // Generate a unique staging path using an internal sequence counter so
        // concurrent writes to the same final path cannot clobber each other.
        const auto seq = m_seq.fetch_add(1, std::memory_order_relaxed);
        auto stagingPath = finalPath;
        stagingPath += L"." + std::to_wstring(seq) + L".tmp";

        auto stagingCleanup = boost::scope::make_scope_exit([&] {
            std::error_code ec;
            std::filesystem::remove(stagingPath, ec);
        });

        if (WriteAllBytesToFile(stagingPath, contents) == FilesystemResult::Failed)
            return FilesystemResult::Failed;

        // Replacing an existing file fails on Windows with a sharing violation or access denied while another process
        // (a virus scanner or the indexer looking at the file that was just written) has it open. That lasts a few
        // milliseconds, so retry briefly instead of failing the write.
        constexpr int kMaxRenameAttempts = 8;
        std::chrono::milliseconds backoff{1};
        std::error_code ec;
        for (int attempt = 1;; ++attempt) {
            std::filesystem::rename(stagingPath, finalPath, ec);
            if (!ec)
                break;
            if (attempt == kMaxRenameAttempts) {
                if (m_logger)
                    BOOST_LOG_SEV(*m_logger, warning) << "Failed to move '" << stagingPath.string() << "' to '"
                                                      << finalPath.string() << "'. Error: " << ec.message();
                return FilesystemResult::Failed;
            }
            std::this_thread::sleep_for(backoff);
            backoff *= 2;
        }

        stagingCleanup.set_active(false);
        return FilesystemResult::Succeeded;
    } catch (...) {
        return FilesystemResult::Failed;
    }
}

FilesystemResult LocalFilesystem::RenameFile(const std::filesystem::path& from,
                                             const std::filesystem::path& to) noexcept {
    std::error_code ec;
    std::filesystem::rename(from, to, ec);
    return ec ? FilesystemResult::Failed : FilesystemResult::Succeeded;
}

FilesystemResult LocalFilesystem::WriteAllBytesToFile(const std::filesystem::path& path,
                                                      const std::span<const char> contents) noexcept {
    try {
        std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!f.is_open())
            return FilesystemResult::Failed;
        f.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        return f.good() ? FilesystemResult::Succeeded : FilesystemResult::Failed;
    } catch (...) {
        return FilesystemResult::Failed;
    }
}
} // namespace AVEVA::RocksDB::Plugin::Core
