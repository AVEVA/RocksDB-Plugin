// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"

#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <fstream>

namespace AVEVA::RocksDB::Plugin::Core
{
    using namespace boost::log::trivial;

    // -----------------------------------------------------------------------
    // LocalFilesystem
    // -----------------------------------------------------------------------
    LocalFilesystem::LocalFilesystem(
        std::shared_ptr<boost::log::sources::severity_logger_mt<
        boost::log::trivial::severity_level>> logger)
        : m_logger(std::move(logger))
    {
    }

    std::unique_ptr<File> LocalFilesystem::Open(const std::filesystem::path& path)
    {
        return std::make_unique<LocalFile>(path);
    }

    bool LocalFilesystem::DeleteFile(const std::filesystem::path& path)
    {
        std::error_code ec;
        std::filesystem::remove(path, ec);
        if (ec)
        {
            if (m_logger)
                BOOST_LOG_SEV(*m_logger, error)
                << "Failed to remove file '" << path.string() << "'. Error: " << ec.message();
            return false;
        }
        return true;
    }

    bool LocalFilesystem::DeleteDir(const std::filesystem::path& path)
    {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        if (ec)
        {
            if (m_logger)
                BOOST_LOG_SEV(*m_logger, error)
                << "Failed to remove directories '" << path.string() << "'. Error: " << ec.message();
            return false;
        }
        return true;
    }

    bool LocalFilesystem::CreateDir(const std::filesystem::path& path)
    {
        std::error_code ec;
        std::filesystem::create_directories(path, ec);
        if (ec)
        {
            if (m_logger)
                BOOST_LOG_SEV(*m_logger, error)
                << "Failed to create directories '" << path.string() << "'. Error: " << ec.message();
            return false;
        }
        return true;
    }

    std::optional<std::string> LocalFilesystem::ReadFileContents(
        const std::filesystem::path& path) noexcept
    {
        try
        {
            std::ifstream f(path, std::ios::in | std::ios::binary | std::ios::ate);
            if (!f.is_open()) return std::nullopt;
            const auto size = f.tellg();
            if (size < 0) return std::nullopt;
            std::string contents(static_cast<size_t>(size), '\0');
            f.seekg(0);
            f.read(contents.data(), size);
            if (!f) return std::nullopt;
            return contents;
        }
        catch (...)
        {
            return std::nullopt;
        }
    }

    bool LocalFilesystem::WriteFileAtomic(
        const std::filesystem::path& finalPath,
        const char* data, size_t size) noexcept
    {
        try
        {
            // Generate a unique staging path using an internal sequence counter so
            // concurrent writes to the same final path cannot clobber each other.
            const auto seq = m_seq.fetch_add(1, std::memory_order_relaxed);
            auto stagingPath = finalPath;
            stagingPath += L"." + std::to_wstring(seq) + L".tmp";

            auto stagingCleanup = boost::scope::make_scope_exit([&] {
                std::error_code ec;
                std::filesystem::remove(stagingPath, ec);
                });

            if (!WriteAllBytesToFile(stagingPath, data, size))
                return false;

            std::error_code ec;
            std::filesystem::rename(stagingPath, finalPath, ec);
            if (ec) return false;

            stagingCleanup.set_active(false);
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    bool LocalFilesystem::RenameFile(
        const std::filesystem::path& from,
        const std::filesystem::path& to) noexcept
    {
        std::error_code ec;
        std::filesystem::rename(from, to, ec);
        return !ec;
    }

    bool LocalFilesystem::WriteAllBytesToFile(
        const std::filesystem::path& path,
        const char* data, size_t size) noexcept
    {
        try
        {
            std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
            if (!f.is_open()) return false;
            f.write(data, static_cast<std::streamsize>(size));
            return f.good();
        }
        catch (...)
        {
            return false;
        }
    }
}
