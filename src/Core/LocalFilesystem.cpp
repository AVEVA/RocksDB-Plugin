// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"
#include "LocalIoUtil.hpp"

#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

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

    std::unique_ptr<MappedFileView> LocalFilesystem::MapReadOnly(
        const std::filesystem::path& path) noexcept
    {
        return LocalIoUtil::MapFileReadOnly(path);
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
            const auto stagingPath = std::filesystem::path(
                finalPath.string() + "." + std::to_string(seq) + ".tmp");

            auto stagingCleanup = boost::scope::make_scope_exit([&] {
                std::error_code ec;
                std::filesystem::remove(stagingPath, ec);
                });

            if (!LocalIoUtil::WriteAllBytesToFile(stagingPath, data, size))
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
}
