// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"
#include <boost/log/trivial.hpp>
namespace AVEVA::RocksDB::Plugin::Core
{
    using namespace boost::log::trivial;
    LocalFilesystem::LocalFilesystem(std::shared_ptr<boost::log::sources::logger_mt> logger)
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
            BOOST_LOG_SEV(*m_logger, error) << "Failed to remove file '" << path.string() << "'. Error: " << ec.message();
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
            BOOST_LOG_SEV(*m_logger, error) << "Failed to remove directories '" << path.string() << "'. Error: " << ec.message();
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
            BOOST_LOG_SEV(*m_logger, error) << "Failed to create directories '" << path.string() << "'. Error: " << ec.message();
            return false;
        }

        return true;
    }
}
