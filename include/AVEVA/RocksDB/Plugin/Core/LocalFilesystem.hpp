// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"

#include <boost/log/trivial.hpp>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Core
{
    class LocalFilesystem final : public Filesystem
    {
    private:
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    public:
        explicit LocalFilesystem(std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
        virtual std::unique_ptr<File> Open(const std::filesystem::path& path) override;
        virtual bool DeleteFile(const std::filesystem::path& path) override;
        virtual bool DeleteDir(const std::filesystem::path& path) override;
        virtual bool CreateDir(const std::filesystem::path& path) override;
    };
}
