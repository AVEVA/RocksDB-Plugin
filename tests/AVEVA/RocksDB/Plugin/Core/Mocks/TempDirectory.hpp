// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>

namespace AVEVA::RocksDB::Plugin::Core::Mocks {
/// <summary>
/// A scratch directory under the system temp path that is unique to this instance and removed with it. The unique
/// component keeps tests that ctest runs in parallel processes (or repeats) from sharing, and deleting, one directory.
/// </summary>
class TempDirectory {
    std::filesystem::path m_path;

  public:
    explicit TempDirectory(std::string_view label)
        : m_path(std::filesystem::temp_directory_path() /
                 ("aveva_" + std::string(label) + "_" + boost::uuids::to_string(boost::uuids::random_generator()()))) {
        std::filesystem::create_directories(m_path);
    }

    ~TempDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(m_path, ignored);
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;
    TempDirectory(TempDirectory&&) = delete;
    TempDirectory& operator=(TempDirectory&&) = delete;

    [[nodiscard]] const std::filesystem::path& Path() const noexcept { return m_path; }
};
} // namespace AVEVA::RocksDB::Plugin::Core::Mocks
