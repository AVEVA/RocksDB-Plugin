// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <algorithm>
#include <string>
#include <string_view>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// A directory handle. Directories are virtual (blob names carry the path), so it only remembers its name.
/// </summary>
class DirectoryImpl {
    std::string m_name;

  public:
    explicit DirectoryImpl(std::string_view dirname) : m_name(dirname) {}

    /// <summary>
    /// Nothing to sync: every blob operation is durable once the service acknowledges it, and creating a directory
    /// writes nothing.
    /// </summary>
    void Fsync() {}

    size_t GetUniqueId(char* id, size_t maxSize) const noexcept {
        size_t res = std::min(maxSize, m_name.length());
        std::copy_n(m_name.data(), res, id);
        return res;
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl