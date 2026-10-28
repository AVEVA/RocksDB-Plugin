// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <filesystem>
namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>Read-only local file using positional reads, so <c>Read</c> is safe to call concurrently.</summary>
class LocalFile final : public File {
#ifdef _WIN32
    void* m_handle;
#else
    int m_fd;
#endif

  public:
    explicit LocalFile(const std::filesystem::path& path);
    ~LocalFile() override;
    LocalFile(const LocalFile&) = delete;
    LocalFile& operator=(const LocalFile&) = delete;
    virtual int64_t Read(char* buffer, int64_t offset, int64_t length) override;
};
} // namespace AVEVA::RocksDB::Plugin::Core
