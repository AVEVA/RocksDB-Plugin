// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"
namespace AVEVA::RocksDB::Plugin::Core {
LocalFile::LocalFile(const std::filesystem::path& path) : m_file(path, std::ios::in | std::ios::binary) {}

int64_t LocalFile::Read(char* buffer, int64_t offset, int64_t length) {
    // A short read (at the end of the file) sets eof/fail bits; clear them so a reused handle keeps working.
    m_file.clear();
    if (!m_file.is_open()) {
        throw std::runtime_error("Cannot read: the file could not be opened.");
    }
    m_file.seekg(static_cast<std::streamoff>(offset));
    if (m_file.fail()) {
        throw std::runtime_error("Cannot read: seeking to offset " + std::to_string(offset) + " failed.");
    }
    m_file.read(buffer, static_cast<std::streamsize>(length));

    const auto bytesRead = m_file.gcount();
    if (bytesRead < 0) {
        throw std::runtime_error("Invalid file read request. Received negative number of bytes read.");
    }

    return static_cast<int64_t>(bytesRead);
}
} // namespace AVEVA::RocksDB::Plugin::Core
