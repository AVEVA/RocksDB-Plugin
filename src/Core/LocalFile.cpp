// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace AVEVA::RocksDB::Plugin::Core {
#ifdef _WIN32
LocalFile::LocalFile(const std::filesystem::path& path)
    : m_handle(::CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                             nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr)) {}

LocalFile::~LocalFile() {
    if (m_handle != INVALID_HANDLE_VALUE) {
        ::CloseHandle(m_handle);
    }
}

ByteCount LocalFile::Read(char* buffer, const ByteOffset offset, const ByteCount length) {
    if (m_handle == INVALID_HANDLE_VALUE) {
        throw std::runtime_error("Cannot read: the file could not be opened.");
    }

    std::uint64_t total = 0;
    while (total < length.Value()) {
        const auto chunk =
            static_cast<DWORD>(std::min<std::uint64_t>(length.Value() - total, std::numeric_limits<DWORD>::max()));
        // An explicit offset makes the read independent of the file pointer, so concurrent reads do not interfere.
        OVERLAPPED position{};
        const auto at = offset.Value() + total;
        position.Offset = static_cast<DWORD>(at & 0xFFFFFFFFu);
        position.OffsetHigh = static_cast<DWORD>(at >> 32);
        DWORD read = 0;
        if (!::ReadFile(m_handle, buffer + total, chunk, &read, &position)) {
            const DWORD error = ::GetLastError();
            if (error == ERROR_HANDLE_EOF) {
                break;
            }
            throw std::system_error(static_cast<int>(error), std::system_category(), "Cannot read the file");
        }
        if (read == 0) {
            break;
        }
        total += read;
    }
    return ByteCount{total};
}
#else
LocalFile::LocalFile(const std::filesystem::path& path) : m_fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC)) {}

LocalFile::~LocalFile() {
    if (m_fd >= 0) {
        ::close(m_fd);
    }
}

ByteCount LocalFile::Read(char* buffer, const ByteOffset offset, const ByteCount length) {
    if (m_fd < 0) {
        throw std::runtime_error("Cannot read: the file could not be opened.");
    }

    std::uint64_t total = 0;
    while (total < length.Value()) {
        const ssize_t read = ::pread(m_fd, buffer + total, static_cast<size_t>(length.Value() - total),
                                     static_cast<off_t>(offset.Value() + total));
        if (read < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "Cannot read the file");
        }
        if (read == 0) {
            break;
        }
        total += static_cast<std::uint64_t>(read);
    }
    return ByteCount{total};
}
#endif
} // namespace AVEVA::RocksDB::Plugin::Core
