// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"

#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <fstream>
#include <limits>
#include <new>
#include <stdexcept>
#include <system_error>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <Windows.h>
#undef DeleteFile
#elif defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace AVEVA::RocksDB::Plugin::Core {
using namespace boost::log::trivial;

namespace {
#if defined(_WIN32)
using NativeHandle = HANDLE;
const auto InvalidHandle = INVALID_HANDLE_VALUE;
void CloseNative(NativeHandle handle) { CloseHandle(handle); }
void CheckNative(bool ok, const char* operation) {
    if (!ok)
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), operation);
}
NativeHandle OpenDirect(const std::filesystem::path& path, bool write) {
    auto handle = CreateFileW(path.c_str(), write ? GENERIC_WRITE : GENERIC_READ,
                              FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                              write ? CREATE_NEW : OPEN_EXISTING, FILE_FLAG_NO_BUFFERING, nullptr);
    CheckNative(handle != InvalidHandle, "open direct-I/O file");
    return handle;
}
size_t DirectAlignment(NativeHandle handle) {
    FILE_STORAGE_INFO info{};
    CheckNative(GetFileInformationByHandleEx(handle, FileStorageInfo, &info, sizeof(info)),
                "query direct-I/O alignment");
    return std::max(info.LogicalBytesPerSector, info.PhysicalBytesPerSectorForPerformance);
}
size_t NativeSize(NativeHandle handle) {
    LARGE_INTEGER size{};
    CheckNative(GetFileSizeEx(handle, &size), "get direct-I/O file size");
    if (size.QuadPart < 0 || static_cast<unsigned long long>(size.QuadPart) > SIZE_MAX)
        throw std::length_error("direct-I/O file is too large");
    return static_cast<size_t>(size.QuadPart);
}
size_t Transfer(NativeHandle handle, char* data, size_t size, bool write) {
    if (size > MAXDWORD)
        throw std::length_error("direct-I/O transfer is too large");
    DWORD transferred = 0;
    CheckNative(write ? WriteFile(handle, data, static_cast<DWORD>(size), &transferred, nullptr)
                      : ReadFile(handle, data, static_cast<DWORD>(size), &transferred, nullptr),
                write ? "direct-I/O write" : "direct-I/O read");
    return transferred;
}
#elif defined(__linux__)
using NativeHandle = int;
constexpr auto InvalidHandle = -1;
void CloseNative(NativeHandle handle) { close(handle); }
void CheckNative(bool ok, const char* operation) {
    if (!ok)
        throw std::system_error(errno, std::generic_category(), operation);
}
NativeHandle OpenDirect(const std::filesystem::path& path, bool write) {
    const auto handle =
        open(path.c_str(), O_DIRECT | O_CLOEXEC | (write ? O_WRONLY | O_CREAT | O_EXCL : O_RDONLY), 0600);
    CheckNative(handle != InvalidHandle, "open direct-I/O file");
    return handle;
}
size_t DirectAlignment(NativeHandle handle) {
#if defined(STATX_DIOALIGN) && defined(SYS_statx)
    struct statx info{};
    CheckNative(syscall(SYS_statx, handle, "", AT_EMPTY_PATH, STATX_DIOALIGN, &info) == 0,
                "query direct-I/O alignment");
    if ((info.stx_mask & STATX_DIOALIGN) && info.stx_dio_mem_align && info.stx_dio_offset_align)
        return std::max(info.stx_dio_mem_align, info.stx_dio_offset_align);
#endif
    throw std::runtime_error("filesystem/kernel does not report direct-I/O alignment (STATX_DIOALIGN required)");
}
size_t NativeSize(NativeHandle handle) {
    struct stat info{};
    CheckNative(fstat(handle, &info) == 0, "get direct-I/O file size");
    if (info.st_size < 0 || static_cast<uintmax_t>(info.st_size) > SIZE_MAX)
        throw std::length_error("direct-I/O file is too large");
    return static_cast<size_t>(info.st_size);
}
size_t Transfer(NativeHandle handle, char* data, size_t size, bool write) {
    if (size > static_cast<size_t>(std::numeric_limits<ssize_t>::max()))
        throw std::length_error("direct-I/O transfer is too large");
    ssize_t transferred;
    do {
        transferred = write ? ::write(handle, data, size) : ::read(handle, data, size);
    } while (transferred < 0 && errno == EINTR);
    CheckNative(transferred >= 0, write ? "direct-I/O write" : "direct-I/O read");
    return static_cast<size_t>(transferred);
}
#endif

/// <summary>Uses sector-aligned I/O, but keeps the on-disk length and returned payload unpadded.</summary>
std::string DirectFileContents(const std::filesystem::path& path, const char* data, size_t size, bool write) {
#if defined(_WIN32) || defined(__linux__)
    auto handle = OpenDirect(path, write);
    auto close = boost::scope::make_scope_exit([&] {
        if (handle != InvalidHandle)
            CloseNative(handle);
    });
    if (!write)
        size = NativeSize(handle);
    const auto alignment = std::max(DirectAlignment(handle), alignof(std::max_align_t));
    if ((alignment & (alignment - 1)) != 0 || size > SIZE_MAX - (alignment - 1))
        throw std::length_error("invalid direct-I/O alignment or file size");
    const auto padded = ((size + alignment - 1) / alignment) * alignment;
    auto* buffer = static_cast<char*>(::operator new(std::max(padded, alignment), std::align_val_t(alignment)));
    auto free = boost::scope::make_scope_exit([&] { ::operator delete(buffer, std::align_val_t(alignment)); });
    if (write) {
        std::memset(buffer, 0, padded);
        if (size)
            std::memcpy(buffer, data, size);
    }
    // A read at EOF may return the logical (unaligned) length. Writes must complete the padded length.
    const auto transferred = padded ? Transfer(handle, buffer, padded, write) : 0;
    if (transferred != (write ? padded : size))
        throw std::runtime_error("incomplete direct-I/O transfer");
    if (!write)
        return std::string(buffer, size);
#if defined(_WIN32)
    // Use a metadata-only handle to set an unaligned EOF; no buffered data I/O is performed.
    CloseNative(handle);
    handle = InvalidHandle;
    auto metadata = CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_DELETE, nullptr,
                                OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    CheckNative(metadata != InvalidHandle, "open file to trim direct-I/O padding");
    auto closeMetadata = boost::scope::make_scope_exit([&] { CloseNative(metadata); });
    LARGE_INTEGER end{};
    end.QuadPart = static_cast<LONGLONG>(size);
    CheckNative(SetFilePointerEx(metadata, end, nullptr, FILE_BEGIN) && SetEndOfFile(metadata),
                "trim direct-I/O padding");
#else
    if (size > static_cast<size_t>(std::numeric_limits<off_t>::max()))
        throw std::length_error("direct-I/O file is too large");
    CheckNative(ftruncate(handle, static_cast<off_t>(size)) == 0, "trim direct-I/O padding");
#endif
    return {};
#else
    (void)path;
    (void)data;
    (void)size;
    (void)write;
    throw std::runtime_error("direct I/O is supported only on Linux and Windows");
#endif
}
} // namespace

// -----------------------------------------------------------------------
// LocalFilesystem
// -----------------------------------------------------------------------
LocalFilesystem::LocalFilesystem(
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_logger(std::move(logger)) {}

LocalFilesystem::LocalFilesystem(
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger, bool directIo)
    : m_logger(std::move(logger)), m_directIo(directIo) {
    if (m_directIo && !m_logger)
        throw std::invalid_argument("LocalFilesystem: direct I/O requires a logger");
}

std::unique_ptr<File> LocalFilesystem::Open(const std::filesystem::path& path) {
    return std::make_unique<LocalFile>(path);
}

bool LocalFilesystem::DeleteFile(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error)
                << "Failed to remove file '" << path.string() << "'. Error: " << ec.message();
        return false;
    }
    return true;
}

bool LocalFilesystem::DeleteDir(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error)
                << "Failed to remove directories '" << path.string() << "'. Error: " << ec.message();
        return false;
    }
    return true;
}

bool LocalFilesystem::CreateDir(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error)
                << "Failed to create directories '" << path.string() << "'. Error: " << ec.message();
        return false;
    }
    return true;
}

std::optional<std::string> LocalFilesystem::ReadFileContents(const std::filesystem::path& path) noexcept {
    try {
        if (m_directIo)
            return DirectFileContents(path, nullptr, 0, false);
        std::ifstream f(path, std::ios::in | std::ios::binary | std::ios::ate);
        if (!f.is_open())
            return std::nullopt;
        const auto size = f.tellg();
        if (size < 0)
            return std::nullopt;
        std::string contents(static_cast<size_t>(size), '\0');
        f.seekg(0);
        f.read(contents.data(), size);
        if (!f)
            return std::nullopt;
        return contents;
    } catch (const std::exception& e) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error) << "Failed to read '" << path.string() << "': " << e.what();
        return std::nullopt;
    }
}

bool LocalFilesystem::WriteFileAtomic(const std::filesystem::path& finalPath, const char* data, size_t size) noexcept {
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

        if (m_directIo)
            DirectFileContents(stagingPath, data, size, true);
        else if (!WriteAllBytesToFile(stagingPath, data, size))
            return false;

        std::error_code ec;
        std::filesystem::rename(stagingPath, finalPath, ec);
        if (ec) {
            if (m_logger)
                BOOST_LOG_SEV(*m_logger, error)
                    << "Failed to rename '" << stagingPath.string() << "': " << ec.message();
            return false;
        }

        stagingCleanup.set_active(false);
        return true;
    } catch (const std::exception& e) {
        if (m_logger)
            BOOST_LOG_SEV(*m_logger, error) << "Failed to write '" << finalPath.string() << "': " << e.what();
        return false;
    }
}

bool LocalFilesystem::RenameFile(const std::filesystem::path& from, const std::filesystem::path& to) noexcept {
    std::error_code ec;
    std::filesystem::rename(from, to, ec);
    return !ec;
}

bool LocalFilesystem::WriteAllBytesToFile(const std::filesystem::path& path, const char* data, size_t size) noexcept {
    try {
        std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!f.is_open())
            return false;
        f.write(data, static_cast<std::streamsize>(size));
        return f.good();
    } catch (...) {
        return false;
    }
}
} // namespace AVEVA::RocksDB::Plugin::Core
