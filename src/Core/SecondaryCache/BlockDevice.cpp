// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "BlockDevice.hpp"

#include <boost/scope/scope_exit.hpp>

#include <algorithm>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

#ifdef _WIN32
#define NOMINMAX
#include <Windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
namespace {
/// Expected first line of the owner marker, used to recognize an existing cache child directory.
constexpr auto kMarker = "AVEVA disposable secondary cache v1";

/// <param name="path">Path whose final component is inspected.</param>
/// <returns>True for a symlink or, on Windows, any reparse point such as a junction.</returns>
bool IsLink(const std::filesystem::path& path) {
#ifdef _WIN32
    const auto attributes = GetFileAttributesW(path.c_str());
    if (attributes != INVALID_FILE_ATTRIBUTES && (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
        return true;
    }
#endif
    return std::filesystem::is_symlink(std::filesystem::symlink_status(path));
}

/// <param name="path">Path whose file name is checked.</param>
/// <returns>True for names of the form region_DIGITS.dat.</returns>
bool IsRegionFile(const std::filesystem::path& path) {
    const auto name = path.filename().string();
    if (!name.starts_with("region_") || !name.ends_with(".dat") || name.size() <= 11) {
        return false;
    }
    return std::all_of(name.begin() + 7, name.end() - 4, [](char ch) { return ch >= '0' && ch <= '9'; });
}

/// <summary>64-bit absolute seek on both platforms.</summary>
/// <param name="file">Open stream.</param>
/// <param name="offset">Byte offset from the start of the file.</param>
/// <returns>False if offset exceeds INT64_MAX or the seek fails.</returns>
bool Seek(std::FILE* file, const size_t offset) {
    if (offset > INT64_MAX) {
        return false;
    }
#ifdef _WIN32
    return _fseeki64(file, static_cast<int64_t>(offset), SEEK_SET) == 0;
#else
    return fseeko(file, static_cast<off_t>(offset), SEEK_SET) == 0;
#endif
}
} // namespace

struct FileBlockDevice::Owner {
#ifdef _WIN32
    HANDLE handle{INVALID_HANDLE_VALUE};
    ~Owner() {
        if (handle != INVALID_HANDLE_VALUE) {
            CloseHandle(handle);
        }
    }
#else
    int descriptor{-1};
    ~Owner() {
        if (descriptor != -1) {
            close(descriptor);
        }
    }
#endif
};

FileBlockDevice::FileBlockDevice(std::filesystem::path parent, MemoryBudget& budget)
    : m_owner(std::make_unique<Owner>()), m_lengths(BudgetAllocator<size_t>(budget)) {
    if (parent.empty()) {
        throw std::invalid_argument("secondary cache parent is empty");
    }
    parent = std::filesystem::absolute(parent).lexically_normal();
    if (parent == parent.root_path()) {
        throw std::invalid_argument("secondary cache parent cannot be a filesystem root");
    }
    for (auto path = parent; !path.empty() && path != path.root_path(); path = path.parent_path()) {
        if (IsLink(path)) {
            throw std::invalid_argument("secondary cache parent contains a link");
        }
    }
    m_directory = parent / "ssd-secondary-cache-v1";
    const auto markerPath = m_directory / "owner";
    const bool existed = std::filesystem::exists(std::filesystem::symlink_status(m_directory));
    bool createdDirectory = false;
    bool createdMarker = false;
    // Best-effort cleanup of this attempt's child and marker; never remove a pre-existing child's contents.
    auto cleanup = boost::scope::make_scope_exit([&] noexcept {
        if (createdDirectory) {
            m_owner.reset();
            std::error_code ignored;
            if (createdMarker) {
                std::filesystem::remove(markerPath, ignored);
            }
            std::filesystem::remove(m_directory, ignored); // Only succeeds if empty.
        }
    });
    if (existed) {
        if (IsLink(m_directory) || !std::filesystem::is_directory(m_directory)) {
            throw std::invalid_argument("secondary cache child is not an owned directory");
        }
        for (const auto& entry : std::filesystem::directory_iterator(m_directory)) {
            if (IsLink(entry.path()) || !entry.is_regular_file() ||
                (entry.path().filename() != "owner" && !IsRegionFile(entry.path()))) {
                throw std::invalid_argument("unexpected contents in secondary cache child");
            }
        }
        std::ifstream marker(markerPath);
        std::array<char, 64> contents{};
        marker.getline(contents.data(), static_cast<std::streamsize>(contents.size()));
        if (!marker || std::string_view(contents.data()) != kMarker) {
            throw std::invalid_argument("secondary cache ownership marker missing or invalid");
        }
    } else {
        std::filesystem::create_directories(parent);
        if (!std::filesystem::create_directory(m_directory)) {
            throw DeviceError("secondary cache directory concurrently created");
        }
        createdDirectory = true;
    }
#ifdef _WIN32
    m_owner->handle = CreateFileW(markerPath.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                                  existed ? OPEN_EXISTING : CREATE_NEW, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (m_owner->handle == INVALID_HANDLE_VALUE) {
        throw DeviceError("secondary cache ownership lock unavailable");
    }
    if (!existed) {
        createdMarker = true;
        DWORD written = 0;
        const std::string marker = std::string(kMarker) + "\n";
        if (!WriteFile(m_owner->handle, marker.data(), static_cast<DWORD>(marker.size()), &written, nullptr) ||
            written != marker.size()) {
            throw DeviceError("secondary cache ownership marker write failed");
        }
    }
#else
    m_owner->descriptor = open(markerPath.c_str(), O_RDWR | O_NOFOLLOW | (existed ? 0 : O_CREAT | O_EXCL), 0600);
    if (m_owner->descriptor == -1 || flock(m_owner->descriptor, LOCK_EX | LOCK_NB) != 0) {
        throw DeviceError("secondary cache ownership lock unavailable");
    }
    if (!existed) {
        createdMarker = true;
        const std::string marker = std::string(kMarker) + "\n";
        if (write(m_owner->descriptor, marker.data(), marker.size()) != static_cast<ssize_t>(marker.size())) {
            throw DeviceError("secondary cache ownership marker write failed");
        }
    }
#endif
    cleanup.set_active(false);
}

FileBlockDevice::~FileBlockDevice() { CloseHandles(); }

void FileBlockDevice::CloseHandles() noexcept {
    for (auto& handle : m_handles) {
        if (handle.file != nullptr) {
            std::fclose(handle.file);
            handle = {};
        }
    }
}

void FileBlockDevice::RemoveOwnedFiles() {
    for (const auto& entry : std::filesystem::directory_iterator(m_directory)) {
        if (IsLink(entry.path()) || !entry.is_regular_file() ||
            (entry.path().filename() != "owner" && !IsRegionFile(entry.path()))) {
            throw DeviceError("unexpected contents in owned secondary cache directory");
        }
    }
    for (const auto& entry : std::filesystem::directory_iterator(m_directory)) {
        if (IsRegionFile(entry.path()) && !std::filesystem::remove(entry.path())) {
            throw DeviceError("failed to remove owned secondary cache region");
        }
    }
}

void FileBlockDevice::Configure(const size_t capacity, const size_t regionSize) {
    CloseHandles();
    m_capacity = 0;
    RemoveOwnedFiles();
    decltype(m_lengths)(m_lengths.get_allocator()).swap(m_lengths);
    const size_t count = capacity / regionSize + (capacity % regionSize != 0 ? 1 : 0);
    m_lengths.assign(count, 0);
    m_regionSize = regionSize;
    m_capacity = capacity;
}

std::FILE* FileBlockDevice::Open(const uint32_t region) {
    for (const auto& handle : m_handles) {
        if (handle.file != nullptr && handle.region == region) {
            return handle.file;
        }
    }
    auto& handle = m_handles[m_nextHandle++ % kHandleLimit];
    if (handle.file != nullptr) {
        std::fclose(handle.file); // Writes were explicitly flushed before publication.
        handle = {};
    }
    const auto path = m_directory / ("region_" + std::to_string(region) + ".dat");
    if (IsLink(path)) {
        return nullptr;
    }
    const bool exists = std::filesystem::exists(path);
#ifdef _WIN32
    if (_wfopen_s(&handle.file, path.c_str(), exists ? L"r+b" : L"w+b") != 0) {
        return nullptr;
    }
#else
    handle.file = std::fopen(path.c_str(), exists ? "r+b" : "w+b");
#endif
    if (handle.file != nullptr) {
        if (std::setvbuf(handle.file, nullptr, _IONBF, 0) != 0) {
            std::fclose(handle.file);
            handle = {};
            return nullptr;
        }
        handle.region = region;
    }
    return handle.file;
}

bool FileBlockDevice::InBounds(const uint32_t region, const size_t offset, const size_t length) const noexcept {
    if (region >= m_lengths.size()) {
        return false;
    }
    const size_t budget = std::min(m_regionSize, m_capacity - size_t{region} * m_regionSize);
    return offset <= budget && length <= budget - offset;
}

bool FileBlockDevice::Read(const uint32_t region, const size_t offset, const std::span<std::byte> out) {
    if (!InBounds(region, offset, out.size()) || offset > m_lengths[region] ||
        out.size() > m_lengths[region] - offset) {
        return false;
    }
    auto* file = Open(region);
    if (file == nullptr) {
        return false;
    }
    std::clearerr(file);
    if (!Seek(file, offset)) {
        return false;
    }
    const auto read = std::fread(out.data(), 1, out.size(), file);
    bytesRead += read;
    return read == out.size();
}

bool FileBlockDevice::Write(const uint32_t region, const size_t offset, const std::span<const std::byte> in) {
    if (!InBounds(region, offset, in.size())) {
        return false;
    }
    auto* file = Open(region);
    if (file == nullptr) {
        return false;
    }
    std::clearerr(file);
    if (!Seek(file, offset)) {
        return false;
    }
    const auto written = std::fwrite(in.data(), 1, in.size(), file);
    bytesWritten += written;
    m_lengths[region] = std::max(m_lengths[region], offset + written);
    return written == in.size() && std::fflush(file) == 0;
}

size_t FileBlockDevice::FileBytes() const noexcept {
    size_t total = 0;
    for (const auto length : m_lengths) {
        total += length;
    }
    return total;
}

size_t FileBlockDevice::OpenHandles() const noexcept {
    return static_cast<size_t>(
        std::count_if(m_handles.begin(), m_handles.end(), [](const Handle& handle) { return handle.file != nullptr; }));
}

void MemoryBlockDevice::Configure(const size_t capacity, const size_t regionSize) {
    if (m_faultHook && m_faultHook(DeviceOperation::kConfigure, 0, 0, capacity)) {
        throw DeviceError("injected device configuration failure");
    }
    m_regions.clear();
    const size_t count = capacity / regionSize + (capacity % regionSize != 0 ? 1 : 0);
    for (size_t region = 0; region < count; ++region) {
        m_regions.emplace_back(std::min(regionSize, capacity - region * regionSize), std::byte{0});
    }
}

bool MemoryBlockDevice::Read(const uint32_t region, const size_t offset, const std::span<std::byte> out) {
    if ((m_faultHook && m_faultHook(DeviceOperation::kRead, region, offset, out.size())) ||
        region >= m_regions.size() || offset > m_regions[region].size() ||
        out.size() > m_regions[region].size() - offset) {
        return false;
    }
    std::copy_n(m_regions[region].data() + offset, out.size(), out.data());
    bytesRead += out.size();
    return true;
}

bool MemoryBlockDevice::Write(const uint32_t region, const size_t offset, const std::span<const std::byte> in) {
    if ((m_faultHook && m_faultHook(DeviceOperation::kWrite, region, offset, in.size())) ||
        region >= m_regions.size() || offset > m_regions[region].size() ||
        in.size() > m_regions[region].size() - offset) {
        return false;
    }
    std::copy(in.begin(), in.end(), m_regions[region].begin() + static_cast<std::ptrdiff_t>(offset));
    bytesWritten += in.size();
    return true;
}
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
