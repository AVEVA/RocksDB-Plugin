// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "BlockDevice.hpp"

#include "RecordFormat.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
namespace {
[[nodiscard]] std::filesystem::path RegionPath(const std::filesystem::path& cacheDir, const uint32_t regionId) {
    std::ostringstream builder;
    builder << "region_" << std::setfill('0') << std::setw(5) << regionId << ".dat";
    return cacheDir / builder.str();
}

void PreallocateFile(const std::filesystem::path& path, const uint64_t sizeBytes) {
    {
        std::ofstream creator(path, std::ios::binary | std::ios::trunc);
        if (!creator.is_open()) {
            throw std::runtime_error("failed to create region file");
        }
        if (sizeBytes != 0) {
            creator.seekp(static_cast<std::streamoff>(sizeBytes - 1), std::ios::beg);
            const char zero = 0;
            creator.write(&zero, 1);
        }
    }
}
} // namespace

FileBlockDevice::FileBlockDevice(std::filesystem::path cacheDir, const uint32_t regionCount, const uint64_t regionSizeBytes)
    : m_cacheDir(std::move(cacheDir)), m_regionSizeBytes(regionSizeBytes), m_regions(regionCount) {
    std::filesystem::remove_all(m_cacheDir);
    std::filesystem::create_directories(m_cacheDir);

    for (uint32_t regionId = 0; regionId < regionCount; ++regionId) {
        const auto path = RegionPath(m_cacheDir, regionId);
        PreallocateFile(path, m_regionSizeBytes);
        auto& region = m_regions[regionId];
        region.stream.open(path, std::ios::binary | std::ios::in | std::ios::out);
        if (!region.stream.is_open()) {
            throw std::runtime_error("failed to open region file");
        }
    }
}

FileBlockDevice::~FileBlockDevice() = default;

bool FileBlockDevice::Read(const uint32_t regionId, const uint64_t offset, const std::span<std::byte> out) noexcept {
    if (regionId >= m_regions.size() || offset + out.size() > m_regionSizeBytes) {
        return false;
    }

    auto& region = m_regions[regionId];
    std::lock_guard lock(region.mu);
    region.stream.clear();
    region.stream.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    region.stream.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(out.size()));
    return region.stream.good() || region.stream.gcount() == static_cast<std::streamsize>(out.size());
}

bool FileBlockDevice::Write(const uint32_t regionId, const uint64_t offset, const std::span<const std::byte> in) noexcept {
    if (regionId >= m_regions.size() || offset + in.size() > m_regionSizeBytes) {
        return false;
    }

    auto& region = m_regions[regionId];
    std::lock_guard lock(region.mu);
    region.stream.clear();
    region.stream.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    region.stream.write(reinterpret_cast<const char*>(in.data()), static_cast<std::streamsize>(in.size()));
    region.stream.flush();
    return region.stream.good();
}

size_t FileBlockDevice::AlignmentBytes() const noexcept { return kDeviceBlock; }

void FileBlockDevice::Reset(const uint32_t regionId) noexcept {
    if (regionId >= m_regions.size()) {
        return;
    }

    auto& region = m_regions[regionId];
    std::lock_guard lock(region.mu);
    region.stream.flush();
}

MemoryBlockDevice::MemoryBlockDevice(const uint32_t regionCount, const uint64_t regionSizeBytes)
    : m_regionSizeBytes(regionSizeBytes), m_regions(regionCount, std::vector<std::byte>(regionSizeBytes, std::byte{0})) {}

bool MemoryBlockDevice::Read(const uint32_t regionId, const uint64_t offset, const std::span<std::byte> out) noexcept {
    if (ShouldFail(DeviceOperation::kRead, regionId, offset, out.size()) || regionId >= m_regions.size() ||
        offset + out.size() > m_regionSizeBytes) {
        return false;
    }
    const auto& region = m_regions[regionId];
    std::copy_n(region.data() + static_cast<std::ptrdiff_t>(offset), out.size(), out.data());
    return true;
}

bool MemoryBlockDevice::Write(const uint32_t regionId, const uint64_t offset,
                              const std::span<const std::byte> in) noexcept {
    if (ShouldFail(DeviceOperation::kWrite, regionId, offset, in.size()) || regionId >= m_regions.size() ||
        offset + in.size() > m_regionSizeBytes) {
        return false;
    }
    auto& region = m_regions[regionId];
    std::copy_n(in.data(), in.size(), region.data() + static_cast<std::ptrdiff_t>(offset));
    return true;
}

size_t MemoryBlockDevice::AlignmentBytes() const noexcept { return kDeviceBlock; }

void MemoryBlockDevice::Reset(const uint32_t regionId) noexcept {
    if (ShouldFail(DeviceOperation::kReset, regionId, 0, 0) || regionId >= m_regions.size()) {
        return;
    }
    std::fill(m_regions[regionId].begin(), m_regions[regionId].end(), std::byte{0});
}

void MemoryBlockDevice::SetFaultHook(FaultHook hook) { m_faultHook = std::move(hook); }

std::vector<std::byte>& MemoryBlockDevice::MutableRegion(const uint32_t regionId) noexcept { return m_regions[regionId]; }

const std::vector<std::byte>& MemoryBlockDevice::Region(const uint32_t regionId) const noexcept {
    return m_regions[regionId];
}

bool MemoryBlockDevice::ShouldFail(const DeviceOperation op, const uint32_t regionId, const uint64_t offset,
                                   const size_t size) noexcept {
    return m_faultHook ? m_faultHook(op, regionId, offset, size) : false;
}
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
