// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <fstream>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
class BlockDevice {
  public:
    virtual ~BlockDevice() = default;
    virtual bool Read(uint32_t regionId, uint64_t offset, std::span<std::byte> out) noexcept = 0;
    virtual bool Write(uint32_t regionId, uint64_t offset, std::span<const std::byte> in) noexcept = 0;
    [[nodiscard]] virtual size_t AlignmentBytes() const noexcept = 0;
    virtual void Reset(uint32_t regionId) noexcept = 0;
};

class FileBlockDevice final : public BlockDevice {
  public:
    FileBlockDevice(std::filesystem::path cacheDir, uint32_t regionCount, uint64_t regionSizeBytes);
    ~FileBlockDevice() override;

    bool Read(uint32_t regionId, uint64_t offset, std::span<std::byte> out) noexcept override;
    bool Write(uint32_t regionId, uint64_t offset, std::span<const std::byte> in) noexcept override;
    [[nodiscard]] size_t AlignmentBytes() const noexcept override;
    void Reset(uint32_t regionId) noexcept override;

  private:
    struct RegionFile {
        std::mutex mu;
        std::fstream stream;
    };

    std::filesystem::path m_cacheDir;
    uint64_t m_regionSizeBytes;
    std::vector<RegionFile> m_regions;
};

enum class DeviceOperation : uint8_t {
    kRead,
    kWrite,
    kReset,
};

class MemoryBlockDevice final : public BlockDevice {
  public:
    using FaultHook = std::function<bool(DeviceOperation, uint32_t, uint64_t, size_t)>;

    MemoryBlockDevice(uint32_t regionCount, uint64_t regionSizeBytes);

    bool Read(uint32_t regionId, uint64_t offset, std::span<std::byte> out) noexcept override;
    bool Write(uint32_t regionId, uint64_t offset, std::span<const std::byte> in) noexcept override;
    [[nodiscard]] size_t AlignmentBytes() const noexcept override;
    void Reset(uint32_t regionId) noexcept override;

    void SetFaultHook(FaultHook hook);
    [[nodiscard]] std::vector<std::byte>& MutableRegion(uint32_t regionId) noexcept;
    [[nodiscard]] const std::vector<std::byte>& Region(uint32_t regionId) const noexcept;

  private:
    [[nodiscard]] bool ShouldFail(DeviceOperation op, uint32_t regionId, uint64_t offset, size_t size) noexcept;

    uint64_t m_regionSizeBytes;
    std::vector<std::vector<std::byte>> m_regions;
    FaultHook m_faultHook;
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
