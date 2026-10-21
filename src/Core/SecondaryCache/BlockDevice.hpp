// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "MemoryBudget.hpp"

#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
class DeviceError final : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Serialized by RegionManager. No durability promise: successful writes reach the OS cache.
class BlockDevice {
  public:
    virtual ~BlockDevice() = default;
    virtual void Configure(size_t capacity, size_t regionSize) = 0;
    virtual bool Read(uint32_t region, size_t offset, std::span<std::byte> out) = 0;
    virtual bool Write(uint32_t region, size_t offset, std::span<const std::byte> in) = 0;
    virtual size_t FileBytes() const noexcept = 0;
    virtual size_t OpenHandles() const noexcept = 0;
    uint64_t bytesWritten{0};
    uint64_t bytesRead{0};
};

class FileBlockDevice final : public BlockDevice {
  public:
    FileBlockDevice(std::filesystem::path parent, MemoryBudget& budget);
    ~FileBlockDevice() override;
    void Configure(size_t capacity, size_t regionSize) override;
    bool Read(uint32_t region, size_t offset, std::span<std::byte> out) override;
    bool Write(uint32_t region, size_t offset, std::span<const std::byte> in) override;
    size_t FileBytes() const noexcept override;
    size_t OpenHandles() const noexcept override;
    static constexpr size_t kHandleLimit = 64;

  private:
    struct Owner;
    struct Handle {
        uint32_t region{UINT32_MAX};
        std::FILE* file{nullptr};
    };
    std::FILE* Open(uint32_t region);
    void CloseHandles() noexcept;
    void RemoveOwnedFiles();
    bool InBounds(uint32_t region, size_t offset, size_t length) const noexcept;
    std::unique_ptr<Owner> m_owner;
    std::filesystem::path m_directory;
    std::array<Handle, kHandleLimit> m_handles{};
    size_t m_nextHandle{0};
    size_t m_capacity{0};
    size_t m_regionSize{0};
    std::vector<size_t, BudgetAllocator<size_t>> m_lengths;
};

enum class DeviceOperation : uint8_t { kConfigure, kRead, kWrite };

class MemoryBlockDevice final : public BlockDevice {
  public:
    using FaultHook = std::function<bool(DeviceOperation, uint32_t, size_t, size_t)>;
    void Configure(size_t capacity, size_t regionSize) override;
    bool Read(uint32_t region, size_t offset, std::span<std::byte> out) override;
    bool Write(uint32_t region, size_t offset, std::span<const std::byte> in) override;
    size_t FileBytes() const noexcept override { return 0; }
    size_t OpenHandles() const noexcept override { return 0; }
    void SetFaultHook(FaultHook hook) { m_faultHook = std::move(hook); }
    std::vector<std::byte>& MutableRegion(uint32_t region) { return m_regions.at(region); }

  private:
    std::vector<std::vector<std::byte>> m_regions;
    FaultHook m_faultHook;
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
