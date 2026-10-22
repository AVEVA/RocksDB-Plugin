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
/// Thrown for device setup and ownership failures; Read and Write report I/O failures by return value instead.
class DeviceError final : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

/// <summary>
/// Stores region bytes addressed by (region, byte offset). Not thread-safe: RegionManager serializes every call
/// under its mutex. No durability promise: a successful write has only reached the OS cache.
/// </summary>
class BlockDevice {
  public:
    virtual ~BlockDevice() = default;

    /// <summary>
    /// Discards all stored data and lays out ceil(capacity / regionSize) regions; the last may be shorter.
    /// A capacity of 0 releases everything.
    /// </summary>
    /// <param name="capacity">Total bytes across all regions.</param>
    /// <param name="regionSize">Maximum bytes per region.</param>
    virtual void Configure(size_t capacity, size_t regionSize) = 0;

    /// <summary>Reads exactly out.size() bytes.</summary>
    /// <param name="region">Region index.</param>
    /// <param name="offset">Byte offset within the region.</param>
    /// <param name="out">[out] Destination; its size is the read length.</param>
    /// <returns>True only if every byte was read; false for out-of-range requests or I/O errors.</returns>
    virtual bool Read(uint32_t region, size_t offset, std::span<std::byte> out) = 0;

    /// <summary>Writes all of in.</summary>
    /// <param name="region">Region index.</param>
    /// <param name="offset">Byte offset within the region.</param>
    /// <param name="in">Bytes to write.</param>
    /// <returns>True only if every byte was written; false for out-of-range requests or I/O errors.</returns>
    virtual bool Write(uint32_t region, size_t offset, std::span<const std::byte> in) = 0;

    /// <returns>Total logical length of backing files, for statistics.</returns>
    virtual size_t FileBytes() const noexcept = 0;

    /// <returns>Number of open OS file handles, for statistics.</returns>
    virtual size_t OpenHandles() const noexcept = 0;

    /// Cumulative bytes transferred by Write, for statistics.
    uint64_t bytesWritten{0};
    /// Cumulative bytes transferred by Read, for statistics.
    uint64_t bytesRead{0};
};

/// <summary>
/// Stores region N in parent/ssd-secondary-cache-v1/region_N.dat. An exclusively locked "owner" marker file stops
/// two instances from sharing the directory. Links, reparse points and unexpected directory contents are rejected,
/// so nothing the cache does not own is ever deleted. Files are created lazily and grow on write.
/// </summary>
class FileBlockDevice final : public BlockDevice {
  public:
    /// <summary>Validates parent, then creates or adopts the owned child directory and locks its marker.</summary>
    /// <param name="parent">Directory to hold the child; must not be a filesystem root or contain links.</param>
    /// <param name="budget">Metadata budget charged for per-region file lengths.</param>
    /// <exception cref="std::invalid_argument">Unsafe parent, or a child with foreign contents or no valid
    /// marker.</exception>
    /// <exception cref="DeviceError">Another instance owns or is creating the directory, or the marker cannot be
    /// written.</exception>
    FileBlockDevice(std::filesystem::path parent, MemoryBudget& budget);

    /// <summary>Closes open handles and releases the lock. Region files stay on disk until a later instance
    /// configures the directory.</summary>
    ~FileBlockDevice() override;

    /// <summary>Closes handles and deletes all owned region files before applying the new geometry.</summary>
    /// <exception cref="DeviceError">The directory has unexpected contents or a region file cannot be
    /// removed.</exception>
    void Configure(size_t capacity, size_t regionSize) override;

    /// <remarks>Also fails for bytes beyond the region file's logical length.</remarks>
    bool Read(uint32_t region, size_t offset, std::span<std::byte> out) override;

    /// <remarks>Succeeds only after the stream is flushed to the OS.</remarks>
    bool Write(uint32_t region, size_t offset, std::span<const std::byte> in) override;

    size_t FileBytes() const noexcept override;
    size_t OpenHandles() const noexcept override;

    /// Maximum region files open at once; slots are recycled round-robin beyond this.
    static constexpr size_t kHandleLimit = 64;

  private:
    /// Platform handle that holds the exclusive lock on the owner marker for this object's lifetime.
    struct Owner;
    /// One cached open region file; region is UINT32_MAX and file is null when the slot is empty.
    struct Handle {
        uint32_t region{UINT32_MAX};
        std::FILE* file{nullptr};
    };

    /// <summary>
    /// Returns an unbuffered stream for region, reusing a cached handle or recycling the next slot. Creates the
    /// file if it does not exist.
    /// </summary>
    /// <param name="region">Region index.</param>
    /// <returns>The stream, or null if the path is a link or the file cannot be opened.</returns>
    std::FILE* Open(uint32_t region);

    /// <summary>Closes every cached handle.</summary>
    void CloseHandles() noexcept;

    /// <summary>Deletes region files after verifying the directory holds only the marker and region files.</summary>
    /// <exception cref="DeviceError">Unexpected contents, or a region file cannot be removed.</exception>
    void RemoveOwnedFiles();

    /// <param name="region">Region index.</param>
    /// <param name="offset">Byte offset within the region.</param>
    /// <param name="length">Byte count.</param>
    /// <returns>True if [offset, offset + length) lies within the region's configured size.</returns>
    bool InBounds(uint32_t region, size_t offset, size_t length) const noexcept;

    /// Ownership lock; released last, on destruction.
    std::unique_ptr<Owner> m_owner;
    /// The owned child directory, parent/ssd-secondary-cache-v1.
    std::filesystem::path m_directory;
    /// Fixed pool of cached region file handles.
    std::array<Handle, kHandleLimit> m_handles{};
    /// Round-robin cursor for the next slot to recycle.
    size_t m_nextHandle{0};
    /// Configured capacity; 0 means unconfigured or disabled.
    size_t m_capacity{0};
    /// Configured maximum region size.
    size_t m_regionSize{0};
    /// Logical length (highest byte written) of each region file; reads beyond it fail.
    std::vector<size_t, BudgetAllocator<size_t>> m_lengths;
};

/// Operation passed to MemoryBlockDevice's fault hook.
enum class DeviceOperation : uint8_t { kConfigure, kRead, kWrite };

/// <summary>In-memory BlockDevice for tests, with optional fault injection.</summary>
class MemoryBlockDevice final : public BlockDevice {
  public:
    /// <summary>
    /// Called before each operation with (operation, region, offset, length); returning true injects a failure
    /// (Configure throws DeviceError, Read and Write return false). For kConfigure, region and offset are 0 and
    /// length is the capacity.
    /// </summary>
    using FaultHook = std::function<bool(DeviceOperation, uint32_t, size_t, size_t)>;
    void Configure(size_t capacity, size_t regionSize) override;
    bool Read(uint32_t region, size_t offset, std::span<std::byte> out) override;
    bool Write(uint32_t region, size_t offset, std::span<const std::byte> in) override;
    size_t FileBytes() const noexcept override { return 0; }
    size_t OpenHandles() const noexcept override { return 0; }

    /// <param name="hook">Hook to install; an empty function disables fault injection.</param>
    void SetFaultHook(FaultHook hook) { m_faultHook = std::move(hook); }

    /// <summary>Gives tests direct access to stored bytes, e.g. to corrupt a record.</summary>
    /// <param name="region">Region index.</param>
    /// <returns>The region's bytes.</returns>
    /// <exception cref="std::out_of_range">region is not configured.</exception>
    std::vector<std::byte>& MutableRegion(uint32_t region) { return m_regions.at(region); }

  private:
    /// Bytes of each region, fully allocated by Configure.
    std::vector<std::vector<std::byte>> m_regions;
    /// Optional fault injector; empty means no faults.
    FaultHook m_faultHook;
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
