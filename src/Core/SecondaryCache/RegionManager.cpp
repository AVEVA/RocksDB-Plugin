// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "RegionManager.hpp"

#include "RecordFormat.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <thread>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
namespace {
constexpr uint32_t kFooterMagic = 0x31544752u; // 'RGT1'
constexpr uint32_t kFooterVersion = 1;

#pragma pack(push, 1)
struct FooterHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t generation;
    uint32_t entryCount;
    uint64_t writeCursor;
    uint32_t overflow;
    uint32_t reserved;
    uint32_t crc;
};
#pragma pack(pop)
static_assert(sizeof(FooterHeader) == 36);
constexpr size_t kFooterCrcPrefixBytes = offsetof(FooterHeader, crc);
} // namespace

RegionManager::RegionManager(std::unique_ptr<BlockDevice> device, ShardedIndex& index, CacheStats& stats,
                             RegionManagerOptions options)
    : m_device(std::move(device)), m_index(index), m_stats(stats), m_options(options), m_footerSizeBytes(1ULL << 20),
      m_usableRegionBytes(m_options.regionSizeBytes > m_footerSizeBytes ? m_options.regionSizeBytes - m_footerSizeBytes
                                                                        : 0),
      m_regions(RegionCountFor(m_options.capacityBytes)) {
    if (m_usableRegionBytes == 0 || m_regions.empty()) {
        throw std::invalid_argument("RegionManager requires a region larger than the reserved footer");
    }

    for (auto& region : m_regions) {
        region.writeBuffer.reserve(std::max(m_options.flushBlockSizeBytes, m_options.maxEntrySizeBytes));
    }
    m_regions[0].state = Region::State::kOpen;
}

bool RegionManager::Reserve(const uint64_t keyHash, const uint32_t recordSize, const bool forceInsert,
                            Reservation& out) noexcept {
    std::lock_guard managerLock(m_managerMutex);
    if (recordSize > m_options.maxEntrySizeBytes) {
        return false;
    }
    if (!EnsureWritableRegion(recordSize, forceInsert)) {
        return false;
    }

    auto& region = m_regions[m_currentOpenRegion];
    std::lock_guard regionLock(region.mu);
    out.regionId = m_currentOpenRegion;
    out.generation = region.generation;
    out.offset = region.writeCursor;
    out.length = recordSize;
    out.keyHash = keyHash;
    region.writeCursor += recordSize;
    return true;
}

rocksdb::Status RegionManager::Publish(const Reservation& reservation,
                                       const std::span<const std::byte> record) noexcept {
    if (record.size() != reservation.length || reservation.regionId >= m_regions.size()) {
        return rocksdb::Status::InvalidArgument("invalid reservation publish");
    }

    auto& region = m_regions[reservation.regionId];
    std::lock_guard lock(region.mu);
    if (region.generation != reservation.generation || region.state != Region::State::kOpen) {
        return rocksdb::Status::Busy("region recycled during publish");
    }

    if (region.writeBuffer.empty()) {
        region.bufferedOffset = reservation.offset;
    }
    if (!region.writeBuffer.empty() && reservation.offset != region.bufferedOffset + region.writeBuffer.size()) {
        FlushOpenRegionLocked(region, reservation.regionId);
        region.bufferedOffset = reservation.offset;
    }

    if (!region.writeBuffer.empty() &&
        region.writeBuffer.size() + record.size() > std::max(m_options.flushBlockSizeBytes, record.size())) {
        FlushOpenRegionLocked(region, reservation.regionId);
        region.bufferedOffset = reservation.offset;
    }
    if (region.writeBuffer.empty()) {
        region.bufferedOffset = reservation.offset;
    }

    region.writeBuffer.insert(region.writeBuffer.end(), record.begin(), record.end());
    region.footerEntries.push_back(
        FooterEntry{reservation.keyHash, static_cast<uint32_t>(reservation.offset), reservation.length});
    const size_t maxFooterEntries = (m_footerSizeBytes - sizeof(FooterHeader)) / sizeof(FooterEntry);
    if (region.footerEntries.size() > maxFooterEntries) {
        region.footerOverflow = true;
        region.footerEntries.clear();
    }

    if (region.writeBuffer.size() >= m_options.flushBlockSizeBytes) {
        FlushOpenRegionLocked(region, reservation.regionId);
    }

    AdjustLiveBytes(static_cast<int64_t>(record.size()));
    m_stats.bytesInserted.value.fetch_add(record.size(), std::memory_order_relaxed);
    return rocksdb::Status::OK();
}

bool RegionManager::IsLive(const Location& location) noexcept {
    if (location.regionId >= m_regions.size()) {
        return false;
    }
    auto& region = m_regions[location.regionId];
    std::lock_guard lock(region.mu);
    return region.generation == location.generation && region.state != Region::State::kReclaiming &&
           region.state != Region::State::kRetired;
}

RegionManager::ReadStatus RegionManager::Read(const Location& location, std::vector<std::byte>& recordOut) noexcept {
    if (location.regionId >= m_regions.size()) {
        return ReadStatus::kRegionReclaimed;
    }

    auto& region = m_regions[location.regionId];
    region.activeReaders.fetch_add(1, std::memory_order_acq_rel);
    auto activeReaderGuard = [&, this] { region.activeReaders.fetch_sub(1, std::memory_order_acq_rel); };

    {
        std::lock_guard lock(region.mu);
        if (region.generation != location.generation || region.state == Region::State::kReclaiming) {
            activeReaderGuard();
            return ReadStatus::kRegionReclaimed;
        }

        const uint64_t offset = static_cast<uint64_t>(location.offsetInAlignUnits) * kRecordAlign;
        const size_t length = LocationLengthBytes(location);
        const uint64_t bufferEnd = region.bufferedOffset + region.writeBuffer.size();
        if (!region.writeBuffer.empty() && offset >= region.bufferedOffset && offset + length <= bufferEnd) {
            const auto beginIndex = offset - region.bufferedOffset;
            recordOut.assign(region.writeBuffer.begin() + static_cast<std::ptrdiff_t>(beginIndex),
                             region.writeBuffer.begin() + static_cast<std::ptrdiff_t>(beginIndex + length));
            activeReaderGuard();
            return ReadStatus::kOk;
        }
    }

    const uint64_t offset = static_cast<uint64_t>(location.offsetInAlignUnits) * kRecordAlign;
    const size_t length = LocationLengthBytes(location);
    recordOut.resize(length);
    const bool ok = m_device->Read(location.regionId, offset, std::span<std::byte>(recordOut.data(), recordOut.size()));
    activeReaderGuard();
    if (!ok) {
        return ReadStatus::kIoError;
    }

    m_stats.bytesRead.value.fetch_add(recordOut.size(), std::memory_order_relaxed);
    return ReadStatus::kOk;
}

rocksdb::Status RegionManager::SetCapacity(const size_t capacityBytes) noexcept {
    std::lock_guard managerLock(m_managerMutex);
    // The region files are preallocated at construction, so capacity can never exceed them.
    m_options.capacityBytes = std::min<size_t>(capacityBytes, m_regions.size() * m_usableRegionBytes);
    if (m_options.capacityBytes == 0) {
        ClearAllLocked();
        return rocksdb::Status::OK();
    }
    ApplyCapacityLocked();
    return rocksdb::Status::OK();
}

void RegionManager::ApplyCapacityLocked() noexcept {
    const uint32_t active = RegionCountFor(m_options.capacityBytes);

    // Growing: regions retired by an earlier shrink come back into rotation.
    for (uint32_t id = 0; id < std::min(active, m_nextUnusedRegion); ++id) {
        auto& region = m_regions[id];
        std::lock_guard lock(region.mu);
        if (region.state == Region::State::kRetired) {
            region.state = Region::State::kSealed;
            m_freeRegions.push_back(id);
        }
    }

    // Shrinking: regions beyond the new budget have zero capacity, so drain them first.
    if (m_currentOpenRegion >= active) {
        SealRegion(m_currentOpenRegion);
    }
    const auto retire = [&](const uint32_t id) {
        auto& region = m_regions[id];
        std::lock_guard lock(region.mu);
        region.state = Region::State::kRetired;
    };
    for (auto it = m_sealedOrder.begin(); it != m_sealedOrder.end();) {
        if (*it >= active) {
            const uint32_t id = *it;
            it = m_sealedOrder.erase(it);
            ReclaimRegion(id);
            retire(id);
        } else {
            ++it;
        }
    }
    for (auto it = m_freeRegions.begin(); it != m_freeRegions.end();) {
        if (*it >= active) {
            retire(*it);
            it = m_freeRegions.erase(it);
        } else {
            ++it;
        }
    }

    while (m_usageBytes.load(std::memory_order_relaxed) > m_options.capacityBytes) {
        if (m_sealedOrder.empty() && (!SealRegion(m_currentOpenRegion) || m_sealedOrder.empty())) {
            break;
        }
        const auto victim = m_sealedOrder.front();
        m_sealedOrder.pop_front();
        ReclaimRegion(victim);
        m_freeRegions.push_back(victim);
    }
}

size_t RegionManager::CapacityBytes() const noexcept {
    std::lock_guard lock(m_managerMutex);
    return m_options.capacityBytes;
}

size_t RegionManager::UsageBytes() const noexcept { return m_usageBytes.load(std::memory_order_relaxed); }

void RegionManager::AdjustLiveBytes(const int64_t delta) noexcept {
    if (delta >= 0) {
        m_usageBytes.fetch_add(static_cast<size_t>(delta), std::memory_order_relaxed);
    } else {
        const auto decrease = static_cast<size_t>(-delta);
        size_t current = m_usageBytes.load(std::memory_order_relaxed);
        while (!m_usageBytes.compare_exchange_weak(current, current > decrease ? current - decrease : 0,
                                                   std::memory_order_relaxed)) {
        }
    }
}

void RegionManager::ClearAll() noexcept {
    std::lock_guard managerLock(m_managerMutex);
    ClearAllLocked();
}

// Wipes every region without per-region footer or record scans: the whole index is dropped at once
// and all generations are bumped, so any in-flight reservation or index publish is detected as stale.
void RegionManager::ClearAllLocked() noexcept {
    m_index.Clear();
    for (auto& region : m_regions) {
        std::lock_guard lock(region.mu);
        region.state = Region::State::kReclaiming;
    }
    for (uint32_t regionId = 0; regionId < m_regions.size(); ++regionId) {
        auto& region = m_regions[regionId];
        while (region.activeReaders.load(std::memory_order_acquire) != 0) {
            std::this_thread::yield();
        }
        m_device->Reset(regionId);
        std::lock_guard lock(region.mu);
        region.writeCursor = 0;
        region.bufferedOffset = 0;
        region.writeBuffer.clear();
        region.footerEntries.clear();
        region.footerOverflow = false;
        ++region.generation;
        region.state = regionId == 0 ? Region::State::kOpen : Region::State::kSealed;
    }
    m_usageBytes.store(0, std::memory_order_relaxed);
    m_sealedOrder.clear();
    m_freeRegions.clear();
    m_currentOpenRegion = 0;
    m_nextUnusedRegion = 1;
}

uint32_t RegionManager::RegionCountFor(const size_t capacityBytes) const noexcept {
    if (capacityBytes == 0 || m_usableRegionBytes == 0) {
        return 1;
    }
    const auto count = static_cast<uint32_t>((capacityBytes + m_usableRegionBytes - 1) / m_usableRegionBytes);
    return std::max<uint32_t>(1, count);
}

uint64_t RegionManager::RegionBudgetBytes(const uint32_t regionId) const noexcept {
    const uint64_t regionStart = static_cast<uint64_t>(regionId) * m_usableRegionBytes;
    if (regionStart >= m_options.capacityBytes) {
        return 0;
    }
    return std::min<uint64_t>(m_usableRegionBytes, m_options.capacityBytes - regionStart);
}

bool RegionManager::EnsureWritableRegion(const uint32_t recordSize, const bool forceInsert) noexcept {
    if (m_options.capacityBytes == 0) {
        return false;
    }
    auto& current = m_regions[m_currentOpenRegion];
    {
        std::lock_guard currentLock(current.mu);
        if (current.state == Region::State::kOpen &&
            current.writeCursor + recordSize <= RegionBudgetBytes(m_currentOpenRegion)) {
            return true;
        }
    }

    const uint32_t activeRegionCount = RegionCountFor(m_options.capacityBytes);
    // Full cache: reclaim the oldest sealed region for every insert, forced or not. Dropping
    // non-forced inserts left the cache frozen on its first contents and every later block missed.
    (void)forceInsert;

    SealRegion(m_currentOpenRegion);

    if (m_nextUnusedRegion < activeRegionCount) {
        m_currentOpenRegion = m_nextUnusedRegion++;
    } else if (!m_freeRegions.empty()) {
        m_currentOpenRegion = m_freeRegions.front();
        m_freeRegions.pop_front();
    } else {
        if (m_sealedOrder.empty()) {
            return false;
        }
        const auto victim = m_sealedOrder.front();
        m_sealedOrder.pop_front();
        if (!ReclaimRegion(victim)) {
            return false;
        }
        m_currentOpenRegion = victim;
    }

    auto& next = m_regions[m_currentOpenRegion];
    std::lock_guard nextLock(next.mu);
    next.state = Region::State::kOpen;
    next.writeCursor = 0;
    next.bufferedOffset = 0;
    next.writeBuffer.clear();
    next.footerEntries.clear();
    next.footerOverflow = false;
    return recordSize <= RegionBudgetBytes(m_currentOpenRegion);
}

bool RegionManager::SealRegion(const uint32_t regionId) noexcept {
    auto& region = m_regions[regionId];
    std::lock_guard lock(region.mu);
    if (region.state != Region::State::kOpen) {
        return true;
    }

    FlushOpenRegionLocked(region, regionId);
    if (region.writeCursor == 0) {
        region.state = Region::State::kSealed;
        m_sealedOrder.push_back(regionId);
        return true;
    }

    if (m_footerSizeBytes < sizeof(FooterHeader)) {
        region.state = Region::State::kSealed;
        m_sealedOrder.push_back(regionId);
        return true;
    }

    std::vector<std::byte> footer(m_footerSizeBytes, std::byte{0});
    FooterHeader header{};
    header.magic = kFooterMagic;
    header.version = kFooterVersion;
    header.generation = region.generation;
    header.entryCount = static_cast<uint32_t>(region.footerEntries.size());
    header.writeCursor = region.writeCursor;
    header.overflow = region.footerOverflow ? 1u : 0u;
    std::memcpy(footer.data(), &header, sizeof(header));

    const size_t maxEntries = (m_footerSizeBytes - sizeof(FooterHeader)) / sizeof(FooterEntry);
    const size_t entryCount = std::min(region.footerEntries.size(), maxEntries);
    if (entryCount != 0) {
        std::memcpy(footer.data() + static_cast<std::ptrdiff_t>(sizeof(FooterHeader)), region.footerEntries.data(),
                    entryCount * sizeof(FooterEntry));
    }

    header.crc = ComputeCrc32c(
        std::span<const std::byte>(footer.data(), footer.data() + static_cast<std::ptrdiff_t>(kFooterCrcPrefixBytes)));
    header.crc = ComputeCrc32c(
        std::span<const std::byte>(
            footer.data() + static_cast<std::ptrdiff_t>(sizeof(FooterHeader)),
            footer.data() + static_cast<std::ptrdiff_t>(sizeof(FooterHeader) + entryCount * sizeof(FooterEntry))),
        header.crc);
    std::memcpy(footer.data(), &header, sizeof(header));

    const uint64_t footerOffset = m_options.regionSizeBytes - m_footerSizeBytes;
    // A failed footer write is survivable: reclaim falls back to scanning the region.
    if (!m_device->Write(regionId, footerOffset, std::span<const std::byte>(footer.data(), footer.size()))) {
        m_stats.deviceWriteFailures.value.fetch_add(1, std::memory_order_relaxed);
    }

    region.state = Region::State::kSealed;
    m_sealedOrder.push_back(regionId);
    return true;
}

bool RegionManager::ReclaimRegion(const uint32_t regionId) noexcept {
    auto& region = m_regions[regionId];
    {
        std::lock_guard lock(region.mu);
        region.state = Region::State::kReclaiming;
    }

    std::vector<FooterEntry> entries;
    bool overflow = false;
    const uint16_t generation = region.generation;
    // Full usable size, not the current budget: a region beyond a just-shrunk capacity has budget 0
    // but still holds records that must be swept from the index.
    const uint64_t budgetBytes = m_usableRegionBytes;
    // Nothing was ever reserved in this generation, so there is nothing to sweep or scan.
    if (region.writeCursor != 0 && (!TryLoadFooter(regionId, generation, budgetBytes, entries, overflow) || overflow)) {
        m_stats.footerOverflowScans.value.fetch_add(1, std::memory_order_relaxed);
        entries = ScanRegionEntries(regionId, budgetBytes);
    }

    for (const auto& entry : entries) {
        if (const auto removed = m_index.RemoveIfInRegion(entry.keyHash, regionId, generation)) {
            AdjustLiveBytes(-static_cast<int64_t>(LocationLengthBytes(*removed)));
            m_stats.entriesEvictedByReclaim.value.fetch_add(1, std::memory_order_relaxed);
        }
    }

    while (region.activeReaders.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }

    m_device->Reset(regionId);
    std::lock_guard lock(region.mu);
    region.writeCursor = 0;
    region.bufferedOffset = 0;
    region.writeBuffer.clear();
    region.footerOverflow = false;
    region.footerEntries.clear();
    ++region.generation;
    region.state = Region::State::kSealed;
    m_stats.regionsReclaimed.value.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void RegionManager::FlushOpenRegionLocked(Region& region, const uint32_t regionId) noexcept {
    if (region.writeBuffer.empty()) {
        return;
    }

    const bool ok = m_device->Write(regionId, region.bufferedOffset,
                                    std::span<const std::byte>(region.writeBuffer.data(), region.writeBuffer.size()));
    if (ok) {
        m_stats.bytesWrittenToDevice.value.fetch_add(region.writeBuffer.size(), std::memory_order_relaxed);
    } else {
        // Drop the buffer rather than keep it: leaving it would make later appends flush at a stale
        // offset. Index entries pointing at the lost bytes fail validation on read and self-heal.
        m_stats.deviceWriteFailures.value.fetch_add(1, std::memory_order_relaxed);
    }
    region.writeBuffer.clear();
    region.bufferedOffset = region.writeCursor;
}

bool RegionManager::TryLoadFooter(const uint32_t regionId, const uint16_t generation, const uint64_t budgetBytes,
                                  std::vector<FooterEntry>& entriesOut, bool& overflowOut) noexcept {
    entriesOut.clear();
    overflowOut = false;

    std::vector<std::byte> footer(m_footerSizeBytes);
    if (!m_device->Read(regionId, m_options.regionSizeBytes - m_footerSizeBytes,
                        std::span<std::byte>(footer.data(), footer.size()))) {
        return false;
    }

    FooterHeader header{};
    if (footer.size() < sizeof(FooterHeader)) {
        return false;
    }
    std::memcpy(&header, footer.data(), sizeof(header));
    if (header.magic != kFooterMagic || header.version != kFooterVersion || header.generation != generation ||
        header.writeCursor > budgetBytes) {
        return false;
    }

    const size_t requiredBytes = sizeof(FooterHeader) + static_cast<size_t>(header.entryCount) * sizeof(FooterEntry);
    if (requiredBytes > footer.size()) {
        return false;
    }

    uint32_t crc = ComputeCrc32c(
        std::span<const std::byte>(footer.data(), footer.data() + static_cast<std::ptrdiff_t>(kFooterCrcPrefixBytes)));
    crc = ComputeCrc32c(std::span<const std::byte>(footer.data() + static_cast<std::ptrdiff_t>(sizeof(FooterHeader)),
                                                   footer.data() + static_cast<std::ptrdiff_t>(requiredBytes)),
                        crc);
    if (crc != header.crc) {
        return false;
    }

    entriesOut.resize(header.entryCount);
    if (header.entryCount != 0) {
        std::memcpy(entriesOut.data(), footer.data() + static_cast<std::ptrdiff_t>(sizeof(FooterHeader)),
                    entriesOut.size() * sizeof(FooterEntry));
    }
    overflowOut = header.overflow != 0;
    return true;
}

std::vector<RegionManager::FooterEntry> RegionManager::ScanRegionEntries(const uint32_t regionId,
                                                                         const uint64_t budgetBytes) noexcept {
    std::vector<std::byte> regionBytes(budgetBytes);
    std::vector<FooterEntry> entries;
    if (!m_device->Read(regionId, 0, std::span<std::byte>(regionBytes.data(), regionBytes.size()))) {
        return entries;
    }

    size_t offset = 0;
    while (offset + sizeof(RecordHeader) <= regionBytes.size()) {
        RecordHeader header{};
        if (!TryReadHeader(std::span<const std::byte>(regionBytes.data() + static_cast<std::ptrdiff_t>(offset),
                                                      regionBytes.size() - offset),
                           header)) {
            break;
        }
        if (header.magic != kRecordMagic || header.formatVersion != kFormatVersion) {
            break;
        }
        const size_t recordSize = RecordSize(header);
        if (recordSize == 0 || offset + recordSize > regionBytes.size()) {
            break;
        }
        entries.push_back(
            FooterEntry{header.keyHash, static_cast<uint32_t>(offset), static_cast<uint32_t>(recordSize)});
        offset += recordSize;
    }

    return entries;
}
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
