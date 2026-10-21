// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "RegionManager.hpp"

#include <algorithm>
#include <cassert>
#include <limits>
#include <stdexcept>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
namespace {
size_t MetadataLimit(const FileBasedSecondaryCacheOptions& options, const size_t capacity) {
    return options.metadataBudget != 0 ? options.metadataBudget : std::max(size_t{1} << 20, capacity / 200);
}
} // namespace

void RegionManager::Validate(const FileBasedSecondaryCacheOptions& options) {
    if (options.engine != FileBasedSecondaryCacheEngine::kRegion ||
        options.admissionPolicy != FileBasedSecondaryCacheAdmissionPolicy::kAdmitAll ||
        options.regionSize < kRecordAlign || options.regionSize > UINT32_MAX ||
        options.regionSize % kRecordAlign != 0 || options.flushBlockSize == 0 ||
        options.flushBlockSize > options.regionSize || options.flushBlockSize % kRecordAlign != 0 ||
        options.maxEntrySize < kRecordAlign || options.maxEntrySize > options.regionSize ||
        options.maxEntrySize % kRecordAlign != 0 || options.transientBudget == 0 ||
        options.capacity / options.regionSize + (options.capacity % options.regionSize != 0 ? 1 : 0) > UINT32_MAX) {
        throw std::invalid_argument("invalid region cache geometry, capacity, memory budget or admission policy");
    }
}

RegionManager::RegionManager(DeviceFactory factory, FileBasedSecondaryCacheOptions options, CacheStats& stats)
    : m_options(options), m_stats(stats), m_metadata{MetadataLimit(options, options.capacity)}, m_index(m_metadata),
      m_regions(BudgetAllocator<Region>(m_metadata)) {
    Validate(options);
    // Allocate the layout before the factory can touch the filesystem.
    ConfigureLocked(options.capacity);
    m_device = factory(m_metadata);
    m_device->Configure(options.capacity, options.regionSize);
}

void RegionManager::ConfigureLocked(const size_t capacity) {
    const size_t count = capacity / m_options.regionSize + (capacity % m_options.regionSize != 0 ? 1 : 0);
    m_regions.reserve(count);
    for (size_t region = 0; region < count; ++region) {
        m_regions.emplace_back(m_metadata);
    }
    if (capacity != 0) {
        m_buffer.reserve(m_options.flushBlockSize);
    }
    m_capacity = capacity;
}

size_t RegionManager::RegionBudget(const uint32_t region) const noexcept {
    return std::min(m_options.regionSize, m_capacity - size_t{region} * m_options.regionSize);
}

void RegionManager::RemoveLocationLocked(const uint64_t hash) {
    if (const auto removed = m_index.Remove(hash)) {
        assert(m_usage >= LocationLengthBytes(*removed));
        m_usage -= LocationLengthBytes(*removed);
    }
}

void RegionManager::ReclaimLocked(const uint32_t regionId) {
    auto& region = m_regions[regionId];
    if (region.generation == UINT32_MAX) {
        throw std::overflow_error("secondary cache generation exhausted; recreate cache");
    }
    for (const auto hash : region.hashes) {
        const auto location = m_index.Find(hash);
        if (location && location->regionId == regionId && location->generation == region.generation) {
            RemoveLocationLocked(hash);
        }
    }
    if (region.cursor != 0) {
        ++m_stats.reclaims;
    }
    region.hashes.clear();
    region.cursor = 0;
    ++region.generation;
}

bool RegionManager::FlushLocked() {
    if (m_buffer.empty()) {
        return true;
    }
    const size_t offset = m_regions[m_current].cursor - m_buffer.size();
    if (!m_device->Write(m_current, offset, m_buffer)) {
        ++m_stats.ioErrors;
        InvalidateLocked();
        return false;
    }
    m_buffer.clear();
    return true;
}

bool RegionManager::SelectRegionLocked(const size_t length) {
    if (m_current != UINT32_MAX && length <= RegionBudget(m_current) - m_regions[m_current].cursor) {
        return true;
    }
    if (!FlushLocked()) {
        return false;
    }
    const auto count = static_cast<uint32_t>(m_regions.size());
    uint32_t next = m_current == UINT32_MAX ? 0 : (m_current + 1) % count;
    for (uint32_t checked = 0; checked < count; ++checked, next = (next + 1) % count) {
        if (length <= RegionBudget(next)) {
            ReclaimLocked(next);
            m_current = next;
            return true;
        }
    }
    return false;
}

rocksdb::Status RegionManager::Append(const uint64_t hash, const std::span<const std::byte> record,
                                      const size_t payloadSize) {
    std::lock_guard lock(m_mutex);
    if (m_capacity == 0) {
        ++m_stats.droppedDisabled;
        return rocksdb::Status::OK();
    }
    if (record.size() % kRecordAlign != 0) {
        return rocksdb::Status::InvalidArgument("secondary cache record alignment");
    }
    if (record.empty() || record.size() > m_options.maxEntrySize ||
        record.size() > std::min(m_capacity, m_options.regionSize)) {
        ++m_stats.droppedTooLarge;
        return rocksdb::Status::OK();
    }
    if (!SelectRegionLocked(record.size())) {
        return rocksdb::Status::IOError("secondary cache region flush failed");
    }
    if (!m_buffer.empty() && record.size() > m_options.flushBlockSize - m_buffer.size() && !FlushLocked()) {
        return rocksdb::Status::IOError("secondary cache append flush failed");
    }
    auto& region = m_regions[m_current];
    const Location location{m_current, static_cast<uint32_t>(region.cursor / kRecordAlign),
                            static_cast<uint32_t>(record.size() / kRecordAlign), region.generation};
    // Allocate both publication structures before performing I/O. Nothing can
    // observe the tentative index mapping while this transaction owns the lock.
    region.hashes.push_back(hash);
    std::optional<Location> previous;
    try {
        previous = m_index.Upsert(hash, location);
    } catch (...) {
        region.hashes.pop_back();
        throw;
    }
    try {
        if (record.size() > m_options.flushBlockSize) {
            if (!m_device->Write(m_current, region.cursor, record)) {
                ++m_stats.ioErrors;
                InvalidateLocked();
                return rocksdb::Status::IOError("secondary cache direct write failed");
            }
        } else {
            m_buffer.insert(m_buffer.end(), record.begin(), record.end());
        }
    } catch (...) {
        InvalidateLocked();
        throw;
    }
    region.cursor += record.size();
    m_usage += record.size();
    if (previous) {
        assert(m_usage >= LocationLengthBytes(*previous));
        m_usage -= LocationLengthBytes(*previous);
    }
    ++m_stats.admitted;
    m_stats.payloadBytes += payloadSize;
    m_stats.encodedBytes += record.size();
    return rocksdb::Status::OK();
}

RegionManager::ReadStatus RegionManager::Read(const uint64_t hash, RecordBytes& record, Identity& identity) {
    std::lock_guard lock(m_mutex);
    const auto found = m_index.Find(hash);
    if (!found) {
        return ReadStatus::kMiss;
    }
    identity = {*found, m_incarnation};
    const size_t offset = size_t{found->offsetInAlignUnits} * kRecordAlign;
    record.resize(LocationLengthBytes(*found));
    const size_t bufferOffset = m_current == UINT32_MAX ? 0 : m_regions[m_current].cursor - m_buffer.size();
    if (found->regionId == m_current && offset >= bufferOffset) {
        const size_t relative = offset - bufferOffset;
        assert(relative <= m_buffer.size() && record.size() <= m_buffer.size() - relative);
        std::copy_n(m_buffer.data() + relative, record.size(), record.data());
    } else if (!m_device->Read(found->regionId, offset, record)) {
        ++m_stats.ioErrors;
        RemoveLocationLocked(hash);
        return ReadStatus::kIoError;
    }
    return ReadStatus::kOk;
}

bool RegionManager::Remove(const uint64_t hash, const Identity& identity) {
    std::lock_guard lock(m_mutex);
    const auto found = m_index.Find(hash);
    if (identity.incarnation != m_incarnation || !found || *found != identity.location) {
        return false;
    }
    RemoveLocationLocked(hash);
    return true;
}

bool RegionManager::Contains(const uint64_t hash, const Identity& identity) const {
    std::lock_guard lock(m_mutex);
    const auto found = m_index.Find(hash);
    return identity.incarnation == m_incarnation && found && *found == identity.location;
}

void RegionManager::Erase(const uint64_t hash) {
    std::lock_guard lock(m_mutex);
    RemoveLocationLocked(hash);
}

void RegionManager::InvalidateLocked() {
    if (m_incarnation == UINT64_MAX) {
        throw std::overflow_error("secondary cache incarnation exhausted; recreate cache");
    }
    ++m_incarnation;
    m_index.Clear();
    for (auto& region : m_regions) {
        region.hashes.clear();
        region.cursor = 0;
        region.generation = 1;
    }
    m_buffer.clear();
    m_current = UINT32_MAX;
    m_usage = 0;
}

rocksdb::Status RegionManager::SetCapacity(const size_t capacity) {
    auto options = m_options;
    options.capacity = capacity;
    Validate(options); // Invalid requests preserve the old state.
    std::lock_guard lock(m_mutex);
    InvalidateLocked();
    m_capacity = 0;
    decltype(m_regions)(m_regions.get_allocator()).swap(m_regions);
    std::vector<std::byte>().swap(m_buffer);
    // Device layout metadata is released before changing its allocator's limit.
    m_device->Configure(0, m_options.regionSize);
    m_metadata.limit = MetadataLimit(options, capacity);
    try {
        ConfigureLocked(capacity);
        m_device->Configure(capacity, m_options.regionSize);
    } catch (...) {
        m_capacity = 0;
        decltype(m_regions)(m_regions.get_allocator()).swap(m_regions);
        std::vector<std::byte>().swap(m_buffer);
        throw;
    }
    return rocksdb::Status::OK();
}

FileBasedSecondaryCacheStats RegionManager::Snapshot() const {
    std::lock_guard lock(m_mutex);
    FileBasedSecondaryCacheStats snapshot;
    snapshot.liveEntries = m_index.Size();
    snapshot.liveBytes = m_usage;
    snapshot.capacityBytes = m_capacity;
    snapshot.fileBytes = m_device->FileBytes();
    snapshot.metadataBytes = m_metadata.used.load(std::memory_order_relaxed);
    snapshot.metadataLimit = m_metadata.limit;
    snapshot.activeBufferBytes = m_buffer.capacity();
    snapshot.openHandles = m_device->OpenHandles();
    snapshot.deviceWriteBytes = m_device->bytesWritten;
    snapshot.deviceReadBytes = m_device->bytesRead;
#define COPY_COUNTER(name) snapshot.name = m_stats.name.load(std::memory_order_relaxed)
    COPY_COUNTER(inserts);
    COPY_COUNTER(admitted);
    COPY_COUNTER(droppedTooLarge);
    COPY_COUNTER(droppedPressure);
    COPY_COUNTER(droppedDisabled);
    COPY_COUNTER(lookups);
    COPY_COUNTER(hits);
    COPY_COUNTER(corruptions);
    COPY_COUNTER(keyMismatches);
    COPY_COUNTER(ioErrors);
    COPY_COUNTER(errors);
    COPY_COUNTER(reclaims);
    COPY_COUNTER(payloadBytes);
    COPY_COUNTER(encodedBytes);
#undef COPY_COUNTER
    return snapshot;
}

size_t RegionManager::CapacityBytes() const {
    std::lock_guard lock(m_mutex);
    return m_capacity;
}

size_t RegionManager::UsageBytes() const {
    std::lock_guard lock(m_mutex);
    return m_usage;
}
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
