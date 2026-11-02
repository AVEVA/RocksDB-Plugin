// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"
#include <boost/intrusive/list.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core {
namespace detail {
enum class HandleState : std::uint8_t {
    Open,
    Closed,
};

// One open handle on a cached file, shared by every reader of that file so a read does not pay for an open.
// File::Read must be safe to call concurrently (positional I/O), so Mutex only guards Opened/State and is not
// held during a read; readers keep the file alive through their own shared_ptr.
// Close() marks the handle unusable for good: readers that still hold it fall back to the blob instead of
// reopening a file that is being evicted, replaced or deleted.
class CachedReadHandle {
  public:
    std::mutex Mutex;
    std::shared_ptr<File> Opened;
    HandleState State = HandleState::Open;

    void Close() {
        std::scoped_lock lock(Mutex);
        State = HandleState::Closed;
        Opened.reset();
    }
};
} // namespace detail

/// <summary>Whether an entry still holds the read handle the caller used.</summary>
enum class HandleOwnership : std::uint8_t {
    NotOwned,
    Owned,
};

/// <summary>Whether nothing of an entry's name can be on disk, so removing the entry needs no delete.</summary>
enum class DiskPresence : std::uint8_t {
    PossiblyOnDisk,
    KnownAbsent,
};

/// <summary>Whether an entry is currently in the cache's LRU list.</summary>
enum class ListLink : std::uint8_t {
    Unlinked,
    Linked,
};

class FileCacheEntry
    : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
  public:
    enum class State {
        Stale,
        QueuedForDownload,
        Downloading,
        Active,
    };

  private:
    // Links the entry into the cache's list of metadata-only stale entries (stale, nothing on disk) while it is one,
    // so the oldest can be pruned without scanning the LRU list.
    using MetadataOnlyHook =
        boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    MetadataOnlyHook MetadataOnly;
    // Set only for an entry recorded on first access with no download ever queued for it and nothing of that name
    // possibly on disk; pruning it then skips the delete. Guarded by the owning cache's mutex.
    DiskPresence Disk = DiskPresence::PossiblyOnDisk;
    // The cache's access epoch when this entry was last moved to the front of the LRU list. Guarded by the owning
    // cache's mutex.
    AccessEpoch LastRelinkEpoch;

    State m_state;
    std::string m_filePath;
    ByteCount m_size;
    Generation m_generation;
    std::shared_ptr<detail::CachedReadHandle> m_readHandle;

  public:
    FileCacheEntry(std::string_view filePath, ByteCount size);

    ByteCount GetSize() const noexcept;
    [[nodiscard]] std::string GetFilePath() const;
    State GetState() const noexcept;

    void SetSize(ByteCount size) noexcept;
    void SetState(State state) noexcept;

    // Changed every time the blob is replaced, whatever state the entry is in, so a download that began
    // before the replacement can tell its bytes are out of date.
    Generation GetGeneration() const noexcept;
    void SetGeneration(Generation generation) noexcept;

    // The handle readers share; created on first use. Callers must hold the owning cache's mutex.
    std::shared_ptr<detail::CachedReadHandle> GetOrCreateReadHandle();
    // Detaches the handle (null if none) so the owner can close it outside its mutex.
    std::shared_ptr<detail::CachedReadHandle> TakeReadHandle() noexcept;
    // Owned if this entry still owns exactly this handle, i.e. it is the entry the caller read from.
    HandleOwnership OwnsReadHandle(const std::shared_ptr<detail::CachedReadHandle>& handle) const noexcept;

  private:
    // Intrusive-hook operations; only the owning cache, which holds its mutex, may relink an entry.
    friend class FileCache;
    void unlink();
    ListLink LinkState() const;
};
} // namespace AVEVA::RocksDB::Plugin::Core
