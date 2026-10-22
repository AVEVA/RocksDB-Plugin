// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <boost/intrusive/list.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core {
namespace detail {
// One open handle on a cached file, shared by every reader of that file so a read does not pay for an open.
// File::Read is not thread-safe (it seeks, then reads), so readers take Mutex around the whole access.
// Close() marks the handle unusable for good: readers that still hold it fall back to the blob instead of
// reopening a file that is being evicted, replaced or deleted.
class CachedReadHandle {
  public:
    std::mutex Mutex;
    std::unique_ptr<File> Opened;
    bool Closed = false;

    void Close() {
        std::scoped_lock lock(Mutex);
        Closed = true;
        Opened.reset();
    }
};
} // namespace detail

class FileCacheEntry
    : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
  public:
    // Links the entry into the cache's list of metadata-only stale entries (stale, nothing on disk) while it is one,
    // so the oldest can be pruned without scanning the LRU list.
    using MetadataOnlyHook =
        boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    MetadataOnlyHook MetadataOnly;
    // Set only for an entry recorded on first access with no download ever queued for it and nothing of that name
    // possibly on disk; pruning it then skips the delete. Guarded by the owning cache's mutex.
    bool KnownNotOnDisk = false;

    enum class State {
        Stale,
        QueuedForDownload,
        Downloading,
        Active,
    };

  private:
    State m_state;
    std::string m_filePath;
    int64_t m_size;
    uint64_t m_generation = 0;
    std::shared_ptr<detail::CachedReadHandle> m_readHandle;

  public:
    FileCacheEntry(std::string_view filePath, int64_t size);

    int64_t GetSize() const noexcept;
    const std::string& GetFilePath() const noexcept;
    State GetState() const noexcept;

    void SetSize(int64_t size) noexcept;
    void SetState(State state) noexcept;

    // Bumped every time the blob is replaced, whatever state the entry is in, so a download that began
    // before the replacement can tell its bytes are out of date.
    uint64_t GetGeneration() const noexcept;
    void BumpGeneration() noexcept;
    void SetGeneration(uint64_t generation) noexcept;

    // The handle readers share; created on first use. Callers must hold the owning cache's mutex.
    std::shared_ptr<detail::CachedReadHandle> GetOrCreateReadHandle();
    // Detaches the handle (null if none) so the owner can close it outside its mutex.
    std::shared_ptr<detail::CachedReadHandle> TakeReadHandle() noexcept;
    bool HasReadHandle() const noexcept;
    // True if this entry still owns exactly this handle, i.e. it is the entry the caller read from.
    bool HasReadHandle(const std::shared_ptr<detail::CachedReadHandle>& handle) const noexcept;

  private:
    // Intrusive-hook operations; only the owning cache, which holds its mutex, may relink an entry.
    friend class FileCache;
    void unlink();
    bool is_linked() const;
};
} // namespace AVEVA::RocksDB::Plugin::Core
