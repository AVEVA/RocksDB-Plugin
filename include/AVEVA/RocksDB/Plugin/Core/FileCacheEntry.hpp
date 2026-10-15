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
    State m_state;
    std::string m_filePath;
    int64_t m_size;
    uint64_t m_generation = 0;
    std::shared_ptr<CachedReadHandle> m_readHandle;

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

    // The handle readers share; created on first use. Callers must hold the owning cache's mutex.
    std::shared_ptr<CachedReadHandle> GetOrCreateReadHandle();
    // Detaches the handle (null if none) so the owner can close it outside its mutex.
    std::shared_ptr<CachedReadHandle> TakeReadHandle() noexcept;

    void unlink();
    bool is_linked();
};
} // namespace AVEVA::RocksDB::Plugin::Core
