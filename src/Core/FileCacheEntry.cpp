// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
namespace AVEVA::RocksDB::Plugin::Core {
// New entries start as stale. The first access records the file in cache
// metadata, and a subsequent access queues it for background download.
FileCacheEntry::FileCacheEntry(const std::string_view filePath, const int64_t size)
    : m_state(State::Stale), m_filePath(filePath), m_size(size) {}

int64_t FileCacheEntry::GetSize() const noexcept { return m_size; }

const std::string& FileCacheEntry::GetFilePath() const noexcept { return m_filePath; }

FileCacheEntry::State FileCacheEntry::GetState() const noexcept { return m_state; }

void FileCacheEntry::SetSize(int64_t size) noexcept { m_size = size; }

void FileCacheEntry::SetState(State state) noexcept { m_state = state; }

uint64_t FileCacheEntry::GetGeneration() const noexcept { return m_generation; }

void FileCacheEntry::BumpGeneration() noexcept { ++m_generation; }

std::shared_ptr<CachedReadHandle> FileCacheEntry::GetOrCreateReadHandle() {
    if (!m_readHandle) {
        m_readHandle = std::make_shared<CachedReadHandle>();
    }
    return m_readHandle;
}

std::shared_ptr<CachedReadHandle> FileCacheEntry::TakeReadHandle() noexcept { return std::move(m_readHandle); }

void FileCacheEntry::unlink() {
    boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::unlink();
}

bool FileCacheEntry::is_linked() {
    return boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::is_linked();
}
} // namespace AVEVA::RocksDB::Plugin::Core
