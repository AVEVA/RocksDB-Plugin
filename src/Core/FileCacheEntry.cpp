// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
namespace AVEVA::RocksDB::Plugin::Core {
// New entries start as stale. The first access records the file in cache
// metadata, and a subsequent access queues it for background download.
FileCacheEntry::FileCacheEntry(const std::string_view filePath, const ByteCount size)
    : m_state(State::Stale), m_filePath(filePath), m_size(size) {}

ByteCount FileCacheEntry::GetSize() const noexcept { return m_size; }

std::string FileCacheEntry::GetFilePath() const { return m_filePath; }

FileCacheEntry::State FileCacheEntry::GetState() const noexcept { return m_state; }

void FileCacheEntry::SetSize(ByteCount size) noexcept { m_size = size; }

void FileCacheEntry::SetState(State state) noexcept { m_state = state; }

Generation FileCacheEntry::GetGeneration() const noexcept { return m_generation; }

void FileCacheEntry::SetGeneration(Generation generation) noexcept { m_generation = generation; }

std::shared_ptr<detail::CachedReadHandle> FileCacheEntry::GetOrCreateReadHandle() {
    if (!m_readHandle) {
        m_readHandle = std::make_shared<detail::CachedReadHandle>();
    }
    return m_readHandle;
}

std::shared_ptr<detail::CachedReadHandle> FileCacheEntry::TakeReadHandle() noexcept { return std::move(m_readHandle); }

HandleOwnership FileCacheEntry::OwnsReadHandle(const std::shared_ptr<detail::CachedReadHandle>& handle) const noexcept {
    return m_readHandle == handle ? HandleOwnership::Owned : HandleOwnership::NotOwned;
}

void FileCacheEntry::unlink() {
    boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::unlink();
}

ListLink FileCacheEntry::LinkState() const {
    return boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::is_linked()
               ? ListLink::Linked
               : ListLink::Unlinked;
}
} // namespace AVEVA::RocksDB::Plugin::Core
