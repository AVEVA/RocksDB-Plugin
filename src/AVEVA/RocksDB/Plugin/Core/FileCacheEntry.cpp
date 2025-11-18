// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
namespace AVEVA::RocksDB::Plugin::Core
{
    // NOTE: the state of the file cache entry should default to QueuedForDownload.
    // This way, there is no confusion if another read request is made before
    // the file has finished downloading and we can safely return nothing without
    // queuing up another download.
    FileCacheEntry::FileCacheEntry(const std::string_view filePath, const int64_t size)
        : m_state(State::QueuedForDownload), m_filePath(std::move(filePath)), m_size(size)
    {
    }

    void FileCacheEntry::Accessed()
    {
    }

    int64_t FileCacheEntry::GetSize() const noexcept
    {
        return m_size;
    }

    const std::string& FileCacheEntry::GetFilePath() const noexcept
    {
        return m_filePath;
    }

    FileCacheEntry::State FileCacheEntry::GetState() const noexcept
    {
        return m_state;
    }

    void FileCacheEntry::SetSize(int64_t size) noexcept
    {
        m_size = size;
    }

    void FileCacheEntry::SetState(State state) noexcept
    {
        m_state = state;
    }

    void FileCacheEntry::unlink()
    {
        boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::unlink();
    }

    bool FileCacheEntry::is_linked()
    {
        return boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::is_linked();
    }
}
