// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/intrusive/list.hpp>
#include <chrono>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core
{
    class FileCacheEntry : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
    {
    public:
        enum class State
        {
            Stale,
            QueuedForDownload,
            Downloading,
            Active,
        };

    private:
        State m_state;
        std::string m_filePath;
        std::size_t m_size;
        std::chrono::time_point<std::chrono::system_clock> m_lastAccessTime;

    public:
        FileCacheEntry(std::string_view filePath, std::size_t size);
        void Accessed();

        std::size_t GetSize() const noexcept;
        const std::string& GetFilePath() const noexcept;
        State GetState() const noexcept;

        void SetSize(std::size_t size) noexcept;
        void SetState(State state) noexcept;

        void unlink();
        bool is_linked();
    };
}
