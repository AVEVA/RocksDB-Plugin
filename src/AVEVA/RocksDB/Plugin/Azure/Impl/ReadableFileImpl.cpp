// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include <cassert>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    ReadableFileImpl::ReadableFileImpl(std::string_view name,
        std::shared_ptr<Core::BlobClient> blobClient,
        std::shared_ptr<Core::FileCache> fileCache,
        bool isSecondary)
        : m_name(name),
        m_blobClient(std::move(blobClient)),
        m_fileCache(std::move(fileCache)),
        m_offset(0),
        m_size(m_blobClient ? m_blobClient->GetSize() : 0LL),
        m_isSecondary(isSecondary)
    {
    }

    int64_t ReadableFileImpl::SequentialRead(const int64_t bytesToRead, char* buffer)
    {
        if (bytesToRead <= 0)
        {
            return 0;
        }

        if (m_fileCache)
        {
            const auto bytesRead = m_fileCache->ReadFile(m_name, m_offset, bytesToRead, buffer);
            if (bytesRead)
            {
                m_offset += static_cast<int64_t>(*bytesRead);
                return static_cast<int64_t>(*bytesRead);
            }
        }
        
        if (m_offset >= m_size)
        {
            if (IsLogFile(GetFileType(m_name)) && m_isSecondary)
            {
                m_size = GetFileSize(*m_pbClient);
            }

            if (m_offset >= m_size)
            {
                m_offset = m_size;
                return 0;
            }
        }

        int64_t bytesRead = 0;
        assert(m_size >= m_offset && "m_size needs to be bigger than m_offset or else we will overflow");
        int64_t bytesRequested = m_size - m_offset;
        if (bytesRequested > bytesToRead) bytesRequested = bytesToRead;
        if (bytesRequested <= 0)
        {
            return 0;
        }

        const auto result = m_blobClient->DownloadTo(std::span<char>(buffer, static_cast<std::size_t>(bytesRequested)), m_offset, bytesRequested);
        bytesRead = result > 0 ? result : 0;

        m_offset += bytesRead;
        return bytesRead;
    }

    int64_t ReadableFileImpl::RandomRead(const int64_t offset, const int64_t bytesToRead, char* buffer) const
    {
        if (offset < 0 || bytesToRead <= 0)
        {
            return 0;
        }

        if (m_fileCache)
        {
            const auto bytesRead = m_fileCache->ReadFile(m_name, offset, bytesToRead, buffer);
            if (bytesRead)
            {
                return static_cast<int64_t>(*bytesRead);
            }
        }

        int64_t bytesRead = 0;

        assert(m_size >= offset && "m_size needs to be bigger than or equal to offset or else we will overflow");
        int64_t bytesRequested = m_size - offset;
        if (bytesRequested > bytesToRead) bytesRequested = bytesToRead;
        if (bytesRequested <= 0)
        {
            return 0;
        }

        const auto result = m_blobClient->DownloadTo(std::span<char>(buffer, static_cast<std::size_t>(bytesRequested)), offset, bytesRequested);
        bytesRead = result > 0 ? result : 0;

        return bytesRead;
    }

    int64_t ReadableFileImpl::GetOffset() const
    {
        return m_offset;
    }

    void ReadableFileImpl::Skip(const int64_t n)
    {
        m_offset += n;
    }

    int64_t ReadableFileImpl::GetSize() const
    {
        return m_size;
    }
}
