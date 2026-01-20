// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include <cassert>
#include <azure/core/exception.hpp>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    ReadableFileImpl::ReadableFileImpl(std::string_view name,
        std::shared_ptr<Core::BlobClient> blobClient,
        std::shared_ptr<Core::FileCache> fileCache)
        : m_name(name),
        m_blobClient(std::move(blobClient)),
        m_fileCache(std::move(fileCache)),
        m_offset(0),
        m_size(m_blobClient ? m_blobClient->GetSize() : 0LL)
    {
        m_etag = m_blobClient->GetEtag();
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

        assert(m_size >= m_offset && "m_size needs to be bigger than m_offset or else we will overflow");

        auto bytesRead = DownloadWithRetry(m_offset, bytesToRead, buffer);
        bytesRead = bytesRead > 0 ? bytesRead : 0;

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

        auto bytesRead = DownloadWithRetry(offset, bytesToRead, buffer);
        bytesRead = bytesRead > 0 ? bytesRead : 0;

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

    int64_t ReadableFileImpl::DownloadWithRetry(const int64_t offset, const int64_t bytesToRead, char* buffer) const
    {
        int64_t bytesRead = 0;

        bool success = false;
        do
        {
            auto remaining = std::max<int64_t>(0, m_size - offset);
            if (remaining == 0)
            {
                auto latestEtag = m_blobClient->GetEtag();
                if (latestEtag != m_etag)
                {
                    RefreshBlobMetadata();
                    continue;
                }

                return 0;
            }

            auto toRead = std::min(bytesToRead, remaining);
            try
            {
                bytesRead = m_blobClient->Download(std::span<char>(buffer, static_cast<size_t>(toRead)), offset, toRead, m_etag);
                bytesRead = std::min(bytesRead, remaining);
                success = true;
            }
            catch (const ::Azure::Core::RequestFailedException& ex)
            {
                if (ex.StatusCode == ::Azure::Core::Http::HttpStatusCode::PreconditionFailed)
                {
                    RefreshBlobMetadata();
                }
                else
                {
                    throw;
                }
            }
        } while (!success);

        return bytesRead;
    }

    void ReadableFileImpl::RefreshBlobMetadata() const
    {
        m_size = m_blobClient->GetSize();
        m_etag = m_blobClient->GetEtag();
    }
}
