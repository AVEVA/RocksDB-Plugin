// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    PageBlob::PageBlob(::Azure::Storage::Blobs::PageBlobClient client)
        : m_client(std::move(client))
    {
    }

    uint64_t PageBlob::GetSize()
    {
        return BlobHelpers::GetFileSize(m_client);
    }

    void PageBlob::SetSize(int64_t size)
    {
        BlobHelpers::SetFileSize(m_client, static_cast<uint64_t>(size));
    }

    uint64_t PageBlob::GetCapacity()
    {
        return BlobHelpers::GetBlobCapacity(m_client);
    }

    void PageBlob::SetCapacity(int64_t capacity)
    {
        m_client.Resize(capacity);
    }

    void PageBlob::DownloadTo(const std::string& path, uint64_t offset, uint64_t length)
    {
        ::Azure::Storage::Blobs::DownloadBlobToOptions options;
        options.Range = ::Azure::Core::Http::HttpRange(static_cast<int64_t>(offset), static_cast<int64_t>(length));
        m_client.DownloadTo(path, options);
    }

    int64_t PageBlob::DownloadTo(std::span<char> buffer, int64_t offset, int64_t length)
    {
        ::Azure::Storage::Blobs::DownloadBlobToOptions options
        {
            .Range = ::Azure::Core::Http::HttpRange { offset, length }
        };

        const auto result = m_client.DownloadTo(reinterpret_cast<uint8_t*>(buffer.data()), buffer.size(), options);
        const auto& downloadedLength = result.Value.ContentRange.Length;
        return downloadedLength.ValueOr(-1);
    }

    void PageBlob::UploadPages(const std::span<char> buffer, const int64_t blobOffset)
    {
        ::Azure::Core::IO::MemoryBodyStream dataStream(reinterpret_cast<uint8_t*>(buffer.data()), buffer.size());
        m_client.UploadPages(blobOffset, dataStream);
    }
}
