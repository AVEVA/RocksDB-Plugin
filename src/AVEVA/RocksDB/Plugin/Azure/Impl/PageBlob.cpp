// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include <azure/core/etag.hpp>
#include <azure/core/context.hpp>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    PageBlob::PageBlob(::Azure::Storage::Blobs::PageBlobClient client)
        : m_client(std::move(client))
    {
    }

    int64_t PageBlob::GetSize()
    {
        return BlobHelpers::GetFileSize(m_client);
    }

    void PageBlob::SetSize(int64_t size)
    {
        BlobHelpers::SetFileSize(m_client, size);
    }

    int64_t PageBlob::GetCapacity()
    {
        return BlobHelpers::GetBlobCapacity(m_client);
    }

    void PageBlob::SetCapacity(int64_t capacity)
    {
        m_client.Resize(capacity);
    }

    void PageBlob::DownloadTo(const std::string& path, int64_t offset, int64_t length)
    {
        ::Azure::Storage::Blobs::DownloadBlobToOptions options;
        options.Range = ::Azure::Core::Http::HttpRange(offset, length);
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

    ::Azure::ETag PageBlob::GetEtag()
    {
        const auto properties = m_client.GetProperties();
        return properties.Value.ETag;
    }

    int64_t PageBlob::Download(std::span<char> buffer, int64_t offset, int64_t length, const ::Azure::ETag& ifMatch)
    {
        ::Azure::Storage::Blobs::DownloadBlobOptions options
        {
          .Range = ::Azure::Core::Http::HttpRange { offset, length }
        };
        options.AccessConditions.IfMatch = ifMatch;
        const auto result = m_client.Download(options, ::Azure::Core::Context{});
        const auto& content = result.Value;

        auto bytesRead = content.BodyStream->ReadToCount(reinterpret_cast<uint8_t*>(buffer.data()), buffer.size());

        assert(static_cast<int>(content.ContentRange.Length.ValueOr(-1)) == bytesRead && "Bytes read differ from server ContentRange");
        return bytesRead;
    }
}
