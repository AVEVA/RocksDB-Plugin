// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include <azure/storage/blobs/page_blob_client.hpp>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class PageBlob final : public Core::BlobClient
    {
        ::Azure::Storage::Blobs::PageBlobClient m_client;

    public:
        explicit PageBlob(::Azure::Storage::Blobs::PageBlobClient client);

        virtual int64_t GetSize() override;
        virtual void SetSize(int64_t size) override;
        virtual int64_t GetCapacity() override;
        virtual void SetCapacity(int64_t capacity) override;
        virtual void DownloadTo(const std::string& path, int64_t offset, int64_t length) override;
        virtual int64_t DownloadTo(std::span<char> buffer, int64_t blobOffset, int64_t readLength) override;
        virtual void UploadPages(const std::span<char> buffer, int64_t blobOffset) override;
    };
}
