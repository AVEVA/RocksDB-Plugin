// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"

#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <memory>
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class PageBlob final : public Core::BlobClient {
    // Declared first so that it is destroyed after the client that references its HTTP client.
    std::shared_ptr<ClientRuntime> m_runtime;
    AzureClient::PageBlobClient m_client;

  public:
    PageBlob(std::shared_ptr<ClientRuntime> runtime, AzureClient::PageBlobClient client);

    virtual int64_t GetSize() override;
    virtual void SetSize(int64_t size) override;
    virtual int64_t GetCapacity() override;
    virtual void SetCapacity(int64_t capacity) override;
    virtual void DownloadTo(const std::string& path, int64_t offset, int64_t length) override;
    virtual int64_t DownloadTo(std::span<char> buffer, int64_t blobOffset, int64_t readLength) override;
    virtual int64_t Download(std::span<char> buffer, int64_t blobOffset, int64_t readLength,
                             const std::string& ifMatch) override;
    virtual void UploadPages(const std::span<char> buffer, int64_t blobOffset) override;
    virtual std::string GetEtag() override;
    virtual Core::BlobMetadata GetMetadata() override;
    // Fully asynchronous: completions run on the injected io_context's threads and never block on it.
    virtual void DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                               DownloadCallback callback) override;
    virtual void DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                               std::chrono::milliseconds timeout, DownloadCallback callback) override;
    virtual void GetMetadataAsync(MetadataCallback callback) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
