// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"

#include <boost/asio/use_future.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
// Chunking used when downloading a range of a blob into a local file.
const constexpr std::size_t g_downloadChunkSize = static_cast<std::size_t>(4) * 1024 * 1024;
const constexpr std::size_t g_downloadConcurrency = 4;

AzureClient::Models::BlobByteRange ToRange(int64_t offset, int64_t length) {
    return AzureClient::Models::BlobByteRange{static_cast<uint64_t>(offset), static_cast<uint64_t>(length)};
}
} // namespace

PageBlob::PageBlob(std::shared_ptr<ClientRuntime> runtime, AzureClient::PageBlobClient client)
    : m_runtime(std::move(runtime)), m_client(std::move(client)) {}

int64_t PageBlob::GetSize() { return BlobHelpers::GetFileSize(m_client); }

void PageBlob::SetSize(int64_t size) { BlobHelpers::SetFileSize(m_client, size); }

int64_t PageBlob::GetCapacity() { return BlobHelpers::GetBlobCapacity(m_client); }

void PageBlob::SetCapacity(int64_t capacity) {
    Unwrap(
        m_client
            .ResizeAsync(static_cast<uint64_t>(capacity), AzureClient::ResizePageBlobOptions{}, boost::asio::use_future)
            .get());
}

void PageBlob::DownloadTo(const std::string& path, int64_t offset, int64_t length) {
    AzureClient::DownloadToOptions options;
    options.Range = ToRange(offset, length);
    options.ChunkSize = g_downloadChunkSize;
    options.Concurrency = g_downloadConcurrency;
    Unwrap(m_client.DownloadToAsync(std::filesystem::path(path), std::move(options), boost::asio::use_future).get());
}

int64_t PageBlob::DownloadTo(std::span<char> buffer, int64_t offset, int64_t length) {
    AzureClient::DownloadBlobOptions options;
    options.Range = ToRange(offset, length);
    auto result = Unwrap(m_client
                             .DownloadAsync(std::move(options), boost::asio::use_future,
                                            RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(),
                                                                      static_cast<uint64_t>(length)))
                             .get());

    if (!result.ContentRange.has_value() || !result.ContentRange->Length.has_value()) {
        return -1;
    }

    const auto bytes = std::min(result.Content.size(), buffer.size());
    std::memcpy(buffer.data(), result.Content.data(), bytes);
    return static_cast<int64_t>(*result.ContentRange->Length);
}

void PageBlob::UploadPages(const std::span<char> buffer, const int64_t blobOffset) {
    Unwrap(m_client
               .UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::as_bytes(buffer), boost::asio::use_future,
                                 RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(), buffer.size()))
               .get());
}

std::string PageBlob::GetEtag() {
    auto properties = Unwrap(m_client.GetPropertiesAsync(boost::asio::use_future).get());
    return std::move(properties.ETag);
}

void PageBlob::DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                             DownloadCallback callback) {
    AzureClient::DownloadBlobOptions options;
    options.Range = ToRange(blobOffset, readLength);
    options.Conditions.IfMatch = ifMatch;
    m_client.DownloadAsync(
        std::move(options),
        [callback = std::move(callback)](auto result) mutable {
            std::exception_ptr error;
            std::string content;
            try {
                content = std::move(Unwrap(std::move(result)).Content);
            } catch (...) {
                error = std::current_exception();
            }
            callback(error, std::move(content));
        },
        RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(), static_cast<uint64_t>(readLength)));
}

void PageBlob::GetMetadataAsync(MetadataCallback callback) {
    m_client.GetPropertiesAsync([callback = std::move(callback)](auto result) mutable {
        std::exception_ptr error;
        int64_t size = 0;
        std::string etag;
        try {
            auto properties = Unwrap(std::move(result));
            size = BlobHelpers::FileSizeFromProperties(properties);
            etag = std::move(properties.ETag);
        } catch (...) {
            error = std::current_exception();
        }
        callback(error, size, std::move(etag));
    });
}

int64_t PageBlob::Download(std::span<char> buffer, int64_t offset, int64_t length, const std::string& ifMatch) {
    AzureClient::DownloadBlobOptions options;
    options.Range = ToRange(offset, length);
    options.Conditions.IfMatch = ifMatch;
    const auto result = Unwrap(m_client
                                   .DownloadAsync(std::move(options), boost::asio::use_future,
                                                  RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(),
                                                                            static_cast<uint64_t>(length)))
                                   .get());

    const auto bytesRead = std::min(result.Content.size(), buffer.size());
    std::memcpy(buffer.data(), result.Content.data(), bytesRead);

    assert((!result.ContentRange.has_value() || !result.ContentRange->Length.has_value() ||
            *result.ContentRange->Length == static_cast<uint64_t>(bytesRead)) &&
           "Bytes read differ from server ContentRange");
    return static_cast<int64_t>(bytesRead);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
