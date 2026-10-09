// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// An in-memory page blob that replaces the type-erased operation entry points of AzureClient::PageBlobClient, so
// the plugin's file classes run their real code paths (size metadata, capacity, ETag preconditions, page uploads)
// without any HTTP traffic. Public members describe the blob and record the requests it received.
class FakePageBlobClient : public AzureClient::PageBlobClient {
  public:
    enum class Operation {
        GetProperties,
        SetMetadata,
        Resize,
        UploadPages,
        DownloadToBuffer,
        Download,
        DownloadToFile,
    };

    struct Upload {
        int64_t Offset = 0;
        std::vector<char> Data;
    };
    struct DownloadRequest {
        int64_t Offset = 0;
        int64_t Length = 0;
        std::string IfMatch;
        std::chrono::milliseconds Timeout{};
    };

    explicit FakePageBlobClient(
        AzureClient::Tests::FakeHttpClient& httpClient,
        const AzureClient::BlobClientOptions& options = AzureClient::Tests::MakeBlobClientOptions("files", "test.sst"))
        : AzureClient::PageBlobClient(httpClient, options) {}

    // The blob: its bytes (whose length is the capacity), the size recorded in its metadata, and its ETag.
    std::vector<char> Data;
    int64_t Size = 0;
    std::string ETag = "\"etag\"";

    // Requests received, in order. Uploads are recorded when they start, whether or not they are deferred.
    std::vector<Upload> Uploads;
    std::vector<const char*> SharedUploadPayloadAddresses;
    std::vector<int64_t> SizeWrites;
    std::vector<int64_t> CapacityWrites;
    std::vector<DownloadRequest> Downloads;
    int PropertiesRequests = 0;

    // Returns an error to fail the operation with, or nullopt to let it succeed.
    std::function<std::optional<AzureClient::BlobStorageError>(Operation)> Fault;
    // Replaces the contents of a download; receives the request and returns the bytes to serve.
    std::function<std::string(const DownloadRequest&)> DownloadOverride;
    // Holds upload / DownloadAsync completions until the test releases them.
    bool DeferUploads = false;
    bool DeferDownloads = false;

    [[nodiscard]] static AzureClient::BlobStorageError Error(unsigned int statusCode, std::string errorCode) {
        AzureClient::BlobStorageError error;
        error.StatusCode = statusCode;
        error.ErrorCode = std::move(errorCode);
        error.Message = error.ErrorCode;
        return error;
    }

    void SetCapacity(const int64_t capacity) { Data.resize(static_cast<size_t>(capacity), '\0'); }

    // Applies the writes of every upload received so far that a test chose to defer and has now released.
    size_t PendingUploads() {
        std::scoped_lock lock(m_mutex);
        return m_pendingUploads.size();
    }
    size_t PendingDownloads() {
        std::scoped_lock lock(m_mutex);
        return m_pendingDownloads.size();
    }

    // Completes the oldest deferred upload (the bytes land in the blob only now).
    void CompleteUpload(const size_t index = 0) {
        auto pending = TakePendingUpload(index);
        Apply(pending.Offset, pending.Content);
        pending.Completion(Response<AzureClient::Models::UploadPagesResult>({}, HttpResponse{201, {}, {}}));
    }
    void FailUpload(const AzureClient::BlobStorageError& error, const size_t index = 0) {
        auto pending = TakePendingUpload(index);
        pending.Completion(std::unexpected(error));
    }

    // Takes the oldest deferred DownloadAsync; the caller completes it with Succeed / Fail.
    struct HeldDownload {
        DownloadRequest Request;
        std::move_only_function<void(std::string content)> Succeed;
        std::move_only_function<void(AzureClient::BlobStorageError error)> Fail;
    };
    HeldDownload TakeDownload() {
        std::scoped_lock lock(m_mutex);
        auto pending = std::move(m_pendingDownloads.front());
        m_pendingDownloads.pop_front();
        auto shared = std::make_shared<DownloadCompletionHandler>(std::move(pending.Completion));
        return HeldDownload{
            pending.Request,
            [shared](std::string content) {
                AzureClient::Models::DownloadBlobResult result;
                result.Content = std::move(content);
                (*shared)(AzureClient::Response<AzureClient::Models::DownloadBlobResult>(std::move(result),
                                                                                         HttpResponse{206, {}, {}}));
            },
            [shared](AzureClient::BlobStorageError error) { (*shared)(std::unexpected(std::move(error))); }};
    }

  private:
    template <class T> using Response = AzureClient::Response<T>;
    struct PendingUpload {
        int64_t Offset = 0;
        std::vector<char> Content;
        UploadPagesCompletionHandler Completion;
    };
    struct PendingDownloadEntry {
        DownloadRequest Request;
        DownloadCompletionHandler Completion;
    };

    std::mutex m_mutex;
    std::deque<PendingUpload> m_pendingUploads;
    std::deque<PendingDownloadEntry> m_pendingDownloads;

    PendingUpload TakePendingUpload(const size_t index) {
        std::scoped_lock lock(m_mutex);
        if (index >= m_pendingUploads.size()) {
            throw std::logic_error("FakePageBlobClient upload index out of range");
        }
        auto it = m_pendingUploads.begin() + static_cast<std::ptrdiff_t>(index);
        auto pending = std::move(*it);
        m_pendingUploads.erase(it);
        return pending;
    }

    std::optional<AzureClient::BlobStorageError> FaultFor(const Operation operation) const {
        return Fault ? Fault(operation) : std::nullopt;
    }

    void Apply(const int64_t offset, const std::vector<char>& content) {
        std::scoped_lock lock(m_mutex);
        const auto end = static_cast<size_t>(offset) + content.size();
        if (end > Data.size()) {
            Data.resize(end, '\0');
        }
        std::copy(content.begin(), content.end(), Data.begin() + offset);
        ETag = "\"etag-" + std::to_string(++m_version) + "\"";
    }

    // Bytes the service would return for the range, or an error for a failed precondition or range.
    std::expected<std::string, AzureClient::BlobStorageError> Read(const DownloadRequest& request) {
        std::scoped_lock lock(m_mutex);
        Downloads.push_back(request);
        if (DownloadOverride) {
            return DownloadOverride(request);
        }
        if (!request.IfMatch.empty() && request.IfMatch != ETag) {
            return std::unexpected(Error(412, "ConditionNotMet"));
        }
        if (static_cast<size_t>(request.Offset) >= Data.size()) {
            return std::unexpected(Error(416, "InvalidRange"));
        }
        const auto available = std::min(static_cast<size_t>(request.Length), Data.size() - request.Offset);
        return std::string(Data.data() + request.Offset, available);
    }

    DownloadRequest ToRequest(const AzureClient::DownloadBlobOptions& options,
                              const AVEVA::HttpRequestOptions& requestOptions) const {
        DownloadRequest request;
        if (options.Range) {
            request.Offset = static_cast<int64_t>(options.Range->Offset);
            request.Length = static_cast<int64_t>(options.Range->Length.value_or(0));
        }
        request.IfMatch = options.Conditions.IfMatch;
        request.Timeout = requestOptions.GetTimeout();
        return request;
    }

    void GetPropertiesAsyncImpl(const AzureClient::GetBlobPropertiesOptions&, GetPropertiesCompletionHandler completion,
                                AVEVA::HttpRequestOptions) override {
        if (const auto fault = FaultFor(Operation::GetProperties)) {
            completion(std::unexpected(*fault));
            return;
        }
        AzureClient::Models::BlobProperties properties;
        {
            std::scoped_lock lock(m_mutex);
            ++PropertiesRequests;
            properties.ETag = ETag;
            properties.ContentLength = Data.size();
            properties.Metadata.emplace("filesize", std::to_string(Size));
        }
        completion(Response<AzureClient::Models::BlobProperties>(std::move(properties), HttpResponse{200, {}, {}}));
    }

    void SetMetadataAsyncImpl(const AzureClient::SetBlobMetadataOptions& options,
                              SetMetadataCompletionHandler completion, AVEVA::HttpRequestOptions) override {
        if (const auto fault = FaultFor(Operation::SetMetadata)) {
            completion(std::unexpected(*fault));
            return;
        }
        {
            std::scoped_lock lock(m_mutex);
            Size = std::stoll(options.Metadata.at("filesize"));
            SizeWrites.push_back(Size);
            ETag = "\"etag-" + std::to_string(++m_version) + "\"";
        }
        completion(Response<AzureClient::Models::SetBlobMetadataResult>({}, HttpResponse{200, {}, {}}));
    }

    void ResizeAsyncImpl(std::uint64_t newSize, const AzureClient::ResizePageBlobOptions&,
                         ResizeCompletionHandler completion, AVEVA::HttpRequestOptions) override {
        if (const auto fault = FaultFor(Operation::Resize)) {
            completion(std::unexpected(*fault));
            return;
        }
        {
            std::scoped_lock lock(m_mutex);
            Data.resize(static_cast<size_t>(newSize), '\0');
            CapacityWrites.push_back(static_cast<int64_t>(newSize));
            ETag = "\"etag-" + std::to_string(++m_version) + "\"";
        }
        completion(Response<AzureClient::Models::ResizePageBlobResult>({}, HttpResponse{200, {}, {}}));
    }

    void StartUpload(const int64_t offset, std::vector<char> content, UploadPagesCompletionHandler completion) {
        {
            std::scoped_lock lock(m_mutex);
            Uploads.push_back({offset, content});
        }
        if (const auto fault = FaultFor(Operation::UploadPages)) {
            completion(std::unexpected(*fault));
            return;
        }
        if (DeferUploads) {
            std::scoped_lock lock(m_mutex);
            m_pendingUploads.push_back({offset, std::move(content), std::move(completion)});
            return;
        }
        Apply(offset, content);
        completion(Response<AzureClient::Models::UploadPagesResult>({}, HttpResponse{201, {}, {}}));
    }

    void UploadPagesBytesAsyncImpl(std::uint64_t offset, std::span<const std::byte> content,
                                   const AzureClient::UploadPagesOptions&, UploadPagesCompletionHandler completion,
                                   AVEVA::HttpRequestOptions) override {
        const auto* begin = reinterpret_cast<const char*>(content.data());
        StartUpload(static_cast<int64_t>(offset), std::vector<char>(begin, begin + content.size()),
                    std::move(completion));
    }

    void UploadPagesSharedAsyncImpl(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                                    const AzureClient::UploadPagesOptions&, UploadPagesCompletionHandler completion,
                                    AVEVA::HttpRequestOptions) override {
        {
            std::scoped_lock lock(m_mutex);
            SharedUploadPayloadAddresses.push_back(content->data());
        }
        StartUpload(static_cast<int64_t>(offset), *content, std::move(completion));
    }

    void UploadPagesStringAsyncImpl(std::uint64_t offset, std::string content, const AzureClient::UploadPagesOptions&,
                                    UploadPagesCompletionHandler completion, AVEVA::HttpRequestOptions) override {
        StartUpload(static_cast<int64_t>(offset), std::vector<char>(content.begin(), content.end()),
                    std::move(completion));
    }

    void DownloadRangeToSpanAsyncImpl(std::span<char> destination, AzureClient::DownloadBlobOptions options,
                                      DownloadToCompletionHandler completion,
                                      AVEVA::HttpRequestOptions requestOptions) override {
        if (const auto fault = FaultFor(Operation::DownloadToBuffer)) {
            completion(std::unexpected(*fault));
            return;
        }
        auto bytes = Read(ToRequest(options, requestOptions));
        if (!bytes) {
            completion(std::unexpected(bytes.error()));
            return;
        }
        if (bytes->size() > destination.size()) {
            completion(std::unexpected(Error(0, "BodyLargerThanBuffer")));
            return;
        }
        std::copy(bytes->begin(), bytes->end(), destination.begin());
        AzureClient::Models::DownloadBlobToResult result;
        result.BytesWritten = bytes->size();
        completion(Response<AzureClient::Models::DownloadBlobToResult>(std::move(result), HttpResponse{206, {}, {}}));
    }

    void DownloadAsyncImpl(AzureClient::DownloadBlobOptions options, DownloadCompletionHandler completion,
                           AVEVA::HttpRequestOptions requestOptions) override {
        if (const auto fault = FaultFor(Operation::Download)) {
            completion(std::unexpected(*fault));
            return;
        }
        const auto request = ToRequest(options, requestOptions);
        if (DeferDownloads) {
            std::scoped_lock lock(m_mutex);
            Downloads.push_back(request);
            m_pendingDownloads.push_back({request, std::move(completion)});
            return;
        }
        auto bytes = Read(request);
        if (!bytes) {
            completion(std::unexpected(bytes.error()));
            return;
        }
        AzureClient::Models::DownloadBlobResult result;
        result.Content = std::move(*bytes);
        completion(Response<AzureClient::Models::DownloadBlobResult>(std::move(result), HttpResponse{206, {}, {}}));
    }

    int64_t m_version = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
