// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
using AzureClient::Tests::FakeHttpClient;

// A request log written by the fake service's transport thread and read by tests. Every read returns a copy taken
// under the lock, so a test can inspect it while requests are still arriving.
template <class T> class RequestLog {
  public:
    RequestLog() = default;
    RequestLog(const RequestLog&) = delete;
    RequestLog& operator=(const RequestLog&) = delete;

    void push_back(T value) {
        const std::scoped_lock lock(m_mutex);
        m_items.push_back(std::move(value));
    }

    [[nodiscard]] std::vector<T> Snapshot() const {
        const std::scoped_lock lock(m_mutex);
        return m_items;
    }
    [[nodiscard]] std::size_t size() const {
        const std::scoped_lock lock(m_mutex);
        return m_items.size();
    }
    [[nodiscard]] bool empty() const { return size() == 0U; }
    [[nodiscard]] T operator[](const std::size_t index) const {
        const std::scoped_lock lock(m_mutex);
        return m_items.at(index);
    }
    [[nodiscard]] T front() const { return (*this)[0]; }
    [[nodiscard]] T back() const {
        const std::scoped_lock lock(m_mutex);
        return m_items.at(m_items.size() - 1U);
    }
    [[nodiscard]] bool operator==(const std::vector<T>& other) const { return Snapshot() == other; }

  private:
    mutable std::mutex m_mutex;
    std::vector<T> m_items;
};

// A request counter that converts to int, so tests read it like a plain value while the transport thread bumps it.
class RequestCounter {
  public:
    RequestCounter() = default;
    RequestCounter(const RequestCounter& other) : m_value(other.m_value.load()) {}
    RequestCounter& operator=(const RequestCounter&) = delete;

    RequestCounter& operator++() {
        ++m_value;
        return *this;
    }
    // NOLINTNEXTLINE(google-explicit-constructor)
    operator int() const { return m_value.load(); }

  private:
    std::atomic<int> m_value{0};
};

using AzureClient::Tests::Knob;

// An in-memory page blob service behind the library's FakeHttpClient. It answers the REST subset the page blob client
// speaks (properties, metadata, resize, page update/clear, ranged download, page list, create), so the plugin's file
// classes run against the real AzureClient request and response handling instead of a hand-written mirror of it.
//
// Public members describe the primary blob (the one `Client` addresses) and record the requests it received.
// Responses are produced when a request is sent; deferred ones apply their effect only when the test releases them.
class FakeBlobServer {
  public:
    using BlobClientType = AzureClient::Tests::PageBlobClient;

    enum class Operation {
        GetProperties,
        SetMetadata,
        Resize,
        UploadPages,
        UploadPagesFromUrl,
        DownloadToBuffer,
        Download,
        DownloadToFile,
        GetPageRanges,
        AcquireLease,
        RenewLease,
        ReleaseLease,
        ListBlobs,
        DeleteBlob,
    };

    struct Upload {
        int64_t Offset = 0;
        std::vector<char> Data;
    };
    // One Put Page From URL request as the service saw it.
    struct PageCopy {
        int64_t Offset = 0;
        int64_t Length = 0;
        std::string SourceUrl;
        int64_t SourceOffset = 0;
        std::string SourceIfMatch;
        std::string SourceAuthorization;
        // The request's timeout, which the plugin bounds by the remaining write budget.
        std::chrono::milliseconds Timeout{};
    };
    struct DownloadRequest {
        int64_t Offset = 0;
        int64_t Length = 0;
        std::string IfMatch;
        std::chrono::milliseconds Timeout{};
    };

  private:
    struct StoredBlob {
        std::vector<char> Data;
        int64_t Size = 0;
        std::string ETag = "\"etag\"";
        int64_t Version = 0;
        std::map<std::string, std::string> Metadata;
        // A deleted blob keeps its node so references to the primary blob's state stay valid.
        bool Exists = true;
        std::string LeaseId;
        long long LeaseSeconds = 0;
        std::chrono::steady_clock::time_point LeaseExpiry;
        // Pages (PageSize bytes each) written by a non-clear write. A page written with zeros is still a valid range
        // on the service. Pages seeded by changing Data directly are recognised by their non-zero bytes instead.
        std::vector<bool> WrittenPages;
    };

    static constexpr size_t PageSize = 512;

    FakeHttpClient* m_http;
    std::mutex m_mutex;
    std::function<std::chrono::steady_clock::time_point()> m_clock = [] { return std::chrono::steady_clock::now(); };
    // Keyed by "container/blob". Nodes are never relocated, so the public members below can refer into the primary.
    std::map<std::string, StoredBlob> m_blobs;
    std::string m_primaryPath;
    std::set<std::string> m_containers;
    // Signalled, with m_mutex held, whenever an upload or a download is deferred.
    std::condition_variable m_requestHeld;
    AzureClient::Tests::HeldRequests<> m_heldUploads;
    AzureClient::Tests::HeldRequests<> m_heldProperties;
    AzureClient::Tests::HeldRequests<DownloadRequest> m_heldDownloads;

  public:
    explicit FakeBlobServer(FakeHttpClient& http, const std::string& container = "files",
                            const std::string& blob = "test.sst")
        : m_http(&http), m_primaryPath(container + "/" + blob), Data(m_blobs[m_primaryPath].Data),
          Size(m_blobs[m_primaryPath].Size), ETag(m_blobs[m_primaryPath].ETag),
          Client(std::make_shared<BlobClientType>(http, AzureClient::Tests::MakeBlobClientOptions(container, blob))) {
        m_http->SetResponder([this](const FakeHttpClient::RequestRecord& record) { return Respond(record); });
    }

    ~FakeBlobServer() { m_http->SetResponder({}); }

    // Adds a blob of `capacity` zero bytes to the same container and returns a real client addressing it.
    [[nodiscard]] std::shared_ptr<BlobClientType> AddBlob(const std::string& blob, const int64_t capacity);

    // The bytes of a blob added with AddBlob (or created through the service).
    [[nodiscard]] std::vector<char> BlobBytes(const std::string& blob) {
        std::scoped_lock lock(m_mutex);
        return m_blobs.at(ContainerOfPrimary() + "/" + blob).Data;
    }

    // The names of the blobs that exist in the primary blob's container.
    [[nodiscard]] std::vector<std::string> BlobNames();

    FakeBlobServer(const FakeBlobServer&) = delete;
    FakeBlobServer& operator=(const FakeBlobServer&) = delete;

    // The primary blob: its bytes (whose length is the capacity), the size recorded in its metadata, and its ETag.
    std::vector<char>& Data;
    int64_t& Size;
    std::string& ETag;

    // The real page blob client, bound to the primary blob and to the fake transport.
    std::shared_ptr<BlobClientType> Client;

    // Requests received, in order. Uploads are recorded when they start, whether or not they are deferred. Reads
    // return copies taken under a lock, so they are safe while other threads keep sending.
    RequestLog<Upload> Uploads;
    // Address of the zero-copy body of every upload that was sent as a view of a caller-owned buffer.
    RequestLog<const char*> SharedUploadPayloadAddresses;
    RequestLog<int64_t> SizeWrites;
    RequestLog<int64_t> CapacityWrites;
    // One entry per ranged GET, so a download split into several requests is recorded as several.
    RequestLog<DownloadRequest> Downloads;
    RequestCounter PropertiesRequests;
    RequestCounter PageRangeRequests;
    RequestCounter LeaseAcquisitions;
    RequestCounter LeaseRenewals;
    RequestCounter LeaseReleases;
    RequestCounter LeaseBreaks;
    // One entry per Put Page From URL request that reached the service, whether or not it succeeded.
    RequestLog<PageCopy> PageCopies;
    // Like a private source: a copy without x-ms-copy-source-authorization fails with 401 CannotVerifyCopySource.
    Knob<bool> RequireCopySourceAuthorization;
    // Replaces the page listing, which otherwise reports every 512-byte page that holds a non-zero byte.
    Knob<std::optional<std::vector<AzureClient::Models::PageRange>>> PageRangeOverride;

    // Returns an error to fail the operation with, or nullopt to let it succeed. Both download operations map to
    // the same GET, so a download is offered as DownloadToBuffer first and then as Download.
    Knob<std::function<std::optional<AzureClient::BlobStorageError>(Operation)>> Fault;
    // Replaces the contents of a download; receives the request and returns the bytes to serve.
    Knob<std::function<std::string(const DownloadRequest&)>> DownloadOverride;
    // Holds page uploads / ranged downloads until the test releases them.
    Knob<bool> DeferUploads;
    Knob<bool> DeferDownloads;
    // Defined before its first use: GCC rejects calling an auto-returning member before its definition.
    template <class THeld> auto Take(THeld& held, const std::size_t position, const char* missing) {
        std::scoped_lock lock(m_mutex);
        auto entry = held.Take(*m_http, position);
        if (!entry) {
            throw std::logic_error(missing);
        }
        return std::move(*entry);
    }

    // Holds Get Blob Properties until CompleteProperties; the response carries the values it had when sent.
    Knob<bool> DeferProperties;
    size_t PendingProperties() { return Count(m_heldProperties); }
    void CompleteProperties() {
        if (!m_http->CompleteRequest(
                Take(m_heldProperties, 0, "FakeBlobServer has no deferred properties request").RequestIndex)) {
            throw std::logic_error("FakeBlobServer properties request was already completed");
        }
    }

    [[nodiscard]] static AzureClient::BlobStorageError Error(unsigned int statusCode, std::string errorCode);

    void SetCapacity(const int64_t capacity) {
        Data.resize(static_cast<size_t>(capacity), '\0');
        m_blobs.at(m_primaryPath).WrittenPages.resize(PageCount(static_cast<size_t>(capacity)));
    }

    size_t PendingUploads() { return Count(m_heldUploads); }
    // Blocks until at least `count` uploads are held, or the timeout passes. Returns whether they are.
    [[nodiscard]] bool WaitForPendingUploads(const size_t count,
                                             const std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        std::unique_lock lock(m_mutex);
        return m_requestHeld.wait_for(lock, timeout, [&] { return m_heldUploads.Count(*m_http) >= count; });
    }
    size_t PendingDownloads() { return Count(m_heldDownloads); }
    // Blocks until at least `count` downloads are held, or the timeout passes. Returns whether they are.
    [[nodiscard]] bool WaitForPendingDownloads(const size_t count,
                                               const std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        std::unique_lock lock(m_mutex);
        return m_requestHeld.wait_for(lock, timeout, [&] { return m_heldDownloads.Count(*m_http) >= count; });
    }

    // Completes a deferred upload; the bytes land in the blob only now.
    void CompleteUpload(const size_t index = 0) {
        if (!m_http->CompleteRequest(
                Take(m_heldUploads, index, "FakeBlobServer upload index out of range").RequestIndex)) {
            throw std::logic_error("FakeBlobServer upload was already completed");
        }
    }
    void FailUpload(const AzureClient::BlobStorageError& error, const size_t index = 0) {
        FailHeld(Take(m_heldUploads, index, "FakeBlobServer upload index out of range").RequestIndex, error);
    }

    // Takes the oldest deferred download; the caller completes it with Succeed / Fail.
    struct HeldDownload {
        DownloadRequest Request;
        std::move_only_function<void(std::string content)> Succeed;
        std::move_only_function<void(AzureClient::BlobStorageError error)> Fail;
    };
    HeldDownload TakeDownload();

  private:
    using Result = FakeHttpClient::ScriptedResult;

    template <class THeld> std::size_t Count(THeld& held) {
        std::scoped_lock lock(m_mutex);
        return held.Count(*m_http);
    }

    void FailHeld(const std::size_t requestIndex, const AzureClient::BlobStorageError& error) {
        auto result = FailureResult(error);
        (void)m_http->FailRequest(requestIndex, result.Error, std::move(result.Response));
    }

    // ---- HTTP helpers ----

    [[nodiscard]] static std::string Header(const HttpRequest& request, std::string_view name) {
        return FakeHttpClient::FindHeaderValue(request, name);
    }

    [[nodiscard]] static std::string NextETag(StoredBlob& blob) {
        blob.ETag = "\"etag-" + std::to_string(++blob.Version) + "\"";
        return blob.ETag;
    }

    [[nodiscard]] static HttpResponse MakeResponse(unsigned int status, std::vector<HttpHeader> headers = {},
                                                   std::string body = {}) {
        headers.emplace_back("x-ms-request-id", "fake-request-id");
        return HttpResponse{status, std::move(headers), std::move(body)};
    }

    [[nodiscard]] static Result Success(HttpResponse response) {
        Result result;
        result.Response = std::move(response);
        return result;
    }

    // A service failure as the wire carries it: status, x-ms-error-code and the XML error body. A status of 0 is a
    // transport failure that never produced a response.
    [[nodiscard]] static Result FailureResult(const AzureClient::BlobStorageError& error);

    [[nodiscard]] static Result Failure(unsigned int status, std::string code) {
        return FailureResult(Error(status, std::move(code)));
    }

    [[nodiscard]] std::optional<AzureClient::BlobStorageError> FaultFor(const Operation operation) const {
        const auto fault = Fault.Get();
        return fault ? fault(operation) : std::nullopt;
    }

    // Parses "bytes=a-b" (inclusive; b optional). Returns {offset, length}; length 0 means "to the end".
    [[nodiscard]] static std::optional<std::pair<int64_t, int64_t>> ParseRange(const std::string& header);

    [[nodiscard]] static std::vector<HttpHeader> BlobHeaders(const StoredBlob& blob);

    [[nodiscard]] static HttpResponse RangeResponse(std::size_t offset, std::string content, std::size_t total,
                                                    const std::string& etag);

    [[nodiscard]] std::string ContainerOfPrimary() const { return m_primaryPath.substr(0, m_primaryPath.find('/')); }

    // ---- request handling ----

    [[nodiscard]] std::optional<Result> Respond(const FakeHttpClient::RequestRecord& record);

    [[nodiscard]] static bool IfMatchFails(const HttpRequest& request, const StoredBlob& blob) {
        const auto ifMatch = Header(request, "If-Match");
        return !ifMatch.empty() && ifMatch != "*" && ifMatch != blob.ETag;
    }

    Result Create(const HttpRequest& request, const std::string& path);

    // Replaces the blob's metadata with the request's x-ms-meta-* headers; "filesize" is the plugin's logical size.
    static void ApplyMetadata(const HttpRequest& request, StoredBlob& blob);

  public:
    // Lease expiry reads this clock, so a test can lapse a lease deterministically instead of sleeping.
    void SetClock(std::function<std::chrono::steady_clock::time_point()> clock) {
        std::scoped_lock lock(m_mutex);
        m_clock = std::move(clock);
    }

  private:
    Result DeleteBlob(const HttpRequest& request, StoredBlob& blob);
    // A held lease makes writes without its id fail the way the service does (412), so fencing is testable.
    std::optional<Result> LeaseConflict(const HttpRequest& request, const StoredBlob& blob) const;

    // Lease Blob: one lease per blob, held until released or its duration passes. A lease that has lapsed can be
    // taken by anyone, as on the service.
    Result Lease(const HttpRequest& request, StoredBlob& blob);

    // Renewals keep the duration the lease was acquired with, which the test clients send only on acquire.
    void ExtendLease(const HttpRequest& request, StoredBlob& blob);

    [[nodiscard]] static std::string XmlEscape(std::string_view text);

    // Container operations: create (the plugin only ever creates if missing) and list blobs.
    Result ContainerRequest(const HttpMethod method, const std::string& comp, const std::string& url,
                            const std::string& container, const std::string& body);

    // Blob Batch: answers each DELETE sub-request in order with its own status, like the service's multipart reply.
    Result BatchDelete(const std::string& body);

    Result ListBlobs(const std::string& url, const std::string& container);

    Result GetProperties(const StoredBlob& blob, const std::size_t requestIndex);

    Result SetMetadata(const HttpRequest& request, StoredBlob& blob);

    Result Resize(const HttpRequest& request, StoredBlob& blob);

    Result GetPageRanges(const HttpRequest& request, const StoredBlob& blob);

    // Put Page From URL: the service copies the range itself, so no body is expected and none is recorded as an upload.
    Result PageCopyFromUrl(const FakeHttpClient::RequestRecord& record, StoredBlob& blob);

    Result PageWrite(const FakeHttpClient::RequestRecord& record, const std::string& path, StoredBlob& blob);

    // Writes (or, for `clear`, zeroes and unmarks) a page range. Unlike a lenient in-memory buffer the service never
    // grows a page blob by writing past its end, so a range that no longer fits (e.g. the blob was shrunk after the
    // request was queued) fails with nullopt and leaves the blob untouched.
    [[nodiscard]] static std::optional<std::string> Apply(StoredBlob& blob, const int64_t offset,
                                                          const std::string& payload, const bool clear = false);

    [[nodiscard]] static size_t PageCount(const size_t bytes) { return (bytes + PageSize - 1) / PageSize; }

    Result Download(const FakeHttpClient::RequestRecord& record, StoredBlob& blob);
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
