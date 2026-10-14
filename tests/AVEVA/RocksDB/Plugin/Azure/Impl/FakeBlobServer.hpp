// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "FakeHttpClient.hpp"
#include "FakeHttpUrl.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
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
    // Keyed by "container/blob". Nodes are never relocated, so the public members below can refer into the primary.
    std::map<std::string, StoredBlob> m_blobs;
    std::string m_primaryPath;
    std::set<std::string> m_containers;
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
    [[nodiscard]] std::shared_ptr<BlobClientType> AddBlob(const std::string& blob, const int64_t capacity) {
        {
            std::scoped_lock lock(m_mutex);
            auto& stored = m_blobs[ContainerOfPrimary() + "/" + blob];
            stored.Data.assign(static_cast<size_t>(capacity), '\0');
            stored.Exists = true;
        }
        return std::make_shared<BlobClientType>(*m_http,
                                                AzureClient::Tests::MakeBlobClientOptions(ContainerOfPrimary(), blob));
    }

    // The bytes of a blob added with AddBlob (or created through the service).
    [[nodiscard]] std::vector<char> BlobBytes(const std::string& blob) {
        std::scoped_lock lock(m_mutex);
        return m_blobs.at(ContainerOfPrimary() + "/" + blob).Data;
    }

    FakeBlobServer(const FakeBlobServer&) = delete;
    FakeBlobServer& operator=(const FakeBlobServer&) = delete;

    // The primary blob: its bytes (whose length is the capacity), the size recorded in its metadata, and its ETag.
    std::vector<char>& Data;
    int64_t& Size;
    std::string& ETag;

    // The real page blob client, bound to the primary blob and to the fake transport.
    std::shared_ptr<BlobClientType> Client;

    // Requests received, in order. Uploads are recorded when they start, whether or not they are deferred.
    std::vector<Upload> Uploads;
    // Address of the zero-copy body of every upload that was sent as a view of a caller-owned buffer.
    std::vector<const char*> SharedUploadPayloadAddresses;
    std::vector<int64_t> SizeWrites;
    std::vector<int64_t> CapacityWrites;
    // One entry per ranged GET, so a download split into several requests is recorded as several.
    std::vector<DownloadRequest> Downloads;
    int PropertiesRequests = 0;
    int PageRangeRequests = 0;
    int LeaseAcquisitions = 0;
    int LeaseRenewals = 0;
    int LeaseReleases = 0;
    int LeaseBreaks = 0;
    // One entry per Put Page From URL request that reached the service, whether or not it succeeded.
    std::vector<PageCopy> PageCopies;
    // Like a private source: a copy without x-ms-copy-source-authorization fails with 401 CannotVerifyCopySource.
    bool RequireCopySourceAuthorization = false;
    // Replaces the page listing, which otherwise reports every 512-byte page that holds a non-zero byte.
    std::optional<std::vector<AzureClient::Models::PageRange>> PageRangeOverride;

    // Returns an error to fail the operation with, or nullopt to let it succeed. Both download operations map to
    // the same GET, so a download is offered as DownloadToBuffer first and then as Download.
    std::function<std::optional<AzureClient::BlobStorageError>(Operation)> Fault;
    // Replaces the contents of a download; receives the request and returns the bytes to serve.
    std::function<std::string(const DownloadRequest&)> DownloadOverride;
    // Holds page uploads / ranged downloads until the test releases them.
    bool DeferUploads = false;
    bool DeferDownloads = false;
    // Holds Get Blob Properties until CompleteProperties; the response carries the values it had when sent.
    bool DeferProperties = false;
    size_t PendingProperties() { return Count(m_heldProperties); }
    void CompleteProperties() {
        if (!m_http->CompleteRequest(
                Take(m_heldProperties, 0, "FakeBlobServer has no deferred properties request").RequestIndex)) {
            throw std::logic_error("FakeBlobServer properties request was already completed");
        }
    }

    [[nodiscard]] static AzureClient::BlobStorageError Error(unsigned int statusCode, std::string errorCode) {
        AzureClient::BlobStorageError error;
        error.StatusCode = statusCode;
        error.ErrorCode = std::move(errorCode);
        error.Message = error.ErrorCode;
        return error;
    }

    void SetCapacity(const int64_t capacity) {
        Data.resize(static_cast<size_t>(capacity), '\0');
        m_blobs.at(m_primaryPath).WrittenPages.resize(PageCount(static_cast<size_t>(capacity)));
    }

    size_t PendingUploads() { return Count(m_heldUploads); }
    size_t PendingDownloads() { return Count(m_heldDownloads); }

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
    HeldDownload TakeDownload() {
        const auto held = Take(m_heldDownloads, 0, "FakeBlobServer has no deferred download");
        const std::size_t requestIndex = held.RequestIndex;
        const DownloadRequest request = held.Meta;
        return HeldDownload{
            request,
            [this, requestIndex, request](std::string content) {
                // The service-side range and total are derived from the content handed over, so the library's
                // length checks hold even when a test serves bytes that differ from the blob's contents.
                std::size_t total = 0;
                std::string etag;
                {
                    std::scoped_lock lock(m_mutex);
                    total = m_blobs.at(m_primaryPath).Data.size();
                    etag = m_blobs.at(m_primaryPath).ETag;
                }
                const auto offset = static_cast<std::size_t>(request.Offset);
                const bool shortRead = request.Length > 0 && content.size() < static_cast<std::size_t>(request.Length);
                total = shortRead ? offset + content.size() : (std::max)(total, offset + content.size());
                (void)m_http->FailRequest(requestIndex, {}, RangeResponse(offset, std::move(content), total, etag));
            },
            [this, requestIndex](AzureClient::BlobStorageError error) { FailHeld(requestIndex, error); }};
    }

  private:
    using Result = FakeHttpClient::ScriptedResult;

    template <class THeld> std::size_t Count(THeld& held) {
        std::scoped_lock lock(m_mutex);
        return held.Count(*m_http);
    }

    template <class THeld> auto Take(THeld& held, const std::size_t position, const char* missing) {
        std::scoped_lock lock(m_mutex);
        auto entry = held.Take(*m_http, position);
        if (!entry) {
            throw std::logic_error(missing);
        }
        return std::move(*entry);
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
    [[nodiscard]] static Result FailureResult(const AzureClient::BlobStorageError& error) {
        Result result;
        if (error.StatusCode == 0U) {
            result.Error = error.Code ? error.Code : std::make_error_code(std::errc::io_error);
            return result;
        }
        result.Response = MakeResponse(
            error.StatusCode,
            {HttpHeader("x-ms-error-code", error.ErrorCode), HttpHeader("Content-Type", "application/xml")},
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>" + error.ErrorCode + "</Code><Message>" +
                error.Message + "</Message></Error>");
        return result;
    }

    [[nodiscard]] static Result Failure(unsigned int status, std::string code) {
        return FailureResult(Error(status, std::move(code)));
    }

    [[nodiscard]] std::optional<AzureClient::BlobStorageError> FaultFor(const Operation operation) const {
        return Fault ? Fault(operation) : std::nullopt;
    }

    // Parses "bytes=a-b" (inclusive; b optional). Returns {offset, length}; length 0 means "to the end".
    [[nodiscard]] static std::optional<std::pair<int64_t, int64_t>> ParseRange(const std::string& header) {
        constexpr std::string_view prefix = "bytes=";
        if (!header.starts_with(prefix)) {
            return std::nullopt;
        }
        const std::string_view range = std::string_view(header).substr(prefix.size());
        const auto dash = range.find('-');
        if (dash == std::string_view::npos) {
            return std::nullopt;
        }
        int64_t start = 0;
        int64_t end = -1;
        if (std::from_chars(range.data(), range.data() + dash, start).ec != std::errc{}) {
            return std::nullopt;
        }
        if (dash + 1 < range.size() &&
            std::from_chars(range.data() + dash + 1, range.data() + range.size(), end).ec != std::errc{}) {
            return std::nullopt;
        }
        return std::pair<int64_t, int64_t>{start, end < 0 ? 0 : end - start + 1};
    }

    [[nodiscard]] static std::vector<HttpHeader> BlobHeaders(const StoredBlob& blob) {
        std::vector<HttpHeader> headers{HttpHeader("ETag", blob.ETag), HttpHeader("x-ms-blob-type", "PageBlob"),
                                        HttpHeader("x-ms-meta-filesize", std::to_string(blob.Size))};
        for (const auto& [key, value] : blob.Metadata) {
            headers.emplace_back("x-ms-meta-" + key, value);
        }
        return headers;
    }

    [[nodiscard]] static HttpResponse RangeResponse(std::size_t offset, std::string content, std::size_t total,
                                                    const std::string& etag) {
        std::vector<HttpHeader> headers{
            HttpHeader("ETag", etag), HttpHeader("x-ms-blob-type", "PageBlob"),
            HttpHeader("Content-Length", std::to_string(content.size())),
            HttpHeader("Content-Range", "bytes " + std::to_string(offset) + "-" +
                                            std::to_string(offset + (content.empty() ? 0 : content.size() - 1)) + "/" +
                                            std::to_string(total))};
        return MakeResponse(206U, std::move(headers), std::move(content));
    }

    [[nodiscard]] std::string ContainerOfPrimary() const { return m_primaryPath.substr(0, m_primaryPath.find('/')); }

    // ---- request handling ----

    [[nodiscard]] std::optional<Result> Respond(const FakeHttpClient::RequestRecord& record) {
        const HttpRequest& request = record.Request;
        const std::string& url = request.GetUrl();
        const std::string path = AzureClient::Tests::UrlPath(url);
        const std::string comp = AzureClient::Tests::QueryValue(url, "comp");
        const HttpMethod method = request.GetMethod();

        if (AzureClient::Tests::QueryValue(url, "restype") == "container") {
            return ContainerRequest(method, comp, url, path, record.Body);
        }
        if (method == HttpMethod::Put && comp.empty()) {
            return Create(request, path);
        }

        std::scoped_lock lock(m_mutex);
        const auto it = m_blobs.find(path);
        if (it == m_blobs.end() || !it->second.Exists) {
            return Failure(404U, "BlobNotFound");
        }
        StoredBlob& blob = it->second;

        if (method == HttpMethod::Head) {
            return GetProperties(blob, record.Index);
        }
        if (method == HttpMethod::Get && comp.empty()) {
            return Download(record, blob);
        }
        if (method == HttpMethod::Get && comp == "pagelist") {
            return GetPageRanges(request, blob);
        }
        if (method == HttpMethod::Delete) {
            return DeleteBlob(blob);
        }
        if (method == HttpMethod::Put && comp == "lease") {
            return Lease(request, blob);
        }
        if (method == HttpMethod::Put && comp == "metadata") {
            return SetMetadata(request, blob);
        }
        if (method == HttpMethod::Put && comp == "properties") {
            return Resize(request, blob);
        }
        if (method == HttpMethod::Put && comp == "page") {
            return PageWrite(record, path, blob);
        }
        return Failure(400U, "UnsupportedHeaderOrQuery");
    }

    [[nodiscard]] static bool IfMatchFails(const HttpRequest& request, const StoredBlob& blob) {
        const auto ifMatch = Header(request, "If-Match");
        return !ifMatch.empty() && ifMatch != "*" && ifMatch != blob.ETag;
    }

    Result Create(const HttpRequest& request, const std::string& path) {
        int64_t capacity = 0;
        const auto length = Header(request, "x-ms-blob-content-length");
        if (std::from_chars(length.data(), length.data() + length.size(), capacity).ec != std::errc{}) {
            return Failure(400U, "InvalidHeaderValue");
        }
        std::scoped_lock lock(m_mutex);
        const auto existing = m_blobs.find(path);
        if (existing != m_blobs.end() && existing->second.Exists && Header(request, "If-None-Match") == "*") {
            return Failure(409U, "BlobAlreadyExists");
        }
        // Put Page Blob overwrites an existing blob outright.
        StoredBlob& blob = m_blobs[path];
        blob.Exists = true;
        blob.Data.assign(static_cast<size_t>(capacity), '\0');
        blob.WrittenPages.assign(PageCount(static_cast<size_t>(capacity)), false);
        blob.Size = 0;
        blob.Metadata.clear();
        ApplyMetadata(request, blob);
        return Success(MakeResponse(201U, {HttpHeader("ETag", NextETag(blob))}));
    }

    // Replaces the blob's metadata with the request's x-ms-meta-* headers; "filesize" is the plugin's logical size.
    static void ApplyMetadata(const HttpRequest& request, StoredBlob& blob) {
        constexpr std::string_view prefix = "x-ms-meta-";
        blob.Metadata.clear();
        blob.Size = 0;
        for (const auto& header : request.GetHeaders()) {
            std::string name = header.GetName();
            std::ranges::transform(name, name.begin(),
                                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (!name.starts_with(prefix)) {
                continue;
            }
            name.erase(0, prefix.size());
            if (name == "filesize") {
                blob.Size = std::stoll(header.GetValue());
            } else {
                blob.Metadata.emplace(std::move(name), header.GetValue());
            }
        }
    }

    Result DeleteBlob(StoredBlob& blob) {
        if (const auto fault = FaultFor(Operation::DeleteBlob)) {
            return FailureResult(*fault);
        }
        blob.Exists = false;
        blob.Data.clear();
        blob.WrittenPages.clear();
        blob.Size = 0;
        blob.Metadata.clear();
        blob.LeaseId.clear();
        return Success(MakeResponse(202U));
    }

    // Lease Blob: one lease per blob, held until released or its duration passes. A lease that has lapsed can be
    // taken by anyone, as on the service.
    Result Lease(const HttpRequest& request, StoredBlob& blob) {
        const auto action = Header(request, "x-ms-lease-action");
        const auto now = std::chrono::steady_clock::now();
        const bool held = !blob.LeaseId.empty() && blob.LeaseExpiry > now;
        if (action == "acquire") {
            if (const auto fault = FaultFor(Operation::AcquireLease)) {
                return FailureResult(*fault);
            }
            const auto proposed = Header(request, "x-ms-proposed-lease-id");
            if (held && blob.LeaseId != proposed) {
                return Failure(409U, "LeaseAlreadyPresent");
            }
            ++LeaseAcquisitions;
            blob.LeaseId = proposed;
            ExtendLease(request, blob);
            return Success(
                MakeResponse(201U, {HttpHeader("ETag", blob.ETag), HttpHeader("x-ms-lease-id", blob.LeaseId)}));
        }
        const auto leaseId = Header(request, "x-ms-lease-id");
        if (action == "renew") {
            if (const auto fault = FaultFor(Operation::RenewLease)) {
                return FailureResult(*fault);
            }
            if (blob.LeaseId != leaseId) {
                return Failure(409U, "LeaseIdMismatchWithLeaseOperation");
            }
            ++LeaseRenewals;
            ExtendLease(request, blob);
            return Success(
                MakeResponse(200U, {HttpHeader("ETag", blob.ETag), HttpHeader("x-ms-lease-id", blob.LeaseId)}));
        }
        if (action == "release") {
            if (const auto fault = FaultFor(Operation::ReleaseLease)) {
                return FailureResult(*fault);
            }
            if (blob.LeaseId != leaseId) {
                return Failure(409U, "LeaseIdMismatchWithLeaseOperation");
            }
            ++LeaseReleases;
            blob.LeaseId.clear();
            blob.LeaseSeconds = 0;
            blob.LeaseExpiry = {};
            return Success(MakeResponse(200U, {HttpHeader("ETag", blob.ETag)}));
        }
        if (action == "break") {
            if (blob.LeaseId.empty()) {
                return Failure(409U, "LeaseNotPresentWithLeaseOperation");
            }
            ++LeaseBreaks;
            // The fake only models immediate breaks, so the lease is over as soon as the call completes.
            blob.LeaseExpiry = {};
            return Success(MakeResponse(202U, {HttpHeader("ETag", blob.ETag), HttpHeader("x-ms-lease-time", "0")}));
        }
        return Failure(400U, "InvalidHeaderValue");
    }

    // Renewals keep the duration the lease was acquired with, which the test clients send only on acquire.
    void ExtendLease(const HttpRequest& request, StoredBlob& blob) {
        const auto duration = Header(request, "x-ms-lease-duration");
        long long seconds = 0;
        if (!duration.empty() &&
            std::from_chars(duration.data(), duration.data() + duration.size(), seconds).ec == std::errc{}) {
            blob.LeaseSeconds = seconds;
        }
        blob.LeaseExpiry = blob.LeaseSeconds < 0
                               ? std::chrono::steady_clock::time_point::max()
                               : std::chrono::steady_clock::now() + std::chrono::seconds(blob.LeaseSeconds);
    }

    [[nodiscard]] static std::string XmlEscape(std::string_view text) {
        std::string escaped;
        for (const char c : text) {
            switch (c) {
            case '&':
                escaped += "&amp;";
                break;
            case '<':
                escaped += "&lt;";
                break;
            case '>':
                escaped += "&gt;";
                break;
            default:
                escaped += c;
            }
        }
        return escaped;
    }

    // Container operations: create (the plugin only ever creates if missing) and list blobs.
    Result ContainerRequest(const HttpMethod method, const std::string& comp, const std::string& url,
                            const std::string& container, const std::string& body) {
        std::scoped_lock lock(m_mutex);
        if (method == HttpMethod::Post && comp == "batch") {
            return BatchDelete(body);
        }
        if (method == HttpMethod::Put && comp.empty()) {
            if (!m_containers.insert(container).second) {
                return Failure(409U, "ContainerAlreadyExists");
            }
            return Success(MakeResponse(201U, {HttpHeader("ETag", "\"container\"")}));
        }
        if (method == HttpMethod::Get && comp == "list") {
            return ListBlobs(url, container);
        }
        return Failure(400U, "UnsupportedHeaderOrQuery");
    }

    // Blob Batch: answers each DELETE sub-request in order with its own status, like the service's multipart reply.
    Result BatchDelete(const std::string& body) {
        constexpr std::string_view marker = "DELETE ";
        std::string reply;
        std::size_t position = 0;
        int contentId = 0;
        while ((position = body.find(marker, position)) != std::string::npos) {
            const auto urlStart = position + marker.size();
            const auto urlEnd = body.find(' ', urlStart);
            if (urlEnd == std::string::npos) {
                break;
            }
            position = urlEnd;
            const auto path = AzureClient::Tests::UrlPath(body.substr(urlStart, urlEnd - urlStart));
            const auto it = m_blobs.find(path);

            std::optional<AzureClient::BlobStorageError> error;
            if (it == m_blobs.end() || !it->second.Exists) {
                error = Error(404U, "BlobNotFound");
            } else if ((error = FaultFor(Operation::DeleteBlob)).has_value()) {
                // Keep the fault's error.
            } else {
                StoredBlob& blob = it->second;
                blob.Exists = false;
                blob.Data.clear();
                blob.WrittenPages.clear();
                blob.Size = 0;
                blob.Metadata.clear();
                blob.LeaseId.clear();
            }

            reply += "--fakeresponse\r\nContent-Type: application/http\r\nContent-ID: " + std::to_string(contentId++) +
                     "\r\n\r\n";
            if (error) {
                reply += "HTTP/1.1 " + std::to_string(error->StatusCode) +
                         " Error\r\nx-ms-error-code: " + error->ErrorCode + "\r\nContent-Length: 0\r\n\r\n\r\n";
            } else {
                reply += "HTTP/1.1 202 Accepted\r\nx-ms-request-id: fake-request-id\r\nContent-Length: 0\r\n\r\n\r\n";
            }
        }
        reply += "--fakeresponse--\r\n";
        return Success(MakeResponse(202U, {HttpHeader("Content-Type", "multipart/mixed; boundary=fakeresponse")},
                                    std::move(reply)));
    }

    Result ListBlobs(const std::string& url, const std::string& container) {
        if (const auto fault = FaultFor(Operation::ListBlobs)) {
            return FailureResult(*fault);
        }
        const std::string root = container + "/";
        const std::string prefix = AzureClient::Tests::QueryValue(url, "prefix");
        const std::string marker = AzureClient::Tests::QueryValue(url, "marker");
        const std::string maxText = AzureClient::Tests::QueryValue(url, "maxresults");
        size_t maxResults = 5000;
        if (!maxText.empty()) {
            std::from_chars(maxText.data(), maxText.data() + maxText.size(), maxResults);
        }
        const bool includeMetadata =
            AzureClient::Tests::QueryValue(url, "include").find("metadata") != std::string::npos;

        std::string blobs;
        std::string nextMarker;
        size_t count = 0;
        for (auto it = m_blobs.lower_bound(root + (marker.empty() ? prefix : marker)); it != m_blobs.end(); ++it) {
            const std::string name = it->first.substr(std::min(root.size(), it->first.size()));
            if (!it->first.starts_with(root + prefix)) {
                break;
            }
            if (!it->second.Exists) {
                continue;
            }
            if (count == maxResults) {
                nextMarker = name;
                break;
            }
            ++count;
            blobs += "<Blob><Name>" + XmlEscape(name) + "</Name><Properties><Etag>" + XmlEscape(it->second.ETag) +
                     "</Etag><Content-Length>" + std::to_string(it->second.Data.size()) +
                     "</Content-Length><BlobType>PageBlob</BlobType></Properties>";
            if (includeMetadata) {
                blobs += "<Metadata><filesize>" + std::to_string(it->second.Size) + "</filesize>";
                for (const auto& [key, value] : it->second.Metadata) {
                    blobs += "<" + key + ">" + XmlEscape(value) + "</" + key + ">";
                }
                blobs += "</Metadata>";
            }
            blobs += "</Blob>";
        }
        std::string body = "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ContainerName=\"" +
                           XmlEscape(container) + "\"><Prefix>" + XmlEscape(prefix) + "</Prefix><Marker>" +
                           XmlEscape(marker) + "</Marker><Blobs>" + blobs + "</Blobs><NextMarker>" +
                           XmlEscape(nextMarker) + "</NextMarker></EnumerationResults>";
        return Success(MakeResponse(200U, {HttpHeader("Content-Type", "application/xml")}, std::move(body)));
    }

    Result GetProperties(const StoredBlob& blob, const std::size_t requestIndex) {
        if (const auto fault = FaultFor(Operation::GetProperties)) {
            return FailureResult(*fault);
        }
        ++PropertiesRequests;
        auto headers = BlobHeaders(blob);
        headers.emplace_back("Content-Length", std::to_string(blob.Data.size()));
        Result result = Success(MakeResponse(200U, std::move(headers)));
        if (DeferProperties) {
            result.Deferred = true;
            m_heldProperties.Add(requestIndex);
        }
        return result;
    }

    Result SetMetadata(const HttpRequest& request, StoredBlob& blob) {
        if (const auto fault = FaultFor(Operation::SetMetadata)) {
            return FailureResult(*fault);
        }
        if (IfMatchFails(request, blob)) {
            return Failure(412U, "ConditionNotMet");
        }
        ApplyMetadata(request, blob);
        SizeWrites.push_back(blob.Size);
        return Success(MakeResponse(200U, {HttpHeader("ETag", NextETag(blob))}));
    }

    Result Resize(const HttpRequest& request, StoredBlob& blob) {
        if (const auto fault = FaultFor(Operation::Resize)) {
            return FailureResult(*fault);
        }
        if (IfMatchFails(request, blob)) {
            return Failure(412U, "ConditionNotMet");
        }
        int64_t capacity = 0;
        const auto length = Header(request, "x-ms-blob-content-length");
        if (std::from_chars(length.data(), length.data() + length.size(), capacity).ec != std::errc{}) {
            return Failure(400U, "InvalidHeaderValue");
        }
        blob.Data.resize(static_cast<size_t>(capacity), '\0');
        blob.WrittenPages.resize(PageCount(static_cast<size_t>(capacity)));
        CapacityWrites.push_back(capacity);
        return Success(MakeResponse(200U, {HttpHeader("ETag", NextETag(blob))}));
    }

    Result GetPageRanges(const HttpRequest& request, const StoredBlob& blob) {
        if (const auto fault = FaultFor(Operation::GetPageRanges)) {
            return FailureResult(*fault);
        }
        ++PageRangeRequests;
        if (IfMatchFails(request, blob)) {
            return Failure(412U, "ConditionNotMet");
        }
        std::vector<AzureClient::Models::PageRange> ranges;
        if (PageRangeOverride) {
            ranges = *PageRangeOverride;
        } else {
            // A page is populated if it was written (even with zeros) or, for seeded data, holds a non-zero byte.
            for (size_t offset = 0; offset < blob.Data.size(); offset += PageSize) {
                const auto end = (std::min)(offset + PageSize, blob.Data.size());
                const bool written =
                    offset / PageSize < blob.WrittenPages.size() && blob.WrittenPages[offset / PageSize];
                if (written || std::any_of(blob.Data.begin() + static_cast<std::ptrdiff_t>(offset),
                                           blob.Data.begin() + static_cast<std::ptrdiff_t>(end),
                                           [](char byte) { return byte != '\0'; })) {
                    if (!ranges.empty() && ranges.back().Offset + ranges.back().Length == offset) {
                        ranges.back().Length += PageSize;
                    } else {
                        ranges.push_back({offset, PageSize});
                    }
                }
            }
        }
        std::string body = "<?xml version=\"1.0\" encoding=\"utf-8\"?><PageList>";
        for (const auto& range : ranges) {
            body += "<PageRange><Start>" + std::to_string(range.Offset) + "</Start><End>" +
                    std::to_string(range.Offset + range.Length - 1) + "</End></PageRange>";
        }
        body += "</PageList>";
        return Success(MakeResponse(200U,
                                    {HttpHeader("ETag", blob.ETag),
                                     HttpHeader("x-ms-blob-content-length", std::to_string(blob.Data.size())),
                                     HttpHeader("Content-Type", "application/xml")},
                                    std::move(body)));
    }

    // Put Page From URL: the service copies the range itself, so no body is expected and none is recorded as an upload.
    Result PageCopyFromUrl(const HttpRequest& request, StoredBlob& blob) {
        const auto range = ParseRange(Header(request, "x-ms-range"));
        const auto sourceRange = ParseRange(Header(request, "x-ms-source-range"));
        if (!range || !sourceRange || range->second <= 0 || range->second != sourceRange->second) {
            return Failure(400U, "InvalidHeaderValue");
        }
        PageCopy copy{range->first,
                      range->second,
                      Header(request, "x-ms-copy-source"),
                      sourceRange->first,
                      Header(request, "x-ms-source-if-match"),
                      Header(request, "x-ms-copy-source-authorization")};
        PageCopies.push_back(copy);

        if (const auto fault = FaultFor(Operation::UploadPagesFromUrl)) {
            return FailureResult(*fault);
        }
        if (range->second > 4 * 1024 * 1024 || range->first % 512 != 0 || range->second % 512 != 0 ||
            sourceRange->first % 512 != 0) {
            return Failure(400U, "InvalidHeaderValue");
        }
        if (RequireCopySourceAuthorization && copy.SourceAuthorization.empty()) {
            return Failure(401U, "CannotVerifyCopySource");
        }
        const auto source = m_blobs.find(AzureClient::Tests::UrlPath(copy.SourceUrl));
        if (source == m_blobs.end() || !source->second.Exists) {
            return Failure(404U, "CannotVerifyCopySource");
        }
        if (!copy.SourceIfMatch.empty() && copy.SourceIfMatch != source->second.ETag) {
            return Failure(412U, "SourceConditionNotMet");
        }
        if (IfMatchFails(request, blob)) {
            return Failure(412U, "ConditionNotMet");
        }
        const auto capacity = static_cast<int64_t>(blob.Data.size());
        if (copy.Offset + copy.Length > capacity ||
            copy.SourceOffset + copy.Length > static_cast<int64_t>(source->second.Data.size())) {
            return Failure(416U, "InvalidPageRange");
        }
        const std::string payload(source->second.Data.begin() + copy.SourceOffset,
                                  source->second.Data.begin() + copy.SourceOffset + copy.Length);
        const auto etag = Apply(blob, copy.Offset, payload);
        if (!etag) {
            return Failure(416U, "InvalidPageRange");
        }
        return Success(MakeResponse(201U, {HttpHeader("ETag", *etag)}));
    }

    Result PageWrite(const FakeHttpClient::RequestRecord& record, const std::string& path, StoredBlob& blob) {
        const HttpRequest& request = record.Request;
        if (!Header(request, "x-ms-copy-source").empty()) {
            return PageCopyFromUrl(request, blob);
        }
        const auto range = ParseRange(Header(request, "x-ms-range"));
        if (!range || range->second <= 0) {
            return Failure(400U, "InvalidHeaderValue");
        }
        const auto [offset, length] = *range;
        const bool clear = Header(request, "x-ms-page-write") == "clear";
        const auto capacity = static_cast<int64_t>(blob.Data.size());

        if (!clear) {
            Uploads.push_back({offset, std::vector<char>(record.Body.begin(), record.Body.end())});
            if (request.HasBodyView()) {
                SharedUploadPayloadAddresses.push_back(reinterpret_cast<const char*>(request.GetBodyView().data()));
            }
            if (const auto fault = FaultFor(Operation::UploadPages)) {
                return FailureResult(*fault);
            }
        }
        if (IfMatchFails(request, blob)) {
            return Failure(412U, "ConditionNotMet");
        }
        // Unlike a lenient in-memory buffer, the service never grows a page blob by writing past its end.
        if (offset + length > capacity) {
            return Failure(416U, "InvalidPageRange");
        }
        if (!clear && static_cast<int64_t>(record.Body.size()) != length) {
            return Failure(400U, "InvalidHeaderValue");
        }

        const std::string payload = clear ? std::string(static_cast<size_t>(length), '\0') : record.Body;
        if (!clear && DeferUploads) {
            Result deferred = Success(MakeResponse(201U));
            deferred.Deferred = true;
            deferred.OnComplete = [this, path, offset, payload](HttpResponse& response) {
                std::scoped_lock completionLock(m_mutex);
                const auto etag = Apply(m_blobs.at(path), offset, payload);
                if (etag) {
                    response = MakeResponse(201U, {HttpHeader("ETag", *etag)});
                } else {
                    response = Failure(416U, "InvalidPageRange").Response;
                }
            };
            m_heldUploads.Add(record.Index);
            return deferred;
        }
        const auto etag = Apply(blob, offset, payload, clear);
        if (!etag) {
            return Failure(416U, "InvalidPageRange");
        }
        return Success(MakeResponse(201U, {HttpHeader("ETag", *etag)}));
    }

    // Writes (or, for `clear`, zeroes and unmarks) a page range. Unlike a lenient in-memory buffer the service never
    // grows a page blob by writing past its end, so a range that no longer fits (e.g. the blob was shrunk after the
    // request was queued) fails with nullopt and leaves the blob untouched.
    [[nodiscard]] static std::optional<std::string> Apply(StoredBlob& blob, const int64_t offset,
                                                          const std::string& payload, const bool clear = false) {
        const auto end = static_cast<size_t>(offset) + payload.size();
        if (end > blob.Data.size()) {
            return std::nullopt;
        }
        std::ranges::copy(payload, blob.Data.begin() + offset);
        if (blob.WrittenPages.size() < PageCount(blob.Data.size())) {
            blob.WrittenPages.resize(PageCount(blob.Data.size()));
        }
        for (size_t page = static_cast<size_t>(offset) / PageSize; page * PageSize < end; ++page) {
            blob.WrittenPages[page] = !clear;
        }
        return NextETag(blob);
    }

    [[nodiscard]] static size_t PageCount(const size_t bytes) { return (bytes + PageSize - 1) / PageSize; }

    Result Download(const FakeHttpClient::RequestRecord& record, StoredBlob& blob) {
        const HttpRequest& request = record.Request;
        std::string rangeHeader = Header(request, "Range");
        if (rangeHeader.empty()) {
            rangeHeader = Header(request, "x-ms-range");
        }
        const auto range = ParseRange(rangeHeader);

        DownloadRequest download;
        if (range) {
            download.Offset = range->first;
            download.Length = range->second;
        }
        download.IfMatch = Header(request, "If-Match");
        download.Timeout = record.Options.GetTimeout();

        if (const auto fault = FaultFor(Operation::DownloadToBuffer)) {
            return FailureResult(*fault);
        }
        if (const auto fault = FaultFor(Operation::Download)) {
            return FailureResult(*fault);
        }
        Downloads.push_back(download);
        if (DeferDownloads) {
            Result deferred = Success(MakeResponse(206U));
            deferred.Deferred = true;
            m_heldDownloads.Add(record.Index, download);
            return deferred;
        }

        if (DownloadOverride) {
            auto content = DownloadOverride(download);
            const auto offset = static_cast<size_t>(download.Offset);
            const auto total = (std::max)(blob.Data.size(), offset + content.size());
            return Success(RangeResponse(offset, std::move(content), total, blob.ETag));
        }
        if (!download.IfMatch.empty() && download.IfMatch != blob.ETag) {
            return Failure(412U, "ConditionNotMet");
        }
        if (!range) {
            auto headers = BlobHeaders(blob);
            return Success(MakeResponse(200U, std::move(headers), std::string(blob.Data.data(), blob.Data.size())));
        }
        const auto offset = static_cast<size_t>(download.Offset);
        if (offset >= blob.Data.size()) {
            Result result = Failure(416U, "InvalidRange");
            return result;
        }
        const auto available = download.Length > 0
                                   ? (std::min)(static_cast<size_t>(download.Length), blob.Data.size() - offset)
                                   : blob.Data.size() - offset;
        return Success(
            RangeResponse(offset, std::string(blob.Data.data() + offset, available), blob.Data.size(), blob.ETag));
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
