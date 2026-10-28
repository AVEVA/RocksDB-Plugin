// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeBlobServer.hpp"

#include <boost/algorithm/string/case_conv.hpp>

#include <algorithm>
#include <charconv>
#include <string_view>
#include <system_error>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
namespace {
using Result = FakeHttpClient::ScriptedResult;
} // namespace

std::shared_ptr<FakeBlobServer::BlobClientType> FakeBlobServer::AddBlob(const std::string& blob,
                                                                        const int64_t capacity) {
    {
        std::scoped_lock lock(m_mutex);
        auto& stored = m_blobs[ContainerOfPrimary() + "/" + blob];
        stored.Data.assign(static_cast<size_t>(capacity), '\0');
        stored.Exists = true;
    }
    return std::make_shared<BlobClientType>(*m_http,
                                            AzureClient::Tests::MakeBlobClientOptions(ContainerOfPrimary(), blob));
}

std::vector<std::string> FakeBlobServer::BlobNames() {
    std::scoped_lock lock(m_mutex);
    const auto root = ContainerOfPrimary() + "/";
    std::vector<std::string> names;
    for (const auto& [path, blob] : m_blobs) {
        if (blob.Exists && path.starts_with(root)) {
            names.push_back(path.substr(root.size()));
        }
    }
    return names;
}

AzureClient::BlobStorageError FakeBlobServer::Error(unsigned int statusCode, std::string errorCode) {
    AzureClient::BlobStorageError error;
    error.StatusCode = statusCode;
    error.ErrorCode = std::move(errorCode);
    error.Message = error.ErrorCode;
    return error;
}

FakeBlobServer::HeldDownload FakeBlobServer::TakeDownload() {
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
            Settle(*m_http);
        },
        [this, requestIndex](AzureClient::BlobStorageError error) {
            FailHeld(requestIndex, error);
            Settle(*m_http);
        }};
}

Result FakeBlobServer::FailureResult(const AzureClient::BlobStorageError& error) {
    Result result;
    if (error.StatusCode == 0U) {
        result.Error = error.Code ? error.Code : std::make_error_code(std::errc::io_error);
        return result;
    }
    result.Response =
        MakeResponse(error.StatusCode,
                     {HttpHeader("x-ms-error-code", error.ErrorCode), HttpHeader("Content-Type", "application/xml")},
                     "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>" + error.ErrorCode + "</Code><Message>" +
                         error.Message + "</Message></Error>");
    return result;
}

std::optional<std::pair<int64_t, int64_t>> FakeBlobServer::ParseRange(const std::string& header) {
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

std::vector<HttpHeader> FakeBlobServer::BlobHeaders(const StoredBlob& blob) {
    std::vector<HttpHeader> headers{HttpHeader("ETag", blob.ETag), HttpHeader("x-ms-blob-type", "PageBlob"),
                                    HttpHeader("x-ms-meta-filesize", std::to_string(blob.Size))};
    for (const auto& [key, value] : blob.Metadata) {
        headers.emplace_back("x-ms-meta-" + key, value);
    }
    return headers;
}

HttpResponse FakeBlobServer::RangeResponse(std::size_t offset, std::string content, std::size_t total,
                                           const std::string& etag) {
    std::vector<HttpHeader> headers{
        HttpHeader("ETag", etag), HttpHeader("x-ms-blob-type", "PageBlob"),
        HttpHeader("Content-Length", std::to_string(content.size())),
        HttpHeader("Content-Range", "bytes " + std::to_string(offset) + "-" +
                                        std::to_string(offset + (content.empty() ? 0 : content.size() - 1)) + "/" +
                                        std::to_string(total))};
    return MakeResponse(206U, std::move(headers), std::move(content));
}

std::optional<Result> FakeBlobServer::Respond(const FakeHttpClient::RequestRecord& record) {
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
        return DeleteBlob(request, blob);
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

Result FakeBlobServer::Create(const HttpRequest& request, const std::string& path) {
    int64_t capacity = 0;
    const auto length = Header(request, "x-ms-blob-content-length");
    if (std::from_chars(length.data(), length.data() + length.size(), capacity).ec != std::errc{}) {
        return Failure(400U, "InvalidHeaderValue");
    }
    std::scoped_lock lock(m_mutex);
    if (const auto fault = FaultFor(Operation::Create)) {
        return FailureResult(*fault);
    }
    const auto existing = m_blobs.find(path);
    if (existing != m_blobs.end() && existing->second.Exists && Header(request, "If-None-Match") == "*") {
        return Failure(409U, "BlobAlreadyExists");
    }
    if (existing != m_blobs.end() && existing->second.Exists) {
        if (const auto conflict = LeaseConflict(request, existing->second)) {
            return *conflict;
        }
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

void FakeBlobServer::ApplyMetadata(const HttpRequest& request, StoredBlob& blob) {
    constexpr std::string_view prefix = "x-ms-meta-";
    blob.Metadata.clear();
    blob.Size = 0;
    for (const auto& header : request.GetHeaders()) {
        std::string name = boost::algorithm::to_lower_copy(std::string(header.GetName()));
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

std::optional<Result> FakeBlobServer::LeaseConflict(const HttpRequest& request, const StoredBlob& blob) const {
    if (blob.LeaseId.empty() || blob.LeaseExpiry <= m_clock()) {
        return std::nullopt;
    }
    const auto supplied = Header(request, "x-ms-lease-id");
    if (supplied.empty()) {
        return Failure(412U, "LeaseIdMissing");
    }
    if (supplied != blob.LeaseId) {
        return Failure(412U, "LeaseIdMismatchWithBlobOperation");
    }
    return std::nullopt;
}

Result FakeBlobServer::DeleteBlob(const HttpRequest& request, StoredBlob& blob) {
    if (const auto fault = FaultFor(Operation::DeleteBlob)) {
        return FailureResult(*fault);
    }
    if (const auto conflict = LeaseConflict(request, blob)) {
        return *conflict;
    }
    blob.Exists = false;
    blob.Data.clear();
    blob.WrittenPages.clear();
    blob.Size = 0;
    blob.Metadata.clear();
    blob.LeaseId.clear();
    return Success(MakeResponse(202U));
}

Result FakeBlobServer::Lease(const HttpRequest& request, StoredBlob& blob) {
    const auto action = Header(request, "x-ms-lease-action");
    const auto now = m_clock();
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
        return Success(MakeResponse(201U, {HttpHeader("ETag", blob.ETag), HttpHeader("x-ms-lease-id", blob.LeaseId)}));
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
        return Success(MakeResponse(200U, {HttpHeader("ETag", blob.ETag), HttpHeader("x-ms-lease-id", blob.LeaseId)}));
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

void FakeBlobServer::ExtendLease(const HttpRequest& request, StoredBlob& blob) {
    const auto duration = Header(request, "x-ms-lease-duration");
    long long seconds = 0;
    if (!duration.empty() &&
        std::from_chars(duration.data(), duration.data() + duration.size(), seconds).ec == std::errc{}) {
        blob.LeaseSeconds = seconds;
    }
    blob.LeaseExpiry = blob.LeaseSeconds < 0 ? std::chrono::steady_clock::time_point::max()
                                             : m_clock() + std::chrono::seconds(blob.LeaseSeconds);
}

std::string FakeBlobServer::XmlEscape(std::string_view text) {
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

Result FakeBlobServer::ContainerRequest(const HttpMethod method, const std::string& comp, const std::string& url,
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

Result FakeBlobServer::BatchDelete(const std::string& body) {
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
    return Success(
        MakeResponse(202U, {HttpHeader("Content-Type", "multipart/mixed; boundary=fakeresponse")}, std::move(reply)));
}

Result FakeBlobServer::ListBlobs(const std::string& url, const std::string& container) {
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
    const bool includeMetadata = AzureClient::Tests::QueryValue(url, "include").find("metadata") != std::string::npos;

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
                       XmlEscape(marker) + "</Marker><Blobs>" + blobs + "</Blobs><NextMarker>" + XmlEscape(nextMarker) +
                       "</NextMarker></EnumerationResults>";
    return Success(MakeResponse(200U, {HttpHeader("Content-Type", "application/xml")}, std::move(body)));
}

Result FakeBlobServer::GetProperties(const StoredBlob& blob, const std::size_t requestIndex) {
    if (const auto fault = FaultFor(Operation::GetProperties)) {
        return FailureResult(*fault);
    }
    ++PropertiesRequests;
    auto headers = BlobHeaders(blob);
    headers.emplace_back("Content-Length", std::to_string(blob.Data.size()));
    Result result = Success(MakeResponse(200U, std::move(headers)));
    if (DeferProperties.Get()) {
        result.Deferred = true;
        m_heldProperties.Add(requestIndex);
    }
    return result;
}

Result FakeBlobServer::SetMetadata(const HttpRequest& request, StoredBlob& blob) {
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

Result FakeBlobServer::Resize(const HttpRequest& request, StoredBlob& blob) {
    if (const auto fault = FaultFor(Operation::Resize)) {
        return FailureResult(*fault);
    }
    if (const auto conflict = LeaseConflict(request, blob)) {
        return *conflict;
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

Result FakeBlobServer::GetPageRanges(const HttpRequest& request, const StoredBlob& blob) {
    if (const auto fault = FaultFor(Operation::GetPageRanges)) {
        return FailureResult(*fault);
    }
    ++PageRangeRequests;
    if (IfMatchFails(request, blob)) {
        return Failure(412U, "ConditionNotMet");
    }
    std::vector<AzureClient::Models::PageRange> ranges;
    if (const auto override = PageRangeOverride.Get()) {
        ranges = *override;
    } else {
        // A page is populated if it was written (even with zeros) or, for seeded data, holds a non-zero byte.
        for (size_t offset = 0; offset < blob.Data.size(); offset += PageSize) {
            const auto end = (std::min)(offset + PageSize, blob.Data.size());
            const bool written = offset / PageSize < blob.WrittenPages.size() && blob.WrittenPages[offset / PageSize];
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

Result FakeBlobServer::PageCopyFromUrl(const FakeHttpClient::RequestRecord& record, StoredBlob& blob) {
    const HttpRequest& request = record.Request;
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
                  Header(request, "x-ms-copy-source-authorization"),
                  record.Options.GetTimeout()};
    PageCopies.push_back(copy);

    if (const auto fault = FaultFor(Operation::UploadPagesFromUrl)) {
        return FailureResult(*fault);
    }
    if (range->second > 4 * 1024 * 1024 || range->first % 512 != 0 || range->second % 512 != 0 ||
        sourceRange->first % 512 != 0) {
        return Failure(400U, "InvalidHeaderValue");
    }
    if (RequireCopySourceAuthorization.Get() && copy.SourceAuthorization.empty()) {
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

Result FakeBlobServer::PageWrite(const FakeHttpClient::RequestRecord& record, const std::string& path,
                                 StoredBlob& blob) {
    const HttpRequest& request = record.Request;
    if (!Header(request, "x-ms-copy-source").empty()) {
        return PageCopyFromUrl(record, blob);
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
    if (!clear && DeferUploads.Get()) {
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
        m_requestHeld.notify_all();
        return deferred;
    }
    const auto etag = Apply(blob, offset, payload, clear);
    if (!etag) {
        return Failure(416U, "InvalidPageRange");
    }
    return Success(MakeResponse(201U, {HttpHeader("ETag", *etag)}));
}

std::optional<std::string> FakeBlobServer::Apply(StoredBlob& blob, const int64_t offset, const std::string& payload,
                                                 const bool clear) {
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

Result FakeBlobServer::Download(const FakeHttpClient::RequestRecord& record, StoredBlob& blob) {
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
    if (DeferDownloads.Get()) {
        Result deferred = Success(MakeResponse(206U));
        deferred.Deferred = true;
        m_heldDownloads.Add(record.Index, download);
        m_requestHeld.notify_all();
        return deferred;
    }

    if (const auto downloadOverride = DownloadOverride.Get()) {
        auto content = downloadOverride(download);
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

} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
