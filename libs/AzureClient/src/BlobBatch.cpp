// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Detail/BlobBatch.hpp>

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>

#include <charconv>
#include <cstddef>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Private {
namespace {
constexpr std::string_view LineBreak = "\r\n";

[[nodiscard]] std::string_view BoundaryOf(std::string_view contentType) {
    constexpr std::string_view key = "boundary=";
    for (std::size_t pos = 0; pos + key.size() <= contentType.size(); ++pos) {
        if (!IEquals(contentType.substr(pos, key.size()), key)) {
            continue;
        }
        std::string_view value = contentType.substr(pos + key.size());
        if (const std::size_t end = value.find(';'); end != std::string_view::npos) {
            value = value.substr(0, end);
        }
        value = TrimWhitespace(value);
        if (value.size() >= 2U && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2U);
        }
        return value;
    }
    return {};
}

// Reads one line (terminated by CRLF or a bare LF) from `text` and advances `pos` past it.
[[nodiscard]] std::string_view ReadLine(std::string_view text, std::size_t& pos) {
    const std::size_t end = text.find('\n', pos);
    std::string_view line = end == std::string_view::npos ? text.substr(pos) : text.substr(pos, end - pos);
    pos = end == std::string_view::npos ? text.size() : end + 1U;
    if (!line.empty() && line.back() == '\r') {
        line.remove_suffix(1);
    }
    return line;
}

// Calls `visit(name, value)` with views into `text` for each "Name: value" line, up to and including the blank
// line that ends the header block. Allocates nothing.
template <class TVisitor> void VisitHeaders(std::string_view text, std::size_t& pos, TVisitor&& visit) {
    while (pos < text.size()) {
        const std::string_view line = ReadLine(text, pos);
        if (line.empty()) {
            break;
        }
        const std::size_t colon = line.find(':');
        if (colon == std::string_view::npos) {
            throw std::invalid_argument("Malformed header line in batch response part.");
        }
        visit(TrimWhitespace(line.substr(0, colon)), TrimWhitespace(line.substr(colon + 1U)));
    }
}

[[nodiscard]] std::vector<HttpHeader> ReadHeaders(std::string_view text, std::size_t& pos) {
    std::vector<HttpHeader> headers;
    VisitHeaders(text, pos, [&headers](std::string_view name, std::string_view value) {
        headers.emplace_back(std::string{name}, std::string{value});
    });
    return headers;
}

[[nodiscard]] std::string_view TrimTrailingLineBreak(std::string_view text) noexcept {
    if (text.ends_with(LineBreak)) {
        text.remove_suffix(LineBreak.size());
    } else if (text.ends_with('\n')) {
        text.remove_suffix(1);
    }
    return text;
}

// One sub-response, as views into the batch response body. Headers are only materialised for failures.
struct BatchPart {
    std::size_t ContentId = 0;
    unsigned int Status = 0;
    std::string_view HeaderBlock; // The sub-response's header lines and everything after them.
};

[[nodiscard]] BatchPart ParsePart(std::string_view part) {
    std::size_t pos = 0;
    std::optional<std::size_t> contentId;
    VisitHeaders(part, pos, [&contentId](std::string_view name, std::string_view text) {
        if (IEquals(name, "Content-ID")) {
            std::size_t value = 0;
            const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
            if (error != std::errc{} || end != text.data() + text.size()) {
                throw std::invalid_argument("Batch response part has an invalid Content-ID.");
            }
            contentId = value;
        }
    });
    if (!contentId) {
        throw std::invalid_argument("Batch response part has no Content-ID.");
    }

    // "HTTP/1.1 202 Accepted"
    const std::string_view statusLine = ReadLine(part, pos);
    const std::size_t statusStart = statusLine.find(' ');
    if (!statusLine.starts_with("HTTP/") || statusStart == std::string_view::npos) {
        throw std::invalid_argument("Batch response part has a malformed status line.");
    }
    const std::string_view statusText = statusLine.substr(statusStart + 1U);
    unsigned int status = 0;
    const auto [statusEnd, statusError] =
        std::from_chars(statusText.data(), statusText.data() + statusText.size(), status);
    if (statusError != std::errc{} || statusEnd == statusText.data()) {
        throw std::invalid_argument("Batch response part has a malformed status code.");
    }

    return BatchPart{.ContentId = *contentId, .Status = status, .HeaderBlock = part.substr(pos)};
}

[[nodiscard]] HttpResponse MaterializeResponse(const BatchPart& part) {
    std::size_t pos = 0;
    std::vector<HttpHeader> headers = ReadHeaders(part.HeaderBlock, pos);
    const std::string_view body = TrimTrailingLineBreak(part.HeaderBlock.substr(pos));
    return HttpResponse{part.Status, std::move(headers), std::string{body}};
}

[[nodiscard]] Models::DeleteBlobItemResult ToItemResult(const std::string& name, const BatchPart& part) {
    constexpr unsigned int FirstNonSuccessStatus = 300;
    Models::DeleteBlobItemResult item;
    item.Name = name;
    if (part.Status < FirstNonSuccessStatus) {
        return item;
    }
    const HttpResponse response = MaterializeResponse(part);
    RequestFailure failure = DetermineBlobStorageFailure({}, response);
    if (!failure.Error) {
        return item;
    }
    // The same outcomes DeleteIfExists treats as "nothing to delete".
    if (failure.Error == BlobStorageErrorCode::BlobNotFound ||
        failure.Error == BlobStorageErrorCode::ResourceNotFound) {
        item.AlreadyAbsent = true;
        return item;
    }
    BlobStorageError details =
        failure.Details.has_value() ? std::move(*failure.Details) : MakeClientError(failure.Error);
    details.Code = failure.Error;
    item.Error = std::move(details);
    return item;
}
} // namespace

void AppendBatchDeletePart(std::string& body, std::string_view boundary, std::size_t contentId,
                           const HttpRequest& subrequest) {
    // Validate before appending anything, so a bad URL leaves `body` untouched.
    const std::string_view url = subrequest.GetUrl();
    const std::size_t schemeEnd = url.find("://");
    const std::size_t pathStart =
        schemeEnd == std::string_view::npos ? std::string_view::npos : url.find('/', schemeEnd + 3U);
    if (pathStart == std::string_view::npos) {
        throw std::invalid_argument("Batch sub-request URL must be absolute and include a path.");
    }
    body.append("--").append(boundary).append(LineBreak);
    body.append("Content-Type: application/http").append(LineBreak);
    body.append("Content-Transfer-Encoding: binary").append(LineBreak);
    body.append("Content-ID: ").append(std::to_string(contentId)).append(LineBreak);
    body.append(LineBreak);
    // The service rejects absolute-form targets inside a batch part, so send path and query only.
    body.append("DELETE ").append(url.substr(pathStart)).append(" HTTP/1.1").append(LineBreak);
    for (const HttpHeader& header : subrequest.GetHeaders()) {
        body.append(header.GetName()).append(": ").append(header.GetValue()).append(LineBreak);
    }
    body.append(LineBreak);
    // The multipart delimiter's own line break is not the sub-request's terminating blank line; the service rejects a
    // part without it.
    body.append(LineBreak);
}

void AppendBatchClosingBoundary(std::string& body, std::string_view boundary) {
    body.append("--").append(boundary).append("--").append(LineBreak);
}

std::vector<Models::DeleteBlobItemResult> ParseBatchDeleteResponse(const HttpResponse& response,
                                                                   std::span<const std::string> names) {
    const std::string_view boundary = BoundaryOf(FindHeaderValue(response, ContentTypeHeaderName));
    if (boundary.empty()) {
        throw std::invalid_argument("Batch response has no multipart boundary.");
    }
    const std::string delimiter = "--" + std::string{boundary};
    const std::string_view body = response.GetBody();

    std::vector<std::optional<BatchPart>> byContentId(names.size());
    // Delimiters only count at the start of a line, so a boundary-like string inside a part is not a split point.
    const auto findDelimiter = [&](std::size_t from) {
        for (std::size_t pos = body.find(delimiter, from); pos != std::string_view::npos;
             pos = body.find(delimiter, pos + 1U)) {
            if (pos == 0U || body[pos - 1U] == '\n') {
                return pos;
            }
        }
        return std::string_view::npos;
    };

    std::size_t delimiterPos = findDelimiter(0);
    bool closed = false;
    while (delimiterPos != std::string_view::npos && !closed) {
        std::size_t partStart = delimiterPos + delimiter.size();
        if (body.substr(partStart).starts_with("--")) {
            closed = true;
            break;
        }
        static_cast<void>(ReadLine(body, partStart));
        const std::size_t next = findDelimiter(partStart);
        if (next == std::string_view::npos) {
            throw std::invalid_argument("Batch response is missing its closing boundary.");
        }
        const BatchPart part = ParsePart(TrimTrailingLineBreak(body.substr(partStart, next - partStart)));
        if (part.ContentId >= names.size() || byContentId[part.ContentId].has_value()) {
            throw std::invalid_argument("Batch response has an unexpected or duplicate Content-ID.");
        }
        byContentId[part.ContentId] = part;
        delimiterPos = next;
    }
    if (!closed) {
        throw std::invalid_argument("Batch response has no parts.");
    }

    std::vector<Models::DeleteBlobItemResult> items;
    items.reserve(names.size());
    for (std::size_t index = 0; index < names.size(); ++index) {
        if (!byContentId[index].has_value()) {
            throw std::invalid_argument("Batch response does not answer every sub-request.");
        }
        items.push_back(ToItemResult(names[index], *byContentId[index]));
    }
    return items;
}
} // namespace AVEVA::AzureClient::Private
