// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <cstdint>
#include <string>
#include <string_view>

namespace AVEVA::AzureClient::Tests
{
    [[nodiscard]] inline std::string RangeHeader(std::uint64_t offset, std::uint64_t length)
    {
        return "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1U);
    }

    // A 206 response for `body` at `offset` of a blob of `total` bytes.
    [[nodiscard]] inline HttpResponse RangeResponse(std::uint64_t offset,
        std::string body,
        std::uint64_t total,
        std::string_view etag = DefaultETag)
    {
        const std::string contentRange = "bytes " + std::to_string(offset) + "-" +
                                         std::to_string(offset + body.size() - 1U) + "/" + std::to_string(total);
        const std::string contentLength = std::to_string(body.size());
        return HttpResponse{206,
            MakeCanonicalSuccessHeaders(
                {{"Content-Range", contentRange}, {"Content-Length", contentLength}, {"ETag", etag}}),
            std::move(body)};
    }

    // Scripts `response` for the request whose Range header is `range`.
    inline void EnqueueForRange(FakeHttpClient& httpClient,
        std::string range,
        HttpResponse response,
        bool deferred = true)
    {
        httpClient.EnqueueResponse(std::move(response),
            {},
            deferred,
            [range = std::move(range)](const FakeHttpClient::RequestRecord& record)
        {
            return FakeHttpClient::FindHeaderValue(record.Request, "Range") == range;
        });
    }

    // Scripts every chunk of `content` (ChunkSize `chunk`) as a deferred 206 response.
    inline void EnqueueChunks(FakeHttpClient& httpClient,
        const std::string& content,
        std::size_t chunk,
        std::string_view etag = DefaultETag)
    {
        for (std::size_t offset = 0; offset < content.size(); offset += chunk)
        {
            const std::string body = content.substr(offset, chunk);
            EnqueueForRange(httpClient,
                RangeHeader(offset, body.size()),
                RangeResponse(offset, body, content.size(), etag));
        }
    }

    // Index of the recorded request whose Range header is `range`, or RequestCount() if none.
    [[nodiscard]] inline std::size_t FindRequestByRange(const FakeHttpClient& httpClient, std::string_view range)
    {
        for (std::size_t index = 0; index < httpClient.RequestCount(); ++index)
        {
            if (FakeHttpClient::FindHeaderValue(httpClient.RequestAt(index).Request, "Range") == range)
            {
                return index;
            }
        }
        return httpClient.RequestCount();
    }
} // namespace AVEVA::AzureClient::Tests
