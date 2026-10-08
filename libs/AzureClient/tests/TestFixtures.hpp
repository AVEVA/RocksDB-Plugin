// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Tests
{
    inline constexpr std::string_view DefaultServiceEndpoint = "https://storageaccount.blob.core.windows.net/";
    inline constexpr std::string_view DefaultSasToken = "?sv=2025-01-05&sig=fakesig";
    inline constexpr std::string_view DefaultApiVersion = "2023-11-03";
    inline constexpr std::string_view DefaultLastModified = "Fri, 26 Jun 2015 18:59:17 GMT";
    inline constexpr std::string_view DefaultETag = "\"0x8D1234\"";

    [[nodiscard]] inline BlobClientOptions MakeBlobClientOptions(std::string containerName = "images",
        std::string blobName = "photo.png")
    {
        BlobClientOptions options{
            .ServiceEndpoint = std::string(DefaultServiceEndpoint),
            .ContainerName = std::move(containerName),
            .BlobName = std::move(blobName),
            .SasToken = std::string(DefaultSasToken),
            .ApiVersion = std::string(DefaultApiVersion),
        };
        // Scripted single-response tests must not consume extra responses through automatic
        // retries; retry behavior is covered explicitly by RetryTests.
        options.Retry.MaxRetries = 0;
        return options;
    }

    [[nodiscard]] inline BlobContainerClientOptions MakeBlobContainerClientOptions(std::string containerName = "images")
    {
        BlobContainerClientOptions options{
            .ServiceEndpoint = std::string(DefaultServiceEndpoint),
            .ContainerName = std::move(containerName),
            .SasToken = std::string(DefaultSasToken),
            .ApiVersion = std::string(DefaultApiVersion),
        };
        options.Retry.MaxRetries = 0;
        return options;
    }

    [[nodiscard]] inline BlobServiceClientOptions MakeBlobServiceClientOptions()
    {
        BlobServiceClientOptions options{
            .ServiceEndpoint = std::string(DefaultServiceEndpoint),
            .SasToken = std::string(DefaultSasToken),
            .ApiVersion = std::string(DefaultApiVersion),
        };
        options.Retry.MaxRetries = 0;
        return options;
    }

    [[nodiscard]] inline std::vector<HttpHeader> MakeCanonicalSuccessHeaders(
        std::initializer_list<std::pair<std::string_view, std::string_view>> extraHeaders = {})
    {
        std::vector<HttpHeader> headers{
            HttpHeader{"ETag", std::string(DefaultETag)},
            HttpHeader{"Last-Modified", std::string(DefaultLastModified)},
            HttpHeader{"x-ms-request-id", "request-123"},
        };
        for (const auto& [name, value] : extraHeaders)
        {
            bool replaced = false;
            for (auto& header : headers)
            {
                if (header.GetName() == name)
                {
                    header = HttpHeader{std::string(name), std::string(value)};
                    replaced = true;
                    break;
                }
            }
            if (!replaced)
            {
                headers.emplace_back(std::string(name), std::string(value));
            }
        }
        return headers;
    }

    [[nodiscard]] inline HttpResponse MakeAzureErrorResponse(unsigned int statusCode,
        std::string_view errorCode,
        std::string_view message,
        std::string_view requestId)
    {
        std::vector<HttpHeader> headers;
        if (!errorCode.empty())
        {
            headers.emplace_back("x-ms-error-code", std::string(errorCode));
        }
        if (!requestId.empty())
        {
            headers.emplace_back("x-ms-request-id", std::string(requestId));
        }

        const std::string body =
            message.empty() ? std::string{} : "<Error><Message>" + std::string(message) + "</Message></Error>";
        return HttpResponse{statusCode, std::move(headers), body};
    }
} // namespace AVEVA::AzureClient::Tests
