// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/BlobServiceClient.hpp>

#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <string>
#include <utility>

namespace AVEVA::AzureClient
{
    BlobServiceClient::BlobServiceClient(IHttpClient& httpClient, BlobServiceClientOptions options)
        : m_httpClient(&httpClient), m_connection(Private::MakeConnectionState(std::move(options)))
    {
    }

    const HttpRequestOptions& BlobServiceClient::GetDefaultRequestOptions() const noexcept
    {
        return m_connection->DefaultRequestOptions;
    }

    BlobContainerClient BlobServiceClient::GetBlobContainerClient(std::string containerName) const
    {
        return BlobContainerClient{*m_httpClient, m_connection, std::move(containerName)};
    }
} // namespace AVEVA::AzureClient
