// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobServiceClient.hpp>

#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "BlobRequestHelpers.hpp"
#include "RequestPipeline.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <string>
#include <utility>

namespace AVEVA::AzureClient {
template <HttpClientLike THttp>
BasicBlobServiceClient<THttp>::BasicBlobServiceClient(THttp& httpClient, BlobServiceClientOptions options)
    : m_httpClient(&httpClient), m_connection(Private::MakeConnectionState(std::move(options))) {}

template <HttpClientLike THttp>
const HttpRequestOptions& BasicBlobServiceClient<THttp>::GetDefaultRequestOptions() const noexcept {
    return m_connection->DefaultRequestOptions;
}

template <HttpClientLike THttp>
std::unique_ptr<BasicBlobContainerClient<THttp>>
BasicBlobServiceClient<THttp>::GetBlobContainerClient(std::string containerName) const {
    return std::unique_ptr<BasicBlobContainerClient<THttp>>(
        new BasicBlobContainerClient<THttp>{*m_httpClient, m_connection, std::move(containerName)});
}
} // namespace AVEVA::AzureClient
