// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobServiceClient.hpp>

#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <string>
#include <utility>

namespace AVEVA::AzureClient {
template <HttpClientLike THttp, TokenCredentialLike TCred>
BasicBlobServiceClient<THttp, TCred>::BasicBlobServiceClient(THttp& httpClient, BlobServiceClientOptions options,
                                                             std::shared_ptr<TCred> credential)
    : m_httpClient(&httpClient), m_connection(Private::MakeConnectionState(std::move(options), credential != nullptr)),
      m_credential(Private::MakeCachedCredential(std::move(credential))) {}

template <HttpClientLike THttp, TokenCredentialLike TCred>
const HttpRequestOptions& BasicBlobServiceClient<THttp, TCred>::GetDefaultRequestOptions() const noexcept {
    return m_connection->DefaultRequestOptions;
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
std::unique_ptr<BasicBlobContainerClient<THttp, TCred>>
BasicBlobServiceClient<THttp, TCred>::GetBlobContainerClient(std::string containerName) const {
    return std::unique_ptr<BasicBlobContainerClient<THttp, TCred>>(new BasicBlobContainerClient<THttp, TCred>{
        *m_httpClient, m_connection, m_credential, std::move(containerName)});
}
} // namespace AVEVA::AzureClient
