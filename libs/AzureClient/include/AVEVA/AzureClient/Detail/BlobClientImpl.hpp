// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include <AVEVA/AzureClient/Detail/BlobDownload.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <filesystem>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient {
template <HttpClientLike THttp, TokenCredentialLike TCred>
BasicBlobClient<THttp, TCred>::BasicBlobClient(THttp& httpClient, const BlobClientOptions& options,
                                               std::shared_ptr<TCred> credential)
    : m_httpClient(&httpClient), m_target(std::make_shared<const Private::BasicBlobTarget<TCred>>(
                                     Private::MakeBlobTarget(options, std::move(credential)))) {}

template <HttpClientLike THttp, TokenCredentialLike TCred>
BasicBlobClient<THttp, TCred>::BasicBlobClient(THttp& httpClient,
                                               std::shared_ptr<const Private::BasicBlobTarget<TCred>> target)
    : m_httpClient(&httpClient), m_target(std::move(target)) {}

template <HttpClientLike THttp, TokenCredentialLike TCred>
const HttpRequestOptions& BasicBlobClient<THttp, TCred>::GetDefaultRequestOptions() const noexcept {
    return m_target->Connection->DefaultRequestOptions;
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DownloadAsyncImpl(DownloadBlobOptions options, THandler completion,
                                                      HttpRequestOptions requestOptions) {
    Private::DownloadBlobAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DownloadToStreamAsyncImpl(std::ostream& stream, DownloadToOptions options,
                                                              THandler completion, HttpRequestOptions requestOptions) {
    Private::DownloadBlobToAsync(*m_httpClient, *m_target, std::move(options), stream, std::move(completion),
                                 requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DownloadToFileAsyncImpl(const std::filesystem::path& path,
                                                            DownloadToOptions options, THandler completion,
                                                            HttpRequestOptions requestOptions) {
    Private::DownloadBlobToFileAsync(*m_httpClient, *m_target, std::move(options), path, std::move(completion),
                                     requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DownloadRangeToStreamAsyncImpl(std::ostream& stream, DownloadBlobOptions options,
                                                                   THandler completion,
                                                                   HttpRequestOptions requestOptions) {
    DownloadToStreamAsyncImpl(stream, Private::ToDownloadToOptions(std::move(options)), std::move(completion),
                              requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DownloadRangeToSpanAsyncImpl(std::span<char> destination,
                                                                 DownloadBlobOptions options, THandler completion,
                                                                 HttpRequestOptions requestOptions) {
    Private::DownloadBlobToSpanAsync(*m_httpClient, *m_target, Private::ToDownloadToOptions(std::move(options)),
                                     destination, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DownloadRangeToFileAsyncImpl(const std::filesystem::path& path,
                                                                 DownloadBlobOptions options, THandler completion,
                                                                 HttpRequestOptions requestOptions) {
    DownloadToFileAsyncImpl(path, Private::ToDownloadToOptions(std::move(options)), std::move(completion),
                            requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DeleteAsyncImpl(const DeleteBlobOptions& options, THandler completion,
                                                    HttpRequestOptions requestOptions) {
    Private::DeleteBlobAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::DeleteIfExistsAsyncImpl(const DeleteBlobOptions& options, THandler completion,
                                                            HttpRequestOptions requestOptions) {
    Private::DeleteBlobAsync(
        *m_httpClient, *m_target, options,
        Private::SuppressErrors<Models::DeleteBlobResult>(
            std::move(completion), {BlobStorageErrorCode::BlobNotFound, BlobStorageErrorCode::ResourceNotFound}),
        requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::GetPropertiesAsyncImpl(const GetBlobPropertiesOptions& options, THandler completion,
                                                           HttpRequestOptions requestOptions) {
    Private::GetBlobPropertiesAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::ExistsAsyncImpl(THandler completion, HttpRequestOptions requestOptions) {
    Private::ExistsBlobAsync(*m_httpClient, *m_target, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::SetMetadataAsyncImpl(const SetBlobMetadataOptions& options, THandler completion,
                                                         HttpRequestOptions requestOptions) {
    Private::SetBlobMetadataAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::AcquireLeaseAsyncImpl(AcquireLeaseOptions options, THandler completion,
                                                          HttpRequestOptions requestOptions) {
    Private::AcquireLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::RenewLeaseAsyncImpl(RenewLeaseOptions options, THandler completion,
                                                        HttpRequestOptions requestOptions) {
    Private::RenewLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlobClient<THttp, TCred>::ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options, THandler completion,
                                                          HttpRequestOptions requestOptions) {
    Private::ReleaseLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

} // namespace AVEVA::AzureClient
