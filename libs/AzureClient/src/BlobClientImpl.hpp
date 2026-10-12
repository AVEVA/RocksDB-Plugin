// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "BlobDownload.hpp"
#include "BlobRequestHelpers.hpp"
#include "RequestPipeline.hpp"

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
template <HttpClientLike THttp>
BasicBlobClient<THttp>::BasicBlobClient(THttp& httpClient, const BlobClientOptions& options)
    : m_httpClient(&httpClient),
      m_target(std::make_shared<const Private::BlobTarget>(Private::MakeBlobTarget(options))) {}

template <HttpClientLike THttp>
BasicBlobClient<THttp>::BasicBlobClient(THttp& httpClient, std::shared_ptr<const Private::BlobTarget> target)
    : m_httpClient(&httpClient), m_target(std::move(target)) {}

template <HttpClientLike THttp>
const HttpRequestOptions& BasicBlobClient<THttp>::GetDefaultRequestOptions() const noexcept {
    return m_target->Connection->DefaultRequestOptions;
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DownloadAsyncImpl(DownloadBlobOptions options, DownloadCompletionHandler completion,
                                               HttpRequestOptions requestOptions) {
    Private::DownloadBlobAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DownloadToStreamAsyncImpl(std::ostream& stream, DownloadToOptions options,
                                                       DownloadToCompletionHandler completion,
                                                       HttpRequestOptions requestOptions) {
    Private::DownloadBlobToAsync(*m_httpClient, *m_target, std::move(options), stream, std::move(completion),
                                 requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DownloadToFileAsyncImpl(const std::filesystem::path& path, DownloadToOptions options,
                                                     DownloadToCompletionHandler completion,
                                                     HttpRequestOptions requestOptions) {
    Private::DownloadBlobToFileAsync(*m_httpClient, *m_target, std::move(options), path, std::move(completion),
                                     requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DownloadRangeToStreamAsyncImpl(std::ostream& stream, DownloadBlobOptions options,
                                                            DownloadToCompletionHandler completion,
                                                            HttpRequestOptions requestOptions) {
    DownloadToStreamAsyncImpl(stream, Private::ToDownloadToOptions(std::move(options)), std::move(completion),
                              requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DownloadRangeToSpanAsyncImpl(std::span<char> destination, DownloadBlobOptions options,
                                                          DownloadToCompletionHandler completion,
                                                          HttpRequestOptions requestOptions) {
    Private::DownloadBlobToSpanAsync(*m_httpClient, *m_target, Private::ToDownloadToOptions(std::move(options)),
                                     destination, std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DownloadRangeToFileAsyncImpl(const std::filesystem::path& path,
                                                          DownloadBlobOptions options,
                                                          DownloadToCompletionHandler completion,
                                                          HttpRequestOptions requestOptions) {
    DownloadToFileAsyncImpl(path, Private::ToDownloadToOptions(std::move(options)), std::move(completion),
                            requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DeleteAsyncImpl(const DeleteBlobOptions& options, DeleteCompletionHandler completion,
                                             HttpRequestOptions requestOptions) {
    Private::DeleteBlobAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::DeleteIfExistsAsyncImpl(const DeleteBlobOptions& options,
                                                     DeleteCompletionHandler completion,
                                                     HttpRequestOptions requestOptions) {
    Private::DeleteBlobAsync(
        *m_httpClient, *m_target, options,
        Private::SuppressErrors<Models::DeleteBlobResult>(
            std::move(completion), {BlobStorageErrorCode::BlobNotFound, BlobStorageErrorCode::ResourceNotFound}),
        requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::GetPropertiesAsyncImpl(const GetBlobPropertiesOptions& options,
                                                    GetPropertiesCompletionHandler completion,
                                                    HttpRequestOptions requestOptions) {
    Private::GetBlobPropertiesAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::ExistsAsyncImpl(ExistsCompletionHandler completion, HttpRequestOptions requestOptions) {
    Private::ExistsBlobAsync(*m_httpClient, *m_target, std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::SetMetadataAsyncImpl(const SetBlobMetadataOptions& options,
                                                  SetMetadataCompletionHandler completion,
                                                  HttpRequestOptions requestOptions) {
    Private::SetBlobMetadataAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::AcquireLeaseAsyncImpl(AcquireLeaseOptions options,
                                                   AcquireLeaseCompletionHandler completion,
                                                   HttpRequestOptions requestOptions) {
    Private::AcquireLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::RenewLeaseAsyncImpl(RenewLeaseOptions options, RenewLeaseCompletionHandler completion,
                                                 HttpRequestOptions requestOptions) {
    Private::RenewLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

template <HttpClientLike THttp>
void BasicBlobClient<THttp>::ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options,
                                                   ReleaseLeaseCompletionHandler completion,
                                                   HttpRequestOptions requestOptions) {
    Private::ReleaseLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
}

} // namespace AVEVA::AzureClient
