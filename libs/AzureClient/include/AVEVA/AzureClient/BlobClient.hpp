// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/ClientForwardDeclarations.hpp>
#include <AVEVA/AzureClient/Detail/BlobDownload.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/ClientCore.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/awaitable.hpp>

#include <expected>
#include <filesystem>
#include <gsl/pointers>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient {

/// <summary>The result of every blob operation.</summary>
template <class T> using BlobResult = std::expected<Response<T>, BlobStorageError>;

// Operations common to every blob type; PageBlobClient derives from it. Usable on its own when the blob
// type is irrelevant.
//
// Every operation is a C++20 coroutine: `co_await` it (or `co_spawn` it) and it yields
// std::expected<Response<T>, BlobStorageError>. Argument validation failures and every other error are reported
// through that result, never thrown.
//
// Thread safety: distinct instances are independent; concurrent operations on one instance are supported
// (connection state is immutable once constructed). Span arguments are not synchronized by the library and must
// not be touched by the caller until the operation completes.
//
// Lifetime: the arguments, this client's target and request options are captured when the operation is called, so
// destroying or moving from this BlobClient afterwards is fine. The HTTP client (and its io_context) is borrowed
// and must outlive the operation, unless this client was built from a std::shared_ptr to it (then every operation
// shares ownership). Span arguments and caller-owned objects passed by reference must outlive the operation.
//
// Cancellation: cancel the coroutine (for example through a cancellation slot bound to the completion token of
// co_spawn). Only terminal and total cancellation abort an operation; a partial cancellation request is ignored.
// An aborted operation completes with std::errc::operation_canceled.
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicBlobClient : Private::ClientCore<THttp, Private::BasicBlobTarget<TCred>> {
    using Core = Private::ClientCore<THttp, Private::BasicBlobTarget<TCred>>;

  public:
    using Target = Private::BasicBlobTarget<TCred>;

    BasicBlobClient(THttp& httpClient, const BlobClientOptions& options, std::shared_ptr<TCred> credential = nullptr)
        : Core(httpClient, std::make_shared<const Target>(Private::MakeBlobTarget(options, std::move(credential)))) {}
    // Shares ownership of the HTTP client for as long as this client or any operation started from it exists.
    BasicBlobClient(std::shared_ptr<THttp> httpClient, const BlobClientOptions& options,
                    std::shared_ptr<TCred> credential = nullptr)
        : Core(std::move(httpClient),
               std::make_shared<const Target>(Private::MakeBlobTarget(options, std::move(credential)))) {}
    BasicBlobClient(const BasicBlobClient&) = delete;
    BasicBlobClient& operator=(const BasicBlobClient&) = delete;
    BasicBlobClient(BasicBlobClient&&) noexcept = default;
    BasicBlobClient& operator=(BasicBlobClient&&) noexcept = default;
    ~BasicBlobClient() = default;

    // The executor of the underlying HTTP client.
    using executor_type = typename THttp::executor_type;
    using Core::get_executor;
    using Core::GetDefaultRequestOptions;

    // The blob's URL carrying the client's SAS token when it has one, so it is a secret: use it as the source of a
    // server-side copy, never as a key or in a log line (use ResourceUrl() for those).
    [[nodiscard]] std::string Url() const { return Private::BuildBlobUrl(*m_target); }

    // The blob's URL without any query string (no SAS), stable for the blob's lifetime and safe to log or to use as
    // an identity key.
    [[nodiscard]] const std::string& ResourceUrl() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target->EncodedResource;
    }

    // True when Url() carries a SAS token, so the blob can be named as a copy source without any other authorization.
    [[nodiscard]] bool HasSasToken() const noexcept { return !m_target->Connection->SasToken.empty(); }

    // True when this client authorizes with a bearer token that UploadPagesFromUrlOptions::
    // AuthorizeSourceWithClientCredential can forward to the service. A SAS or Shared Key client cannot forward
    // anything, so a copy from a source without a SAS is bound to be refused.
    [[nodiscard]] bool CanForwardCredential() const noexcept { return m_target->Credential != nullptr; }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobResult>>
    DownloadAsync(DownloadBlobOptions options = {}, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobAsync(*m_httpClient, m_target, std::move(options),
                                          Core::Resolve(std::move(requestOptions)));
    }

    // DownloadToAsync overload set (the first argument picks the destination, the options type picks the mode):
    //   (path, DownloadToOptions)   - whole blob (or DownloadToOptions::Range), chunked and optionally parallel.
    //   (path, DownloadBlobOptions) - honours DownloadBlobOptions::Range; goes through the same chunked engine.
    //   (span, DownloadBlobOptions) - the range straight into caller memory.
    // File writes never run on the threads completing HTTP requests: they go to DownloadToOptions::WriteScheduler,
    // else the transport's GetDownloadWriter() if it has one (::AVEVA::HttpClient does not), else the download fails
    // with operation_not_supported.

    // Chunked download into `path`, via a temporary sibling file that replaces `path` only on success.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobToResult>>
    DownloadToAsync(std::filesystem::path path, DownloadToOptions options = {},
                    std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobToFileAsync(*m_httpClient, m_target, std::move(options), std::move(path),
                                                Core::Resolve(std::move(requestOptions)));
    }

    // Downloads the range into `destination`. The HTTP client delivers the body as a string, so the bytes are
    // copied once from that string into the span (not zero-copy). The span must stay valid until the operation
    // completes. A range larger than the span fails; BytesWritten reports the bytes stored when the blob ends
    // before the requested range does.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobToResult>>
    DownloadToAsync(std::span<char> destination, DownloadBlobOptions options,
                    std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobToSpanAsync(*m_httpClient, m_target,
                                                Private::ToDownloadToOptions(std::move(options)), destination,
                                                Core::Resolve(std::move(requestOptions)));
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobToResult>>
    DownloadToAsync(std::filesystem::path path, DownloadBlobOptions options,
                    std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobToFileAsync(*m_httpClient, m_target,
                                                Private::ToDownloadToOptions(std::move(options)), std::move(path),
                                                Core::Resolve(std::move(requestOptions)));
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DeleteBlobResult>>
    DeleteAsync(DeleteBlobOptions options = {}, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DeleteImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                          Core::Resolve(std::move(requestOptions)), false);
    }

    // Suppressed-error contract: a not-found response is not a failure here. The expected is engaged, the
    // Value() is default-constructed, and Response::Error() carries the suppressed service error; any other
    // failure arrives as `unexpected(BlobStorageError)`. Check Response::Error() before assuming deletion.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DeleteBlobResult>>
    DeleteIfExistsAsync(DeleteBlobOptions options = {},
                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DeleteImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                          Core::Resolve(std::move(requestOptions)), true);
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::BlobProperties>>
    GetPropertiesAsync(GetBlobPropertiesOptions options = {},
                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return GetPropertiesImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                                 Core::Resolve(std::move(requestOptions)));
    }

    // Resolves to `true`/`false` for existence. Only a not-found response yields `false`; authentication,
    // throttling, timeout and transport failures still produce `unexpected(BlobStorageError)`.
    [[nodiscard]] boost::asio::awaitable<BlobResult<bool>>
    ExistsAsync(std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ExistsImpl(*m_httpClient, m_target, m_httpOwner, Core::Resolve(std::move(requestOptions)));
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::SetBlobMetadataResult>>
    SetMetadataAsync(SetBlobMetadataOptions options, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return SetMetadataImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                               Core::Resolve(std::move(requestOptions)));
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::AcquireBlobLeaseResult>>
    AcquireLeaseAsync(AcquireLeaseOptions options = {},
                      std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return AcquireLeaseImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                                Core::Resolve(std::move(requestOptions)));
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::RenewBlobLeaseResult>>
    RenewLeaseAsync(RenewLeaseOptions options, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return RenewLeaseImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                              Core::Resolve(std::move(requestOptions)));
    }

    // If a retried release gets 409 LeaseNotPresentWithLeaseOperation (the lost earlier attempt already released
    // the lease) it completes as success with the 409 details in Response::Error(); on the first attempt it fails.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::ReleaseBlobLeaseResult>>
    ReleaseLeaseAsync(ReleaseLeaseOptions options, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ReleaseLeaseImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                                Core::Resolve(std::move(requestOptions)));
    }

  protected:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    // Shares the container client's validated connection state (no re-normalisation).
    BasicBlobClient(THttp& httpClient, std::shared_ptr<const Target> target, std::shared_ptr<THttp> owner = nullptr)
        : Core(httpClient, std::move(target), std::move(owner)) {}

    using Core::Resolve;
    [[nodiscard]] THttp& Http() const noexcept AVEVA_LIFETIMEBOUND { return *m_httpClient; }
    [[nodiscard]] const std::shared_ptr<THttp>& HttpOwner() const noexcept AVEVA_LIFETIMEBOUND { return m_httpOwner; }
    [[nodiscard]] const Target& GetTarget() const noexcept AVEVA_LIFETIMEBOUND { return *m_target; }
    [[nodiscard]] const std::shared_ptr<const Target>& SharedTarget() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target;
    }

  private:
    using Core::m_httpClient;
    using Core::m_httpOwner;
    using Core::m_target;

    // The operation bodies are static coroutines whose frames own the target and the HTTP client's owner, so a
    // running operation does not depend on this client object.
    static boost::asio::awaitable<BlobResult<Models::DeleteBlobResult>>
    DeleteImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
               DeleteBlobOptions options, AzureRequestOptions requestOptions, bool ifExists) {
        if (ifExists) {
            co_return co_await Private::DeleteBlobAsync(
                http, *target, std::move(options), std::move(requestOptions), true,
                {BlobStorageErrorCode::BlobNotFound, BlobStorageErrorCode::ResourceNotFound});
        }
        co_return co_await Private::DeleteBlobAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    static boost::asio::awaitable<BlobResult<Models::BlobProperties>>
    GetPropertiesImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                      GetBlobPropertiesOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::GetBlobPropertiesAsync(http, *target, std::move(options),
                                                           std::move(requestOptions));
    }

    static boost::asio::awaitable<BlobResult<bool>> ExistsImpl(THttp& http, std::shared_ptr<const Target> target,
                                                               std::shared_ptr<THttp> /*owner*/,
                                                               AzureRequestOptions requestOptions) {
        co_return co_await Private::ExistsBlobAsync(http, *target, std::move(requestOptions));
    }

    static boost::asio::awaitable<BlobResult<Models::SetBlobMetadataResult>>
    SetMetadataImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                    SetBlobMetadataOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::SetBlobMetadataAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    static boost::asio::awaitable<BlobResult<Models::AcquireBlobLeaseResult>>
    AcquireLeaseImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                     AcquireLeaseOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::AcquireLeaseAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    static boost::asio::awaitable<BlobResult<Models::RenewBlobLeaseResult>>
    RenewLeaseImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                   RenewLeaseOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::RenewLeaseAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    static boost::asio::awaitable<BlobResult<Models::ReleaseBlobLeaseResult>>
    ReleaseLeaseImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                     ReleaseLeaseOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::ReleaseLeaseAsync(http, *target, std::move(options), std::move(requestOptions));
    }
};

// The production client type.
using BlobClient = BasicBlobClient<HttpClient>;
// Covers only non-template members; the coroutine members are instantiated in each consumer.
extern template class BasicBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
