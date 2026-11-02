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

/// <summary>
/// The result of every blob operation.
/// </summary>
/// <typeparam name="T">The operation-specific value of a successful result.</typeparam>
template <class T> using BlobResult = std::expected<Response<T>, BlobStorageError>;

/// <summary>
/// Operations common to every blob type; PageBlobClient derives from it. Usable on its own when the blob
/// type is irrelevant.
/// </summary>
/// <remarks>
/// Every operation is a C++20 coroutine: <c>co_await</c> it (or <c>co_spawn</c> it) and it yields
/// std::expected&lt;Response&lt;T&gt;, BlobStorageError&gt;. Argument validation failures and every other error are
/// reported through that result, never thrown.
///
/// Thread safety: distinct instances are independent; concurrent operations on one instance are supported
/// (connection state is immutable once constructed). Span arguments are not synchronized by the library and must
/// not be touched by the caller until the operation completes.
///
/// Lifetime: the arguments, this client's target and request options are captured when the operation is called, so
/// destroying or moving from this BlobClient afterwards is fine. The HTTP client (and its io_context) is borrowed
/// and must outlive the operation, unless this client was built from a std::shared_ptr to it (then every operation
/// shares ownership). Span arguments and caller-owned objects passed by reference must outlive the operation.
///
/// Cancellation: cancel the coroutine (for example through a cancellation slot bound to the completion token of
/// co_spawn). Only terminal and total cancellation abort an operation; a partial cancellation request is ignored.
/// An aborted operation completes with std::errc::operation_canceled.
/// </remarks>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicBlobClient : Private::ClientCore<THttp, Private::BasicBlobTarget<TCred>> {
    using Core = Private::ClientCore<THttp, Private::BasicBlobTarget<TCred>>;

  public:
    /// <summary>The immutable description of the blob this client operates on.</summary>
    using Target = Private::BasicBlobTarget<TCred>;

    /// <summary>
    /// Creates a client that borrows the HTTP client.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with; it must outlive this client and its
    /// operations.</param>
    /// <param name="options">The blob, endpoint and authentication settings.</param>
    /// <param name="credential">The token credential to authorize with; null when the options carry the
    /// authentication (SAS, Shared Key or a static bearer token).</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="options"/> are invalid.</exception>
    BasicBlobClient(THttp& httpClient, const BlobClientOptions& options, std::shared_ptr<TCred> credential = nullptr)
        : Core(httpClient, std::make_shared<const Target>(Private::MakeBlobTarget(options, std::move(credential)))) {}

    /// <summary>
    /// Creates a client that shares ownership of the HTTP client for as long as this client or any operation started
    /// from it exists.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with.</param>
    /// <param name="options">The blob, endpoint and authentication settings.</param>
    /// <param name="credential">The token credential to authorize with; null when the options carry the
    /// authentication (SAS, Shared Key or a static bearer token).</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="httpClient"/> is null or
    /// <paramref name="options"/> are invalid.</exception>
    BasicBlobClient(std::shared_ptr<THttp> httpClient, const BlobClientOptions& options,
                    std::shared_ptr<TCred> credential = nullptr)
        : Core(std::move(httpClient),
               std::make_shared<const Target>(Private::MakeBlobTarget(options, std::move(credential)))) {}
    /// <summary>Not copyable: a client owns its target.</summary>
    BasicBlobClient(const BasicBlobClient&) = delete;
    /// <summary>Not copy-assignable.</summary>
    BasicBlobClient& operator=(const BasicBlobClient&) = delete;
    /// <summary>Moves the client; the moved-from client must not be used.</summary>
    BasicBlobClient(BasicBlobClient&&) noexcept = default;
    /// <summary>Move-assigns the client; the moved-from client must not be used.</summary>
    BasicBlobClient& operator=(BasicBlobClient&&) noexcept = default;
    /// <summary>Destroys the client; operations already started keep their own share of the target.</summary>
    ~BasicBlobClient() = default;

    /// <summary>
    /// The executor of the underlying HTTP client.
    /// </summary>
    using executor_type = typename THttp::executor_type;
    using Core::get_executor;
    using Core::GetDefaultRequestOptions;

    /// <summary>
    /// The blob's URL carrying the client's SAS token when it has one, so it is a secret: use it as the source of a
    /// server-side copy, never as a key or in a log line (use ResourceUrl() for those).
    /// </summary>
    /// <returns>The blob's URL including any SAS query string.</returns>
    [[nodiscard]] std::string Url() const { return Private::BuildBlobUrl(*m_target); }

    /// <summary>
    /// The blob's URL without any query string (no SAS), stable for the blob's lifetime and safe to log or to use as
    /// an identity key.
    /// </summary>
    /// <returns>The URL; valid for as long as this client.</returns>
    [[nodiscard]] const std::string& ResourceUrl() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target->EncodedResource;
    }

    /// <summary>
    /// Whether Url() carries a SAS token, so the blob can be named as a copy source without any other
    /// authorization.
    /// </summary>
    /// <returns><c>CredentialPresence::Present</c> when the client has a SAS token.</returns>
    [[nodiscard]] CredentialPresence SasToken() const noexcept {
        return m_target->Connection->SasToken.empty() ? CredentialPresence::Absent : CredentialPresence::Present;
    }

    /// <summary>
    /// Whether this client authorizes with a bearer token that UploadPagesFromUrlOptions::SourceAuth can forward
    /// to the service. A SAS or Shared Key client cannot forward anything, so a copy from a source without a SAS is
    /// bound to be refused.
    /// </summary>
    /// <returns><c>CredentialPresence::Present</c> when the client has a token credential to forward.</returns>
    [[nodiscard]] CredentialPresence ForwardableCredential() const noexcept {
        return Private::PresenceOf(m_target->Credential);
    }

    /// <summary>
    /// Downloads the blob, or a range of it, into memory.
    /// </summary>
    /// <param name="options">The range and conditions of the download.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The content and properties of the blob, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobResult>>
    DownloadAsync(DownloadBlobOptions options = {}, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobAsync(*m_httpClient, m_target, std::move(options),
                                          Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Chunked download into <paramref name="path"/>, via a temporary sibling file that replaces
    /// <paramref name="path"/> only on success.
    /// </summary>
    /// <remarks>
    /// DownloadToAsync overload set (the first argument picks the destination, the options type picks the mode):
    /// <list type="bullet">
    /// <item>(path, DownloadToOptions) - whole blob (or DownloadToOptions::Range), chunked and optionally
    /// parallel.</item>
    /// <item>(path, DownloadBlobOptions) - honours DownloadBlobOptions::Range; goes through the same chunked
    /// engine.</item>
    /// <item>(span, DownloadBlobOptions) - the range straight into caller memory.</item>
    /// </list>
    /// File writes never run on the threads completing HTTP requests: they go to DownloadToOptions::WriteScheduler,
    /// else the transport's GetDownloadWriter() if it has one (::AVEVA::HttpClient does not), else the download fails
    /// with operation_not_supported.
    /// </remarks>
    /// <param name="path">The file to write the blob to.</param>
    /// <param name="options">The range, chunking and parallelism of the download.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The number of bytes written and the blob's properties, or the error that failed the download.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobToResult>>
    DownloadToAsync(std::filesystem::path path, DownloadToOptions options = {},
                    std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobToFileAsync(*m_httpClient, m_target, std::move(options), std::move(path),
                                                Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Downloads the range into <paramref name="destination"/>. The HTTP client delivers the body as a string, so
    /// the bytes are copied once from that string into the span (not zero-copy). The span must stay valid until the
    /// operation completes. A range larger than the span fails; BytesWritten reports the bytes stored when the blob
    /// ends before the requested range does.
    /// </summary>
    /// <param name="destination">The memory to store the bytes in.</param>
    /// <param name="options">The range and conditions of the download.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The number of bytes stored and the blob's properties, or the error that failed the download.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobToResult>>
    DownloadToAsync(std::span<char> destination, DownloadBlobOptions options,
                    std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobToSpanAsync(*m_httpClient, m_target,
                                                Private::ToDownloadToOptions(std::move(options)), destination,
                                                Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Chunked download of the range selected by <paramref name="options"/> into <paramref name="path"/>; see the
    /// path overload taking DownloadToOptions for how the file is replaced.
    /// </summary>
    /// <param name="path">The file to write the blob to.</param>
    /// <param name="options">The range and conditions of the download.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The number of bytes written and the blob's properties, or the error that failed the download.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DownloadBlobToResult>>
    DownloadToAsync(std::filesystem::path path, DownloadBlobOptions options,
                    std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DownloadBlobToFileAsync(*m_httpClient, m_target,
                                                Private::ToDownloadToOptions(std::move(options)), std::move(path),
                                                Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Deletes the blob.
    /// </summary>
    /// <param name="options">The conditions of the deletion.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The outcome of the deletion, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DeleteBlobResult>>
    DeleteAsync(DeleteBlobOptions options = {}, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DeleteImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                          Core::Resolve(std::move(requestOptions)), MissingTarget::Fail);
    }

    /// <summary>
    /// Deletes the blob unless it does not exist.
    /// </summary>
    /// <remarks>
    /// Suppressed-error contract: a not-found response is not a failure here. The expected is engaged, the
    /// Value() is default-constructed, and Response::Error() carries the suppressed service error; any other
    /// failure arrives as <c>unexpected(BlobStorageError)</c>. Check Response::Error() before assuming deletion.
    /// </remarks>
    /// <param name="options">The conditions of the deletion.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The outcome of the deletion, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DeleteBlobResult>>
    DeleteIfExistsAsync(DeleteBlobOptions options = {},
                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DeleteImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                          Core::Resolve(std::move(requestOptions)), MissingTarget::Tolerate);
    }

    /// <summary>
    /// Reads the blob's properties without its content.
    /// </summary>
    /// <param name="options">The conditions of the request.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The blob's properties, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::BlobProperties>>
    GetPropertiesAsync(GetBlobPropertiesOptions options = {},
                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return GetPropertiesImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                                 Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Checks whether the blob exists. Only a not-found response yields Absent; authentication,
    /// throttling, timeout and transport failures still produce <c>unexpected(BlobStorageError)</c>.
    /// </summary>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns><c>Existence::Present</c> or <c>Existence::Absent</c>, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Existence>>
    ExistsAsync(std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ExistsImpl(*m_httpClient, m_target, m_httpOwner, Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Replaces the blob's metadata.
    /// </summary>
    /// <param name="options">The new metadata and the conditions of the request.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The blob's new ETag and last-modified time, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::SetBlobMetadataResult>>
    SetMetadataAsync(SetBlobMetadataOptions options, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return SetMetadataImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                               Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Acquires a lease on the blob.
    /// </summary>
    /// <param name="options">The lease duration, proposed lease id and conditions.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The lease acquired, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::AcquireBlobLeaseResult>>
    AcquireLeaseAsync(AcquireLeaseOptions options = {},
                      std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return AcquireLeaseImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                                Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Renews a lease on the blob.
    /// </summary>
    /// <param name="options">The lease id to renew and the conditions.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The renewed lease, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::RenewBlobLeaseResult>>
    RenewLeaseAsync(RenewLeaseOptions options, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return RenewLeaseImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                              Core::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Releases a lease on the blob. If a retried release gets 409 LeaseNotPresentWithLeaseOperation (the lost
    /// earlier attempt already released the lease) it completes as success with the 409 details in
    /// Response::Error(); on the first attempt it fails.
    /// </summary>
    /// <param name="options">The lease id to release and the conditions.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time after the release, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::ReleaseBlobLeaseResult>>
    ReleaseLeaseAsync(ReleaseLeaseOptions options, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ReleaseLeaseImpl(*m_httpClient, m_target, m_httpOwner, std::move(options),
                                Core::Resolve(std::move(requestOptions)));
    }

  protected:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    /// <summary>
    /// Creates a client for a target already validated by the container client that makes it.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with.</param>
    /// <param name="target">The validated blob target; shares the container client's connection state (no
    /// re-normalisation).</param>
    /// <param name="owner">The shared owner of the HTTP client; null when it is only borrowed.</param>
    BasicBlobClient(THttp& httpClient, std::shared_ptr<const Target> target, std::shared_ptr<THttp> owner = nullptr)
        : Core(httpClient, std::move(target), std::move(owner)) {}

    using Core::Resolve;
    /// <summary>The HTTP client operations send requests with.</summary>
    /// <returns>The HTTP client; valid for as long as this client.</returns>
    [[nodiscard]] THttp& Http() const noexcept AVEVA_LIFETIMEBOUND { return *m_httpClient; }
    /// <summary>The shared owner of the HTTP client, for operations that must keep it alive.</summary>
    /// <returns>The owner; null when the HTTP client is only borrowed.</returns>
    [[nodiscard]] const std::shared_ptr<THttp>& HttpOwner() const noexcept AVEVA_LIFETIMEBOUND { return m_httpOwner; }
    /// <summary>The blob this client operates on.</summary>
    /// <returns>The target; valid for as long as this client.</returns>
    [[nodiscard]] const Target& GetTarget() const noexcept AVEVA_LIFETIMEBOUND { return *m_target; }
    /// <summary>The blob this client operates on, shared so that an operation can own it.</summary>
    /// <returns>The shared target.</returns>
    [[nodiscard]] const std::shared_ptr<const Target>& SharedTarget() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target;
    }

  private:
    using Core::m_httpClient;
    using Core::m_httpOwner;
    using Core::m_target;

    /// <summary>The body of DeleteAsync and DeleteIfExistsAsync.</summary>
    /// <remarks>
    /// The operation bodies are static coroutines whose frames own the target and the HTTP client's owner, so a
    /// running operation does not depend on this client object.
    /// </remarks>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The blob to delete.</param>
    /// <param name="options">The deletion options.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <param name="missingTarget">Whether a blob that does not exist counts as success.</param>
    /// <returns>The outcome of the deletion, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::DeleteBlobResult>>
    DeleteImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
               DeleteBlobOptions options, AzureRequestOptions requestOptions, MissingTarget missingTarget) {
        if (missingTarget == MissingTarget::Tolerate) {
            co_return co_await Private::DeleteBlobAsync(
                http, *target, std::move(options), std::move(requestOptions), ReplaySafety::Safe,
                {BlobStorageErrorCode::BlobNotFound, BlobStorageErrorCode::ResourceNotFound});
        }
        co_return co_await Private::DeleteBlobAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    /// <summary>The body of GetPropertiesAsync.</summary>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The blob to query.</param>
    /// <param name="options">The request options.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>The blob's properties, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::BlobProperties>>
    GetPropertiesImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                      GetBlobPropertiesOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::GetBlobPropertiesAsync(http, *target, std::move(options),
                                                           std::move(requestOptions));
    }

    /// <summary>The body of ExistsAsync.</summary>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The blob to look for.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>Whether the blob exists, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Existence>> ExistsImpl(THttp& http, std::shared_ptr<const Target> target,
                                                                    std::shared_ptr<THttp> /*owner*/,
                                                                    AzureRequestOptions requestOptions) {
        co_return co_await Private::ExistsBlobAsync(http, *target, std::move(requestOptions));
    }

    /// <summary>The body of SetMetadataAsync.</summary>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The blob to change.</param>
    /// <param name="options">The new metadata and conditions.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>The blob's new ETag and last-modified time, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::SetBlobMetadataResult>>
    SetMetadataImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                    SetBlobMetadataOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::SetBlobMetadataAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    /// <summary>The body of AcquireLeaseAsync.</summary>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The blob to lease.</param>
    /// <param name="options">The lease options.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>The lease acquired, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::AcquireBlobLeaseResult>>
    AcquireLeaseImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                     AcquireLeaseOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::AcquireLeaseAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    /// <summary>The body of RenewLeaseAsync.</summary>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The leased blob.</param>
    /// <param name="options">The lease to renew and conditions.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>The renewed lease, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::RenewBlobLeaseResult>>
    RenewLeaseImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                   RenewLeaseOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::RenewLeaseAsync(http, *target, std::move(options), std::move(requestOptions));
    }

    /// <summary>The body of ReleaseLeaseAsync.</summary>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The leased blob.</param>
    /// <param name="options">The lease to release and conditions.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>The ETag and last-modified time after the release, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::ReleaseBlobLeaseResult>>
    ReleaseLeaseImpl(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/,
                     ReleaseLeaseOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::ReleaseLeaseAsync(http, *target, std::move(options), std::move(requestOptions));
    }
};

/// <summary>The production client type.</summary>
using BlobClient = BasicBlobClient<HttpClient>;
/// <summary>
/// Explicit instantiation declaration of the production client. Covers only non-template members; the coroutine
/// members are instantiated in each consumer.
/// </summary>
extern template class BasicBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
