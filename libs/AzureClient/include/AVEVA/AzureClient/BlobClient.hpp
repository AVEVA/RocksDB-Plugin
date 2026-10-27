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
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/async_result.hpp>

#include <concepts>
#include <expected>
#include <filesystem>
#include <functional>
#include <gsl/pointers>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient {

// Operations common to every blob type; PageBlobClient derives from it. Usable on its own when the blob
// type is irrelevant.
//
// Thread safety: distinct instances are independent; concurrent ...Async calls on one instance are
// supported (connection state is immutable once constructed). Span arguments are not
// synchronized by the library and must not be touched by the caller until the operation completes.
//
// Completion contract: a handler is invoked exactly once and never from inside the initiating call, including
// when the arguments are invalid or the operation fails before any request is sent: such failures are reported
// through the handler (a BlobStorageError), later, rather than thrown. That is a statement about the initiating
// call stack only: a request can complete on another I/O thread while the initiating thread has not yet returned,
// so shared state must be synchronized by the caller. The handler runs on its associated executor (else the HTTP
// client's executor); when that executor is a strand or belongs to another io_context, the handler and an adopted
// cancellation slot are only touched on it, and the client keeps both contexts' work alive until delivery. If an
// executor keeps rejecting work, the handler still runs once on the completing thread instead of being lost.
// Handlers are not serialized with each other unless their executor is a strand.
//
// Lifetime: operations borrow the HTTP client and never own it, unless this client was built from a
// std::shared_ptr to it (then this client, and every client or deferred operation made from it, shares ownership).
// The HTTP client (and its io_context) must outlive every in-flight ...Async operation, so keep it, or a shared
// owner, alive until the completion handler has run. In-flight operations snapshot what they need from this
// client, so destroying or moving from this BlobClient itself after initiating is fine. A deferred operation
// (omitted token) likewise snapshots this client's state when created, so it stays valid if the client is moved
// from or destroyed before the operation is started. Span arguments and caller-owned objects passed by
// reference must still outlive the operation.
//
// Cancellation: AzureRequestOptions carries a boost::asio::cancellation_slot that aborts in-flight work
// (completing with an operation_aborted-flavored BlobStorageError); a slot associated with the
// completion token is adopted when AzureRequestOptions has none. Only terminal and total cancellation
// abort an operation; a partial cancellation request is ignored.
//
// Completion tokens: every ...Async operation accepts any Boost.Asio CompletionToken (callback,
// use_future, use_awaitable, ...) and completes with std::expected<Response<T>, BlobStorageError>.
// Omitting the token yields a deferred, directly co_await-able operation (DefaultCompletionToken).
// Overloads that also take an options struct exclude it from the defaulted token via `requires`.
template <HttpClientLike THttp, TokenCredentialLike TCred> class BasicBlobClient {
  public:
    BasicBlobClient(THttp& httpClient, const BlobClientOptions& options, std::shared_ptr<TCred> credential = nullptr)
        : m_httpClient(&httpClient), m_target(std::make_shared<const Private::BasicBlobTarget<TCred>>(
                                         Private::MakeBlobTarget(options, std::move(credential)))) {}
    // Shares ownership of the HTTP client for as long as this client or any client made from it exists. Operations
    // already in flight borrow the HTTP client without owning it, so keep a client (or the shared_ptr) alive until they
    // complete.
    BasicBlobClient(std::shared_ptr<THttp> httpClient, const BlobClientOptions& options,
                    std::shared_ptr<TCred> credential = nullptr)
        : BasicBlobClient(Private::RequireHttpClient(httpClient), options, std::move(credential)) {
        m_httpOwner = std::move(httpClient);
    }
    BasicBlobClient(const BasicBlobClient&) = delete;
    BasicBlobClient& operator=(const BasicBlobClient&) = delete;
    BasicBlobClient(BasicBlobClient&&) noexcept = default;
    BasicBlobClient& operator=(BasicBlobClient&&) noexcept = default;
    ~BasicBlobClient() = default;

    // The executor of the underlying HTTP client (the default executor for completions).
    using executor_type = typename THttp::executor_type;

    [[nodiscard]] executor_type get_executor() const { return m_httpClient->get_executor(); }

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

    // Request options (timeout, response body limit, cancellation slot) applied to every operation
    // that is not given its own via WithRequestOptions(...) or the trailing requestOptions argument.
    [[nodiscard]] const AzureRequestOptions& GetDefaultRequestOptions() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target->Connection->DefaultRequestOptions;
    }

    using DefaultCompletionToken = boost::asio::default_completion_token<executor_type>::type;

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DownloadBlobOptions>)
    [[nodiscard]] auto DownloadAsync(CompletionToken&& token = CompletionToken{},
                                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DownloadAsync(DownloadBlobOptions{}, std::forward<CompletionToken>(token), std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadAsync(DownloadBlobOptions options, CompletionToken&& token = CompletionToken{},
                                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobResult>(
            this, [](auto& client, auto&&... args) { client.DownloadAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // DownloadToAsync overload set (the first argument picks the destination, the options type picks the mode):
    //   (path, DownloadToOptions)   - whole blob (or DownloadToOptions::Range), chunked and optionally parallel.
    //   (path, token)               - whole blob with default DownloadToOptions, chunked.
    //   (path, DownloadBlobOptions) - honours DownloadBlobOptions::Range; goes through the same chunked engine.
    //   (span, DownloadBlobOptions) - the range straight into caller memory.
    // File writes never run on the threads completing HTTP requests: they go to DownloadToOptions::WriteScheduler,
    // else the HTTP client's     GetDownloadWriter() if it has one (::AVEVA::HttpClient's is a bounded, client-owned
    // pool), else the download fails with operation_not_supported. To cancel, emit the signal from the
    // handler's executor/strand.

    // Chunked download into `path`, via a temporary sibling file that replaces `path` only on success.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path, DownloadToOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobToResult>(
            this,
            [](auto& client, auto&&... args) { client.DownloadToFileAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), path, std::move(options));
    }

    // Downloads the range into `destination`. The HTTP client delivers the body as a string, so the bytes are
    // copied once from that string into the span (not zero-copy). The span must stay valid until the operation
    // completes. A range larger than the span fails; BytesWritten reports the bytes stored when the blob ends
    // before the requested range does.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadToAsync(std::span<char> destination, DownloadBlobOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobToResult>(
            this,
            [](auto& client, auto&&... args) {
                client.DownloadRangeToSpanAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), destination, std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DownloadBlobOptions> &&
                 !std::same_as<std::remove_cvref_t<CompletionToken>, DownloadToOptions>)
    [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path, CompletionToken&& token = CompletionToken{},
                                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DownloadToAsync(path, DownloadToOptions{}, std::forward<CompletionToken>(token),
                               std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path, DownloadBlobOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobToResult>(
            this,
            [](auto& client, auto&&... args) {
                client.DownloadRangeToFileAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), path, std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DeleteBlobOptions>)
    [[nodiscard]] auto DeleteAsync(CompletionToken&& token = CompletionToken{},
                                   std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DeleteAsync(DeleteBlobOptions{}, std::forward<CompletionToken>(token), std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DeleteAsync(DeleteBlobOptions options, CompletionToken&& token = CompletionToken{},
                                   std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DeleteBlobResult>(
            this, [](auto& client, auto&&... args) { client.DeleteAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DeleteIfExistsAsync(CompletionToken&& token = CompletionToken{},
                                           std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DeleteIfExistsAsync(DeleteBlobOptions{}, std::forward<CompletionToken>(token),
                                   std::move(requestOptions));
    }

    // Suppressed-error contract: a not-found response is not a failure here. The expected is engaged, the
    // Value() is default-constructed, and Response::Error() carries the suppressed service error; any other
    // failure arrives as `unexpected(BlobStorageError)`. Check Response::Error() before assuming deletion.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DeleteIfExistsAsync(DeleteBlobOptions options, CompletionToken&& token = CompletionToken{},
                                           std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DeleteBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.DeleteIfExistsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, GetBlobPropertiesOptions>)
    [[nodiscard]] auto GetPropertiesAsync(CompletionToken&& token = CompletionToken{},
                                          std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return GetPropertiesAsync(GetBlobPropertiesOptions{}, std::forward<CompletionToken>(token),
                                  std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto GetPropertiesAsync(GetBlobPropertiesOptions options, CompletionToken&& token = CompletionToken{},
                                          std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::BlobProperties>(
            this,
            [](auto& client, auto&&... args) { client.GetPropertiesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // Resolves to `true`/`false` for existence. Only a not-found response yields `false`; authentication,
    // throttling, timeout and transport failures still produce `unexpected(BlobStorageError)`.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ExistsAsync(CompletionToken&& token = CompletionToken{},
                                   std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<bool>(
            this, [](auto& client, auto&&... args) { client.ExistsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto SetMetadataAsync(SetBlobMetadataOptions options, CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::SetBlobMetadataResult>(
            this,
            [](auto& client, auto&&... args) { client.SetMetadataAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, AcquireLeaseOptions>)
    [[nodiscard]] auto AcquireLeaseAsync(CompletionToken&& token = CompletionToken{},
                                         std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return AcquireLeaseAsync(AcquireLeaseOptions{}, std::forward<CompletionToken>(token),
                                 std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto AcquireLeaseAsync(AcquireLeaseOptions options, CompletionToken&& token = CompletionToken{},
                                         std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::AcquireBlobLeaseResult>(
            this,
            [](auto& client, auto&&... args) { client.AcquireLeaseAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto RenewLeaseAsync(RenewLeaseOptions options, CompletionToken&& token = CompletionToken{},
                                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::RenewBlobLeaseResult>(
            this,
            [](auto& client, auto&&... args) { client.RenewLeaseAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // If a retried release gets 409 LeaseNotPresentWithLeaseOperation (the lost earlier attempt already released
    // the lease) it completes as success with the 409 details in Response::Error(); on the first attempt it fails.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ReleaseLeaseAsync(ReleaseLeaseOptions options, CompletionToken&& token = CompletionToken{},
                                         std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ReleaseBlobLeaseResult>(
            this,
            [](auto& client, auto&&... args) { client.ReleaseLeaseAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

  protected:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    friend struct Private::ClientSnapshotAccess;
    // Shares the container client's validated connection state (no re-normalisation).
    BasicBlobClient(THttp& httpClient, std::shared_ptr<const Private::BasicBlobTarget<TCred>> target,
                    std::shared_ptr<THttp> owner = nullptr)
        : m_httpClient(&httpClient), m_httpOwner(std::move(owner)), m_target(std::move(target)) {}

    // Connection state owned by this client; derived blob-type clients read it to build requests.
    [[nodiscard]] THttp& Http() const noexcept AVEVA_LIFETIMEBOUND { return *m_httpClient; }
    [[nodiscard]] const std::shared_ptr<THttp>& HttpOwner() const noexcept AVEVA_LIFETIMEBOUND { return m_httpOwner; }

    [[nodiscard]] const Private::BasicBlobTarget<TCred>& Target() const noexcept AVEVA_LIFETIMEBOUND {
        return *m_target;
    }

    // A client sharing this one's state, owned by a deferred operation so it survives the original being moved from.
    [[nodiscard]] BasicBlobClient MakeSnapshot() const { return BasicBlobClient{*m_httpClient, m_target, m_httpOwner}; }

    // For operations that outlive the call and must keep the target alive.
    [[nodiscard]] const std::shared_ptr<const Private::BasicBlobTarget<TCred>>&
    SharedTarget() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target;
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    // Set when the client was given shared ownership of the HTTP client; null when it only borrows it.
    std::shared_ptr<THttp> m_httpOwner;
    std::shared_ptr<const Private::BasicBlobTarget<TCred>> m_target;

    // Operation entry points, templated on the completion handler so it is never type-erased.
    template <class THandler>
    void DownloadAsyncImpl(DownloadBlobOptions options, THandler completion, AzureRequestOptions requestOptions) {
        Private::DownloadBlobAsync(*m_httpClient, m_target, std::move(options), std::move(completion), requestOptions);
    }

    template <class THandler>
    void DownloadToFileAsyncImpl(const std::filesystem::path& path, DownloadToOptions options, THandler completion,
                                 AzureRequestOptions requestOptions) {
        Private::DownloadBlobToFileAsync(*m_httpClient, m_target, std::move(options), path, std::move(completion),
                                         requestOptions);
    }

    template <class THandler>
    void DownloadRangeToSpanAsyncImpl(std::span<char> destination, DownloadBlobOptions options, THandler completion,
                                      AzureRequestOptions requestOptions) {
        Private::DownloadBlobToSpanAsync(*m_httpClient, m_target, Private::ToDownloadToOptions(std::move(options)),
                                         destination, std::move(completion), requestOptions);
    }

    template <class THandler>
    void DownloadRangeToFileAsyncImpl(const std::filesystem::path& path, DownloadBlobOptions options,
                                      THandler completion, AzureRequestOptions requestOptions) {
        DownloadToFileAsyncImpl(path, Private::ToDownloadToOptions(std::move(options)), std::move(completion),
                                requestOptions);
    }

    template <class THandler>
    void DeleteAsyncImpl(const DeleteBlobOptions& options, THandler completion, AzureRequestOptions requestOptions) {
        Private::DeleteBlobAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void DeleteIfExistsAsyncImpl(const DeleteBlobOptions& options, THandler completion,
                                 AzureRequestOptions requestOptions) {
        Private::DeleteBlobAsync(
            *m_httpClient, *m_target, options,
            Private::SuppressErrors<Models::DeleteBlobResult>(std::move(completion), BlobStorageErrorCode::BlobNotFound,
                                                              BlobStorageErrorCode::ResourceNotFound),
            requestOptions, true);
    }

    template <class THandler>
    void GetPropertiesAsyncImpl(const GetBlobPropertiesOptions& options, THandler completion,
                                AzureRequestOptions requestOptions) {
        Private::GetBlobPropertiesAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    template <class THandler> void ExistsAsyncImpl(THandler completion, AzureRequestOptions requestOptions) {
        Private::ExistsBlobAsync(*m_httpClient, *m_target, std::move(completion), requestOptions);
    }

    template <class THandler>
    void SetMetadataAsyncImpl(const SetBlobMetadataOptions& options, THandler completion,
                              AzureRequestOptions requestOptions) {
        Private::SetBlobMetadataAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void AcquireLeaseAsyncImpl(AcquireLeaseOptions options, THandler completion, AzureRequestOptions requestOptions) {
        Private::AcquireLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    template <class THandler>
    void RenewLeaseAsyncImpl(RenewLeaseOptions options, THandler completion, AzureRequestOptions requestOptions) {
        Private::RenewLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    template <class THandler>
    void ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options, THandler completion, AzureRequestOptions requestOptions) {
        Private::ReleaseLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }
};

// The production client type.
using BlobClient = BasicBlobClient<HttpClient>;
// Instantiated once in the library so consumers do not recompile the client for the production transport.
extern template class BasicBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
