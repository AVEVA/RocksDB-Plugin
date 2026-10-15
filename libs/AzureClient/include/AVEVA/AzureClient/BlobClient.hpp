// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
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
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/async_result.hpp>

#include <concepts>
#include <expected>
#include <filesystem>
#include <functional>
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
// Completion handlers are not serialized with each other and may run inline, before the initiating
// call returns, or on any thread that drives the HTTP client.
//
// Lifetime: stores a non-owning reference to the HTTP client, which must outlive this client and every
// in-flight ...Async operation; destroying either before a completion handler runs is undefined behavior.
// A deferred operation (omitted token) snapshots this client's state when created, so it stays valid if the
// client is moved from or destroyed before the operation is started.
//
// Cancellation: HttpRequestOptions carries a boost::asio::cancellation_slot that aborts in-flight work
// (completing with an operation_aborted-flavored BlobStorageError); a slot associated with the
// completion token is adopted when HttpRequestOptions has none.
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
    // Shares ownership of the HTTP client, so this client (and clients made from it) keep it alive.
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

    // The blob's URL, carrying the client's SAS token when it has one. Use it as the source of a server-side copy.
    [[nodiscard]] std::string Url() const { return Private::BuildBlobUrl(*m_target); }

    // Request options (timeout, response body limit, cancellation slot) applied to every operation
    // that is not given its own via WithRequestOptions(...) or the trailing requestOptions argument.
    [[nodiscard]] const HttpRequestOptions& GetDefaultRequestOptions() const noexcept {
        return m_target->Connection->DefaultRequestOptions;
    }

    using DefaultCompletionToken = boost::asio::default_completion_token<executor_type>::type;

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DownloadBlobOptions>)
    [[nodiscard]] auto DownloadAsync(CompletionToken&& token = CompletionToken{},
                                     std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobResult>(
            this, [](auto& client, auto&&... args) { client.DownloadAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), DownloadBlobOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadAsync(DownloadBlobOptions options, CompletionToken&& token = CompletionToken{},
                                     std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobResult>(
            this, [](auto& client, auto&&... args) { client.DownloadAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // DownloadToAsync overload set (the first argument picks the destination, the options type picks the mode):
    //   (path, DownloadToOptions)   - whole blob (or DownloadToOptions::Range), chunked and optionally parallel.
    //   (path, token)               - whole blob with default DownloadToOptions, chunked.
    //   (path, DownloadBlobOptions) - honours DownloadBlobOptions::Range; goes through the same chunked engine.
    //   (span, DownloadBlobOptions) - the range straight into caller memory.
    // File writes run on the client's executor (the threads completing HTTP requests): use a dedicated
    // io_context or a fast disk. To cancel, emit the signal from the handler's executor/strand.

    // Chunked download into `path`, via a temporary sibling file that replaces `path` only on success.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path, DownloadToOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobToResult>(
            this,
            [](auto& client, auto&&... args) { client.DownloadToFileAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), path, std::move(options));
    }

    // Downloads the range straight into `destination`, avoiding an intermediate copy of the body. The span must
    // stay valid until the operation completes. A range larger than the span fails; BytesWritten reports the
    // bytes stored when the blob ends before the requested range does.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadToAsync(std::span<char> destination, DownloadBlobOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
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
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DownloadBlobToResult>(
            this,
            [](auto& client, auto&&... args) { client.DownloadToFileAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), path, DownloadToOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path, DownloadBlobOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
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
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DeleteBlobResult>(
            this, [](auto& client, auto&&... args) { client.DeleteAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), DeleteBlobOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DeleteAsync(DeleteBlobOptions options, CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DeleteBlobResult>(
            this, [](auto& client, auto&&... args) { client.DeleteAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DeleteIfExistsAsync(CompletionToken&& token = CompletionToken{},
                                           std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return DeleteIfExistsAsync(DeleteBlobOptions{}, std::forward<CompletionToken>(token),
                                   std::move(requestOptions));
    }

    // Suppressed-error contract: a not-found response is not a failure here. The expected is engaged, the
    // Value() is default-constructed, and Response::Error() carries the suppressed service error; any other
    // failure arrives as `unexpected(BlobStorageError)`. Check Response::Error() before assuming deletion.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DeleteIfExistsAsync(DeleteBlobOptions options, CompletionToken&& token = CompletionToken{},
                                           std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DeleteBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.DeleteIfExistsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, GetBlobPropertiesOptions>)
    [[nodiscard]] auto GetPropertiesAsync(CompletionToken&& token = CompletionToken{},
                                          std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::BlobProperties>(
            this,
            [](auto& client, auto&&... args) { client.GetPropertiesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), GetBlobPropertiesOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto GetPropertiesAsync(GetBlobPropertiesOptions options, CompletionToken&& token = CompletionToken{},
                                          std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::BlobProperties>(
            this,
            [](auto& client, auto&&... args) { client.GetPropertiesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // Resolves to `true`/`false` for existence. Only a not-found response yields `false`; authentication,
    // throttling, timeout and transport failures still produce `unexpected(BlobStorageError)`.
    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ExistsAsync(CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<bool>(
            this, [](auto& client, auto&&... args) { client.ExistsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto SetMetadataAsync(SetBlobMetadataOptions options, CompletionToken&& token = CompletionToken{},
                                        std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::SetBlobMetadataResult>(
            this,
            [](auto& client, auto&&... args) { client.SetMetadataAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, AcquireLeaseOptions>)
    [[nodiscard]] auto AcquireLeaseAsync(CompletionToken&& token = CompletionToken{},
                                         std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::AcquireBlobLeaseResult>(
            this,
            [](auto& client, auto&&... args) { client.AcquireLeaseAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), AcquireLeaseOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto AcquireLeaseAsync(AcquireLeaseOptions options, CompletionToken&& token = CompletionToken{},
                                         std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::AcquireBlobLeaseResult>(
            this,
            [](auto& client, auto&&... args) { client.AcquireLeaseAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto RenewLeaseAsync(RenewLeaseOptions options, CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::RenewBlobLeaseResult>(
            this,
            [](auto& client, auto&&... args) { client.RenewLeaseAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ReleaseLeaseAsync(ReleaseLeaseOptions options, CompletionToken&& token = CompletionToken{},
                                         std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
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
    [[nodiscard]] THttp& Http() const noexcept { return *m_httpClient; }
    [[nodiscard]] const std::shared_ptr<THttp>& HttpOwner() const noexcept { return m_httpOwner; }

    [[nodiscard]] const Private::BasicBlobTarget<TCred>& Target() const noexcept { return *m_target; }

    // A client sharing this one's state, owned by a deferred operation so it survives the original being moved from.
    [[nodiscard]] BasicBlobClient MakeSnapshot() const { return BasicBlobClient{*m_httpClient, m_target, m_httpOwner}; }

    // For operations that outlive the call and must keep the target alive.
    [[nodiscard]] const std::shared_ptr<const Private::BasicBlobTarget<TCred>>& SharedTarget() const noexcept {
        return m_target;
    }

  private:
    THttp* m_httpClient = nullptr;
    // Set when the client was given shared ownership of the HTTP client; null when it only borrows it.
    std::shared_ptr<THttp> m_httpOwner;
    std::shared_ptr<const Private::BasicBlobTarget<TCred>> m_target;

    // Operation entry points, templated on the completion handler so it is never type-erased.
    template <class THandler>
    void DownloadAsyncImpl(DownloadBlobOptions options, THandler completion, HttpRequestOptions requestOptions) {
        Private::DownloadBlobAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    template <class THandler>
    void DownloadToFileAsyncImpl(const std::filesystem::path& path, DownloadToOptions options, THandler completion,
                                 HttpRequestOptions requestOptions) {
        Private::DownloadBlobToFileAsync(*m_httpClient, *m_target, std::move(options), path, std::move(completion),
                                         requestOptions);
    }

    template <class THandler>
    void DownloadRangeToSpanAsyncImpl(std::span<char> destination, DownloadBlobOptions options, THandler completion,
                                      HttpRequestOptions requestOptions) {
        Private::DownloadBlobToSpanAsync(*m_httpClient, *m_target, Private::ToDownloadToOptions(std::move(options)),
                                         destination, std::move(completion), requestOptions);
    }

    template <class THandler>
    void DownloadRangeToFileAsyncImpl(const std::filesystem::path& path, DownloadBlobOptions options,
                                      THandler completion, HttpRequestOptions requestOptions) {
        DownloadToFileAsyncImpl(path, Private::ToDownloadToOptions(std::move(options)), std::move(completion),
                                requestOptions);
    }

    template <class THandler>
    void DeleteAsyncImpl(const DeleteBlobOptions& options, THandler completion, HttpRequestOptions requestOptions) {
        Private::DeleteBlobAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void DeleteIfExistsAsyncImpl(const DeleteBlobOptions& options, THandler completion,
                                 HttpRequestOptions requestOptions) {
        Private::DeleteBlobAsync(
            *m_httpClient, *m_target, options,
            Private::SuppressErrors<Models::DeleteBlobResult>(
                std::move(completion), {BlobStorageErrorCode::BlobNotFound, BlobStorageErrorCode::ResourceNotFound}),
            requestOptions, true);
    }

    template <class THandler>
    void GetPropertiesAsyncImpl(const GetBlobPropertiesOptions& options, THandler completion,
                                HttpRequestOptions requestOptions) {
        Private::GetBlobPropertiesAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    template <class THandler> void ExistsAsyncImpl(THandler completion, HttpRequestOptions requestOptions) {
        Private::ExistsBlobAsync(*m_httpClient, *m_target, std::move(completion), requestOptions);
    }

    template <class THandler>
    void SetMetadataAsyncImpl(const SetBlobMetadataOptions& options, THandler completion,
                              HttpRequestOptions requestOptions) {
        Private::SetBlobMetadataAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void AcquireLeaseAsyncImpl(AcquireLeaseOptions options, THandler completion, HttpRequestOptions requestOptions) {
        Private::AcquireLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    template <class THandler>
    void RenewLeaseAsyncImpl(RenewLeaseOptions options, THandler completion, HttpRequestOptions requestOptions) {
        Private::RenewLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    template <class THandler>
    void ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options, THandler completion, HttpRequestOptions requestOptions) {
        Private::ReleaseLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }
};

// The production client type.
using BlobClient = BasicBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
