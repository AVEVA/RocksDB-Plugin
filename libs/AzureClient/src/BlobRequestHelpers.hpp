#pragma once

#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <algorithm>
#include <boost/asio/append.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <cstdlib>

#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <array>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Private
{
    // HTTP status codes the client reacts to by name.
    inline constexpr unsigned int HttpStatusPartialContent = 206U;
    inline constexpr unsigned int HttpStatusMultipleChoices = 300U;
    inline constexpr unsigned int HttpStatusNotModified = 304U;
    inline constexpr unsigned int HttpStatusRequestTimeout = 408U;
    inline constexpr unsigned int HttpStatusRangeNotSatisfiable = 416U;
    inline constexpr unsigned int HttpStatusTooManyRequests = 429U;
    inline constexpr unsigned int HttpStatusInternalServerError = 500U;
    inline constexpr unsigned int HttpStatusBadGateway = 502U;
    inline constexpr unsigned int HttpStatusServiceUnavailable = 503U;
    inline constexpr unsigned int HttpStatusGatewayTimeout = 504U;

    // Lease duration limits defined by the Blob service.
    inline constexpr std::chrono::seconds MinFixedLeaseDuration{15};
    inline constexpr std::chrono::seconds MaxFixedLeaseDuration{60};

    // Gives an otherwise-plain string-like parameter a distinct type (tagged by `Tag`, an empty
    // caller-defined struct) so it is never adjacent-and-same-type with another string-like
    // parameter in a function signature (see bugprone-easily-swappable-parameters). Implicitly
    // constructible from anything that already implicitly converts to std::string_view at the call
    // site, so callers are unaffected.
    template <class Tag> struct StringLabel
    {
        std::string_view Value;

        constexpr StringLabel() noexcept = default;

        constexpr StringLabel(const char* value) noexcept : Value(value)
        {
        }

        constexpr StringLabel(std::string_view value) noexcept : Value(value)
        {
        }

        StringLabel(const std::string& value) noexcept : Value(value)
        {
        }
    };

    // Same purpose as StringLabel, but for a numeric parameter of type `T`.
    template <class T, class Tag> struct NumericLabel
    {
        T Value;

        constexpr NumericLabel(T value) noexcept : Value(value)
        {
        }
    };

    // Shared Key request signer, created once per connection: the account key is decoded and
    // an HMAC-SHA256 context keyed with it is initialised at construction, so each signature only
    // duplicates that context instead of re-decoding the key and re-fetching the algorithm (which
    // takes global locks in OpenSSL 3). Thread-safe; shared by every client of a connection.
    class SharedKeySigner
    {
      public:
        // Throws std::invalid_argument if `accountKeyBase64` is not valid base64.
        SharedKeySigner(std::string accountName, std::string_view accountKeyBase64);
        ~SharedKeySigner();
        SharedKeySigner(const SharedKeySigner&) = delete;
        SharedKeySigner& operator=(const SharedKeySigner&) = delete;
        SharedKeySigner(SharedKeySigner&&) = delete;
        SharedKeySigner& operator=(SharedKeySigner&&) = delete;

        [[nodiscard]] const std::string& AccountName() const noexcept
        {
            return m_accountName;
        }

        // Base64 HMAC-SHA256 of `stringToSign`.
        [[nodiscard]] std::string Sign(std::string_view stringToSign) const;
        // The full "SharedKey <account>:<signature>" Authorization header value for `request`.
        [[nodiscard]] std::string Authorize(const HttpRequest& request) const;

      private:
        struct Impl;
        std::string m_accountName;
        std::unique_ptr<Impl> m_impl;
    };

    // The Shared Key string-to-sign for `request` (Blob service, version 2009-09-19 and later).
    [[nodiscard]] std::string BuildSharedKeyStringToSign(std::string_view accountName, const HttpRequest& request);

    // Normalised, validated, immutable connection settings shared by a service client and every
    // container/blob client derived from it.
    struct ConnectionState
    {
        std::string ServiceEndpoint;
        std::string SasToken; // without a leading '?'
        std::string ApiVersion;
        std::shared_ptr<ITokenCredential> TokenCredential; // BearerToken is folded in here
        std::vector<std::string> TokenScopes;
        std::shared_ptr<const SharedKeySigner> Signer; // set iff Shared Key authorization is used

        std::string BasePrefix; // scheme://host[:port]
        std::string BasePath;   // encoded endpoint path without a trailing '/', empty for the root

        RetryOptions Retry;
        HttpRequestOptions DefaultRequestOptions;
    };

    struct ContainerTarget
    {
        std::shared_ptr<const ConnectionState> Connection;
        std::string ContainerName;
    };

    struct BlobTarget
    {
        std::shared_ptr<const ConnectionState> Connection;
        std::string ContainerName;
        std::string BlobName;
    };

    // Normalise and validate client options (throw std::invalid_argument on invalid settings).
    [[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobServiceClientOptions options);
    [[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobContainerClientOptions options);
    [[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobClientOptions options);
    [[nodiscard]] ContainerTarget MakeContainerTarget(std::shared_ptr<const ConnectionState> connection,
        std::string containerName);
    [[nodiscard]] BlobTarget MakeBlobTarget(std::shared_ptr<const ConnectionState> connection,
        std::string containerName,
        std::string blobName);
    [[nodiscard]] ContainerTarget MakeContainerTarget(const BlobContainerClientOptions& options);
    [[nodiscard]] BlobTarget MakeBlobTarget(const BlobClientOptions& options);

    inline constexpr std::string_view XMsVersionHeaderName = "x-ms-version";
    inline constexpr std::string_view XMsDateHeaderName = "x-ms-date";
    inline constexpr std::string_view XMsClientRequestIdHeaderName = "x-ms-client-request-id";
    inline constexpr std::string_view XMsRequestIdHeaderName = "x-ms-request-id";
    inline constexpr std::string_view XMsErrorCodeHeaderName = "x-ms-error-code";
    inline constexpr std::string_view XMsContentCrc64HeaderName = "x-ms-content-crc64";
    inline constexpr std::string_view XMsDeleteTypePermanentHeaderName = "x-ms-delete-type-permanent";
    inline constexpr std::string_view AuthorizationHeaderName = "Authorization";
    inline constexpr std::string_view XMsMetaHeaderPrefix = "x-ms-meta-";
    inline constexpr std::string_view XMsBlobTypeHeaderName = "x-ms-blob-type";
    inline constexpr std::string_view XMsBlobContentLengthHeaderName = "x-ms-blob-content-length";
    inline constexpr std::string_view XMsBlobContentTypeHeaderName = "x-ms-blob-content-type";
    inline constexpr std::string_view XMsBlobContentMd5HeaderName = "x-ms-blob-content-md5";
    inline constexpr std::string_view XMsBlobCacheControlHeaderName = "x-ms-blob-cache-control";
    inline constexpr std::string_view XMsBlobContentEncodingHeaderName = "x-ms-blob-content-encoding";
    inline constexpr std::string_view XMsBlobContentLanguageHeaderName = "x-ms-blob-content-language";
    inline constexpr std::string_view XMsBlobContentDispositionHeaderName = "x-ms-blob-content-disposition";
    inline constexpr std::string_view XMsPageWriteHeaderName = "x-ms-page-write";
    inline constexpr std::string_view XMsRangeHeaderName = "x-ms-range";
    inline constexpr std::string_view XMsAccessTierHeaderName = "x-ms-access-tier";
    inline constexpr std::string_view XMsLeaseIdHeaderName = "x-ms-lease-id";
    inline constexpr std::string_view XMsLeaseActionHeaderName = "x-ms-lease-action";
    inline constexpr std::string_view XMsLeaseDurationHeaderName = "x-ms-lease-duration";
    inline constexpr std::string_view XMsProposedLeaseIdHeaderName = "x-ms-proposed-lease-id";
    inline constexpr std::string_view ETagHeaderName = "ETag";
    inline constexpr std::string_view LastModifiedHeaderName = "Last-Modified";
    inline constexpr std::string_view ContentLengthHeaderName = "Content-Length";
    inline constexpr std::string_view ContentTypeHeaderName = "Content-Type";
    inline constexpr std::string_view ContentMd5HeaderName = "Content-MD5";
    inline constexpr std::string_view CacheControlHeaderName = "Cache-Control";
    inline constexpr std::string_view ContentEncodingHeaderName = "Content-Encoding";
    inline constexpr std::string_view ContentLanguageHeaderName = "Content-Language";
    inline constexpr std::string_view RangeHeaderName = "Range";
    inline constexpr std::string_view IfMatchHeaderName = "If-Match";
    inline constexpr std::string_view IfNoneMatchHeaderName = "If-None-Match";
    inline constexpr std::string_view IfModifiedSinceHeaderName = "If-Modified-Since";
    inline constexpr std::string_view IfUnmodifiedSinceHeaderName = "If-Unmodified-Since";

    struct RequestFailure
    {
        std::error_code Error;
        std::optional<BlobStorageError> Details;
    };

    [[nodiscard]] bool IEquals(std::string_view lhs, std::string_view rhs) noexcept;
    [[nodiscard]] bool IStartsWith(std::string_view value, std::string_view prefix) noexcept;
    [[nodiscard]] std::string ToLowerAscii(std::string_view value);
    [[nodiscard]] std::string_view TrimWhitespace(std::string_view value) noexcept;
    // True if `value` contains CR, LF or NUL (header injection / truncation).
    [[nodiscard]] bool HasHeaderControlCharacters(std::string_view value) noexcept;
    // Throws std::invalid_argument unless `value` is canonical padded base64; returns the decoded length.
    // An empty value is accepted only when `allowEmpty` is set.
    [[nodiscard]] std::size_t ValidateBase64AndGetDecodedLength(std::string_view value, bool allowEmpty);
    [[nodiscard]] std::string_view FindHeaderValue(const HttpResponse& response, std::string_view headerName) noexcept;
    [[nodiscard]] bool ParseBoolHeader(std::string_view value) noexcept;
    [[nodiscard]] Models::LeaseStatus ParseLeaseStatus(std::string_view value);
    [[nodiscard]] Models::LeaseState ParseLeaseState(std::string_view value);
    [[nodiscard]] Models::LeaseDurationType ParseLeaseDurationType(std::string_view value);
    [[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseHttpDateHeader(
        std::string_view value) noexcept;
    [[nodiscard]] std::string BuildDateHeaderValue(std::chrono::system_clock::time_point now);
    [[nodiscard]] std::string_view TrimTrailingSlashes(std::string_view value) noexcept;
    [[nodiscard]] std::string_view TrimLeadingQuestionMark(std::string_view value) noexcept;
    [[nodiscard]] std::string CreateClientRequestId();
    // RFC 7231 date name tables shared by the request formatter and the response parsers.
    inline constexpr std::array<std::string_view, 12>
        MonthNames{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    inline constexpr std::array<std::string_view, 7> ShortWeekdayNames{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    inline constexpr std::array<std::string_view, 7>
        LongWeekdayNames{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

    [[nodiscard]] std::span<char> AsChars(std::span<std::byte> bytes) noexcept;
    [[nodiscard]] std::string_view AsChars(std::span<const std::byte> bytes) noexcept;
    [[nodiscard]] std::string BytesToString(std::span<const std::byte> bytes);
    [[nodiscard]] std::string Base64Encode(std::span<const std::byte> bytes);
    [[nodiscard]] std::string UrlEncode(std::string_view value, std::string_view extraSafeChars);
    [[nodiscard]] std::string BuildQueryString(const std::vector<std::pair<std::string, std::string>>& parameters);
    [[nodiscard]] std::string BuildRangeHeaderValue(const Models::BlobByteRange& range);

    [[nodiscard]] RequestFailure DetermineBlobStorageFailure(std::error_code transportError,
        const HttpResponse& response);
    // A successful response whose content could not be parsed (BlobStorageErrorCode::InvalidResponse).
    [[nodiscard]] RequestFailure MakeInvalidResponseFailure(const HttpResponse& response, std::string message);

    struct ParsedContentRange
    {
        std::uint64_t Start = 0;
        std::uint64_t End = 0;
        std::optional<std::uint64_t> Total;
        // True for "bytes */total" (an unsatisfied range); Start and End carry no meaning then.
        bool Unsatisfied = false;
    };

    [[nodiscard]] std::optional<ParsedContentRange> ParseContentRange(std::string_view header) noexcept;

    [[nodiscard]] Models::BlobProperties ParseBlobProperties(const HttpResponse& response);
    [[nodiscard]] Models::DeleteBlobResult ParseDeleteBlobResult(const HttpResponse& response);
    [[nodiscard]] Models::AcquireBlobLeaseResult ParseAcquireBlobLeaseResult(const HttpResponse& response);
    [[nodiscard]] Models::ReleaseBlobLeaseResult ParseReleaseBlobLeaseResult(const HttpResponse& response);
    [[nodiscard]] Models::RenewBlobLeaseResult ParseRenewBlobLeaseResult(const HttpResponse& response);
    // Parses "YYYY-MM-DDThh:mm:ss[.fffffff]Z"; std::nullopt if malformed.
    [[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseIso8601Utc(std::string_view value) noexcept;

    // Tag for StringLabel to give the CRC64 parameter of ApplyTransactionalHashes its own type.
    struct TransactionalCrc64Tag
    {
    };

    // Content-MD5 / x-ms-content-crc64 request headers for service-side body verification.
    void ApplyTransactionalHashes(HttpRequest& request, std::string_view md5, StringLabel<TransactionalCrc64Tag> crc64);
    [[nodiscard]] Models::ListBlobsResult ParseListBlobsResultXml(std::string_view xml);

    [[nodiscard]] HttpRequest BuildBlobRequest(const BlobTarget& target,
        HttpMethod method,
        std::string_view queryString = {});
    [[nodiscard]] std::string BuildBlobUrl(const BlobTarget& target, std::string_view queryString = {});
    [[nodiscard]] HttpRequest BuildContainerRequest(const ContainerTarget& target,
        HttpMethod method,
        std::string_view queryString = {});
    [[nodiscard]] std::string BuildContainerUrl(const ContainerTarget& target, std::string_view queryString = {});
    [[nodiscard]] std::string BuildServiceUrl(const ConnectionState& connection, std::string_view queryString = {});

    // Convenience overloads that normalise and validate client options first (tests/benchmarks).
    [[nodiscard]] std::string BuildBlobUrl(const BlobClientOptions& options, std::string_view queryString = {});
    [[nodiscard]] HttpRequest BuildBlobRequest(const BlobClientOptions& options,
        HttpMethod method,
        std::string_view queryString = {});
    [[nodiscard]] std::string BuildContainerUrl(const BlobContainerClientOptions& options,
        std::string_view queryString = {});
    [[nodiscard]] HttpRequest BuildContainerRequest(const BlobContainerClientOptions& options,
        HttpMethod method,
        std::string_view queryString = {});
    [[nodiscard]] std::string BuildServiceUrl(const BlobServiceClientOptions& options,
        std::string_view queryString = {});

    void AddHeader(HttpRequest& request, std::string_view name, std::string_view value);
    void AddHeaderIfNotEmpty(HttpRequest& request, std::string_view name, std::string_view value);
    void ApplyBlobRequestConditions(HttpRequest& request, const Models::BlobRequestConditions& conditions);
    void ApplyBlobHttpHeadersForUpload(HttpRequest& request, const Models::BlobHttpHeaders& headers);
    void ApplyMetadata(HttpRequest& request, const Models::MetadataMap& metadata);
    // Azure requires metadata names to be C# identifiers: [A-Za-z_][A-Za-z0-9_]*.
    [[nodiscard]] bool IsValidMetadataName(std::string_view name) noexcept;
    // One-off Shared Key signing (constructs a temporary signer); clients use ConnectionState::Signer.
    void AuthorizeRequest(const SharedKeyCredentialOptions& options, HttpRequest& request);
    void AuthorizeRequest(const SharedKeySigner& signer, HttpRequest& request);
    void ValidateBlockId(std::string_view blockId);

    // Maps the exception being handled to a BlobStorageError; must be called from a catch handler.
    [[nodiscard]] BlobStorageError MakeFailureFromCurrentException();

    // A BlobStorageError carrying only `error` and `message` (defaulting to error.message()).
    [[nodiscard]] inline BlobStorageError MakeClientError(std::error_code error, std::string message = {})
    {
        BlobStorageError details;
        details.Message = message.empty() ? error.message() : std::move(message);
        details.Code = error;
        return details;
    }

    // Folds a transport/service std::error_code into the single std::expected error channel. Success
    // is governed solely by `error`: Exists*/*IfExists* clear it while leaving the suppressed
    // diagnostics (e.g. the 404's request id) available via Response<T>::Error().
    template <class T>
    [[nodiscard]] std::expected<Response<T>, BlobStorageError> ToExpected(std::error_code error, Response<T> response)
    {
        if (!error)
        {
            return response;
        }

        if (response.Error().has_value())
        {
            BlobStorageError details = *response.Error();
            details.Code = error;
            return std::unexpected(std::move(details));
        }

        return std::unexpected(MakeClientError(error));
    }

    // Completion adapter for *IfExists / *IfNotExists: a failure whose code is one of `suppressed` completes as
    // success with the real raw response, keeping the suppressed error's details in Response<T>::Error().
    template <class TModel, class TCompletion>
    [[nodiscard]] auto SuppressErrors(TCompletion completion, std::initializer_list<BlobStorageErrorCode> suppressed)
    {
        return [completion = std::move(completion),
                   codes = std::vector<BlobStorageErrorCode>(suppressed)](std::error_code error,
                   Response<TModel> response) mutable
        {
            if (error && std::ranges::any_of(codes,
                             [&error](BlobStorageErrorCode code)
            {
                return error == code;
            }))
            {
                error.clear();
            }
            completion(ToExpected(error, std::move(response)));
        };
    }

    // A client-side failure (e.g. argument validation) with `message` defaulting to error.message().
    template <class T>
    [[nodiscard]] std::expected<Response<T>, BlobStorageError> MakeError(std::error_code error,
        std::string message = {})
    {
        return std::unexpected(MakeClientError(error, std::move(message)));
    }

    // Parses a completed response with `parser` and completes either with
    // std::expected<Response<TModel>, BlobStorageError> or, if `completion` takes them, with (std::error_code,
    // Response<TModel>).
    template <class TModel, class TParser, class TCompletion>
    void CompleteParsed(std::error_code error, HttpResponse response, TParser&& parser, TCompletion&& completion)
    {
        RequestFailure failure = DetermineBlobStorageFailure(error, response);
        TModel model;
        if (!failure.Error)
        {
            try
            {
                model = std::forward<TParser>(parser)(response);
            }
            catch (const std::exception& exception)
            {
                failure = MakeInvalidResponseFailure(response, exception.what());
            }
        }

        Response<TModel> result{std::move(model), std::move(response), std::move(failure.Details)};
        if constexpr (std::is_invocable_v<TCompletion, std::error_code, Response<TModel>>)
        {
            std::forward<TCompletion>(completion)(failure.Error, std::move(result));
        }
        else
        {
            std::forward<TCompletion>(completion)(ToExpected(failure.Error, std::move(result)));
        }
    }

    template <class T>
        requires requires(T result) {
            result.ETag;
            result.LastModified;
        }
    void ApplyETagAndLastModified(T& result, const HttpResponse& response)
    {
        result.ETag = std::string{FindHeaderValue(response, ETagHeaderName)};
        const std::string_view lastModified = FindHeaderValue(response, LastModifiedHeaderName);
        if (!lastModified.empty())
        {
            const auto parsed = ParseHttpDateHeader(lastModified);
            if (!parsed.has_value())
            {
                throw std::invalid_argument("Last-Modified header must contain a valid HTTP-date.");
            }

            result.LastModified = *parsed;
        }
    }

    template <class T>
        requires requires(T result) {
            result.ETag;
            result.LastModified;
        }
    [[nodiscard]] T ParseETagAndLastModified(const HttpResponse& response)
    {
        T result;
        ApplyETagAndLastModified(result, response);
        return result;
    }

    // Authorization material captured (by value) for the lifetime of a single logical request,
    // including all of its retry attempts. Precedence: SAS (in URL) > SharedKey > TokenCredential.
    struct RequestAuth
    {
        enum class Kind : std::uint8_t
        {
            None,
            SharedKey,
            Token
        };

        Kind Type = Kind::None;
        std::shared_ptr<const SharedKeySigner> Signer;
        std::shared_ptr<ITokenCredential> TokenCredential;
        std::vector<std::string> TokenScopes;
    };

    [[nodiscard]] RequestAuth MakeRequestAuth(const ConnectionState& connection);

    // True for HTTP statuses and transport errors that Azure Storage documents as transient.
    [[nodiscard]] bool IsRetriableTransportError(std::error_code error) noexcept;
    [[nodiscard]] bool IsRetriableStatus(unsigned int status) noexcept;
    [[nodiscard]] bool IsRetriableFailure(std::error_code error, const HttpResponse& response) noexcept;

    // Server-provided retry delay (x-ms-retry-after-ms, then Retry-After in seconds), if any.
    [[nodiscard]] std::optional<std::chrono::milliseconds> ParseRetryAfter(const HttpResponse& response) noexcept;

    // Sends `request` with per-attempt authorization and retry/backoff according to `retry`.
    // All state is owned by the operation (no references to the caller's options are retained);
    // only `httpClient` must outlive the operation. A cancellation slot on `requestOptions` cancels
    // the in-flight attempt or the pending backoff, completing with std::errc::operation_canceled.
    // `retryNotFoundAndGone` additionally retries HTTP 404/410 (managed identity endpoints answer these while
    // starting).
    void SendWithRetryAsync(IHttpClient& httpClient,
        RequestAuth auth,
        RetryOptions retry,
        HttpRequest request,
        IHttpClient::CompletionHandler completion,
        HttpRequestOptions requestOptions,
        bool retryNotFoundAndGone = false);

    void SendAuthorizedRequestAsync(IHttpClient& httpClient,
        const ConnectionState& connection,
        HttpRequest request,
        IHttpClient::CompletionHandler completion,
        HttpRequestOptions requestOptions);

    inline void SendAuthorizedRequestAsync(IHttpClient& httpClient,
        const ContainerTarget& target,
        HttpRequest request,
        IHttpClient::CompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        SendAuthorizedRequestAsync(httpClient,
            *target.Connection,
            std::move(request),
            std::move(completion),
            requestOptions);
    }

    inline void SendAuthorizedRequestAsync(IHttpClient& httpClient,
        const BlobTarget& target,
        HttpRequest request,
        IHttpClient::CompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        SendAuthorizedRequestAsync(httpClient,
            *target.Connection,
            std::move(request),
            std::move(completion),
            requestOptions);
    }

    using DownloadCompletion =
        std::move_only_function<void(std::expected<Response<Models::DownloadBlobResult>, BlobStorageError>)>;
    using DownloadToCompletion =
        std::move_only_function<void(std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>)>;

    // Maps the single-request download options onto the chunked engine's options (default chunk
    // size and concurrency).
    [[nodiscard]] DownloadToOptions ToDownloadToOptions(DownloadBlobOptions options);

    // Download engine (src/BlobDownload.cpp). Each call owns all of its state; only `httpClient`
    // (and, for DownloadBlobToAsync, `stream`) must outlive the operation. The first request is a
    // ranged GET of ChunkSize bytes (a 0-byte blob's 416 falls back to one un-ranged GET); the
    // remaining bytes are fetched in ChunkSize ranges, up to Concurrency at a time, with
    // If-Match: <first ETag> unless the caller supplied If-Match, and written in order. On failure or
    // cancellation the in-flight requests are cancelled and drained before the single completion.
    void DownloadBlobAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        DownloadBlobOptions operationOptions,
        DownloadCompletion completion,
        HttpRequestOptions requestOptions);
    void DownloadBlobToAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        DownloadToOptions operationOptions,
        std::ostream& stream,
        DownloadToCompletion completion,
        HttpRequestOptions requestOptions);
    // Downloads into `<path>.partial-<id>` and renames it over `path` only on success.
    void DownloadBlobToFileAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        DownloadToOptions operationOptions,
        const std::filesystem::path& path,
        DownloadToCompletion completion,
        HttpRequestOptions requestOptions);

    template <class TCompletion>
    void DeleteBlobAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        const DeleteBlobOptions& operationOptions,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = BuildBlobRequest(options, HttpMethod::Delete);
        ApplyBlobRequestConditions(request, operationOptions.Conditions);

        SendAuthorizedRequestAsync(httpClient,
            options,
            std::move(request),
            [completion = std::forward<TCompletion>(completion)](std::error_code error, HttpResponse response) mutable
        {
            CompleteParsed<Models::DeleteBlobResult>(error,
                std::move(response),
                ParseDeleteBlobResult,
                std::move(completion));
        },
            std::move(requestOptions));
    }

    template <class TCompletion>
    void GetBlobPropertiesAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        const GetBlobPropertiesOptions& operationOptions,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = BuildBlobRequest(options, HttpMethod::Head);
        ApplyBlobRequestConditions(request, operationOptions.Conditions);

        SendAuthorizedRequestAsync(httpClient,
            options,
            std::move(request),
            [completion = std::forward<TCompletion>(completion)](std::error_code error, HttpResponse response) mutable
        {
            CompleteParsed<Models::BlobProperties>(error,
                std::move(response),
                ParseBlobProperties,
                std::move(completion));
        },
            std::move(requestOptions));
    }

    template <class TCompletion>
    void SetBlobMetadataAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        const SetBlobMetadataOptions& operationOptions,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = BuildBlobRequest(options, HttpMethod::Put, "comp=metadata");
        ApplyMetadata(request, operationOptions.Metadata);
        ApplyBlobRequestConditions(request, operationOptions.Conditions);

        SendAuthorizedRequestAsync(httpClient,
            options,
            std::move(request),
            [completion = std::forward<TCompletion>(completion)](std::error_code error, HttpResponse response) mutable
        {
            CompleteParsed<Models::SetBlobMetadataResult>(error,
                std::move(response),
                ParseETagAndLastModified<Models::SetBlobMetadataResult>,
                std::move(completion));
        },
            std::move(requestOptions));
    }

    // Sends `request` to `target` and completes with the TModel produced by `parser`.
    template <class TModel, class TTarget, class TParser, class TCompletion>
    void SendAndParse(IHttpClient& httpClient,
        const TTarget& target,
        HttpRequest request,
        TParser parser,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        SendAuthorizedRequestAsync(httpClient,
            target,
            std::move(request),
            [parser = std::move(parser), completion = std::forward<TCompletion>(completion)](std::error_code error,
                HttpResponse response) mutable
        {
            CompleteParsed<TModel>(error, std::move(response), parser, std::move(completion));
        },
            std::move(requestOptions));
    }

    // The lease id of a lease request comes from the options' LeaseId; Conditions.LeaseId is ignored.
    inline void ApplyLeaseRequestConditions(HttpRequest& request, Models::BlobRequestConditions conditions)
    {
        conditions.LeaseId.clear();
        ApplyBlobRequestConditions(request, conditions);
    }

    // Lease requests (Lease Blob).
    [[nodiscard]] inline HttpRequest BuildLeaseRequest(const BlobTarget& target, std::string_view action)
    {
        HttpRequest request = BuildBlobRequest(target, HttpMethod::Put, "comp=lease");
        AddHeader(request, XMsLeaseActionHeaderName, action);
        return request;
    }

    template <class TTarget, class TCompletion>
    void AcquireLeaseAsync(IHttpClient& httpClient,
        const TTarget& target,
        AcquireLeaseOptions operationOptions,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        if (operationOptions.Duration.has_value() &&
            (*operationOptions.Duration < MinFixedLeaseDuration || *operationOptions.Duration > MaxFixedLeaseDuration))
        {
            PostCompletion(httpClient,
                std::forward<TCompletion>(completion),
                MakeError<Models::AcquireBlobLeaseResult>(std::make_error_code(std::errc::invalid_argument),
                    "Lease duration must be between 15 and 60 seconds, or infinite."));
            return;
        }

        HttpRequest request = BuildLeaseRequest(target, "acquire");
        AddHeader(request,
            XMsLeaseDurationHeaderName,
            operationOptions.Duration.has_value() ? std::to_string(operationOptions.Duration->count())
                                                  : std::string{"-1"});
        AddHeaderIfNotEmpty(request, XMsProposedLeaseIdHeaderName, operationOptions.ProposedLeaseId);
        ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
        SendAndParse<Models::AcquireBlobLeaseResult>(httpClient,
            target,
            std::move(request),
            ParseAcquireBlobLeaseResult,
            std::forward<TCompletion>(completion),
            std::move(requestOptions));
    }

    template <class TTarget, class TCompletion>
    void RenewLeaseAsync(IHttpClient& httpClient,
        const TTarget& target,
        RenewLeaseOptions operationOptions,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        if (operationOptions.LeaseId.empty())
        {
            PostCompletion(httpClient,
                std::forward<TCompletion>(completion),
                MakeError<Models::RenewBlobLeaseResult>(std::make_error_code(std::errc::invalid_argument),
                    "LeaseId must not be empty."));
            return;
        }

        HttpRequest request = BuildLeaseRequest(target, "renew");
        AddHeader(request, XMsLeaseIdHeaderName, operationOptions.LeaseId);
        ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
        SendAndParse<Models::RenewBlobLeaseResult>(httpClient,
            target,
            std::move(request),
            ParseRenewBlobLeaseResult,
            std::forward<TCompletion>(completion),
            std::move(requestOptions));
    }

    template <class TTarget, class TCompletion>
    void ReleaseLeaseAsync(IHttpClient& httpClient,
        const TTarget& target,
        ReleaseLeaseOptions operationOptions,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        if (operationOptions.LeaseId.empty())
        {
            PostCompletion(httpClient,
                std::forward<TCompletion>(completion),
                MakeError<Models::ReleaseBlobLeaseResult>(std::make_error_code(std::errc::invalid_argument),
                    "LeaseId must not be empty."));
            return;
        }

        HttpRequest request = BuildLeaseRequest(target, "release");
        AddHeader(request, XMsLeaseIdHeaderName, operationOptions.LeaseId);
        ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
        SendAndParse<Models::ReleaseBlobLeaseResult>(httpClient,
            target,
            std::move(request),
            ParseReleaseBlobLeaseResult,
            std::forward<TCompletion>(completion),
            std::move(requestOptions));
    }

    template <class TCompletion>
    void ExistsBlobAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        TCompletion&& completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = BuildBlobRequest(options, HttpMethod::Head);

        SendAuthorizedRequestAsync(httpClient,
            options,
            std::move(request),
            [completion = std::forward<TCompletion>(completion)](std::error_code error, HttpResponse response) mutable
        {
            RequestFailure failure = DetermineBlobStorageFailure(error, response);
            bool exists = false;
            if (!failure.Error)
            {
                exists = true;
            }
            else if (!error && response.GetStatus() == 404)
            {
                // A HEAD 404 carries no body, so the error code may be absent; any 404 (including a missing
                // container) means the blob does not exist.
                failure.Error = {};
            }

            completion(
                ToExpected(failure.Error, Response<bool>{exists, std::move(response), std::move(failure.Details)}));
        },
            std::move(requestOptions));
    }
} // namespace AVEVA::AzureClient::Private
