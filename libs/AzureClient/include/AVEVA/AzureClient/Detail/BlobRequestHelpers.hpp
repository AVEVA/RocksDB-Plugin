// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <algorithm>
#include <boost/asio/append.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/post.hpp>
#include <boost/url/url_view_base.hpp>

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <functional>
#include <gsl/zstring>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Private {
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

// Parses a base-10 integer that must span the whole text (no sign for unsigned types, no padding, no trailing data).
template <std::integral T> [[nodiscard]] constexpr std::optional<T> ParseExact(std::string_view text) noexcept {
    T value{};
    const char* const end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(text.data(), end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

// Lease duration limits defined by the Blob service.
inline constexpr std::chrono::seconds MinFixedLeaseDuration{15};
inline constexpr std::chrono::seconds MaxFixedLeaseDuration{60};

// Gives an otherwise-plain string-like parameter a distinct type (tagged by `Tag`, an empty
// caller-defined struct) so it is never adjacent-and-same-type with another string-like
// parameter in a function signature (see bugprone-easily-swappable-parameters). Implicitly
// constructible from anything that already implicitly converts to std::string_view at the call
// site, so callers are unaffected.
template <class Tag> struct AVEVA_GSL_POINTER StringLabel {
    std::string_view Value;

    constexpr StringLabel() noexcept = default;

    constexpr StringLabel(gsl::czstring value AVEVA_LIFETIMEBOUND) noexcept : Value(value) {}

    constexpr StringLabel(std::string_view value AVEVA_LIFETIMEBOUND) noexcept : Value(value) {}

    StringLabel(const std::string& value AVEVA_LIFETIMEBOUND) noexcept : Value(value) {}
};

// Same purpose as StringLabel, but for a numeric parameter of type `T`.
template <class T, class Tag> struct NumericLabel {
    T Value;

    constexpr NumericLabel(T value) noexcept : Value(value) {}
};

// Shared Key request signer, created once per connection: the account key is decoded and
// an HMAC-SHA256 context keyed with it is initialised at construction, so each signature only
// duplicates that context instead of re-decoding the key and re-fetching the algorithm (which
// takes global locks in OpenSSL 3). Thread-safe; shared by every client of a connection.
class SharedKeySigner {
  public:
    // Throws std::invalid_argument if `accountKeyBase64` is not valid base64.
    SharedKeySigner(std::string accountName, std::string_view accountKeyBase64);
    ~SharedKeySigner();
    SharedKeySigner(const SharedKeySigner&) = delete;
    SharedKeySigner& operator=(const SharedKeySigner&) = delete;
    SharedKeySigner(SharedKeySigner&&) = delete;
    SharedKeySigner& operator=(SharedKeySigner&&) = delete;

    [[nodiscard]] const std::string& AccountName() const noexcept AVEVA_LIFETIMEBOUND { return m_accountName; }

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
struct ConnectionState {
    std::string ServiceEndpoint;
    std::string SasToken; // without a leading '?'
    std::string ApiVersion;
    std::shared_ptr<const std::vector<std::string>> TokenScopes; // shared with every request's RequestAuth
    std::shared_ptr<const SharedKeySigner> Signer;               // set iff Shared Key authorization is used

    std::string BasePrefix; // scheme://host[:port]
    std::string BasePath;   // encoded endpoint path without a trailing '/', empty for the root

    RetryOptions Retry;
    AzureRequestOptions DefaultRequestOptions;
};

struct ContainerTarget {
    std::shared_ptr<const ConnectionState> Connection;
    std::string ContainerName;
    // "<scheme://host[:port]><basePath>/<encoded container>", built once so each request only appends its query.
    std::string EncodedResource;
};

struct BlobTarget {
    std::shared_ptr<const ConnectionState> Connection;
    std::string ContainerName;
    std::string BlobName;
    // "<scheme://host[:port]><basePath>/<encoded container>/<encoded blob>", built once so each request only
    // appends its query string.
    std::string EncodedResource;
};

// Normalise and validate client options (throw std::invalid_argument on invalid settings).
// `hasTokenCredential` tells the validation that the client was also given a token credential (which lives in the
// client targets, not in the connection), so the "at most one credential type" rule covers it.
[[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobServiceClientOptions options,
                                                                         bool hasTokenCredential = false);
[[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobContainerClientOptions options,
                                                                         bool hasTokenCredential = false);
[[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobClientOptions options,
                                                                         bool hasTokenCredential = false);
[[nodiscard]] ContainerTarget MakeContainerTarget(std::shared_ptr<const ConnectionState> connection,
                                                  std::string containerName);
[[nodiscard]] BlobTarget MakeBlobTarget(std::shared_ptr<const ConnectionState> connection, std::string containerName,
                                        std::string blobName);
[[nodiscard]] ContainerTarget MakeContainerTarget(const BlobContainerClientOptions& options,
                                                  bool hasTokenCredential = false);
[[nodiscard]] BlobTarget MakeBlobTarget(const BlobClientOptions& options, bool hasTokenCredential = false);

// The credential the clients actually authorize with: the caller's credential wrapped in a token cache (so
// identity-provider credentials do not fetch a token per request), or null when none was given.
template <TokenCredentialLike TCred>
[[nodiscard]] std::shared_ptr<CachedTokenCredential<TCred>> MakeCachedCredential(std::shared_ptr<TCred> credential) {
    if (!credential) {
        return nullptr;
    }
    if constexpr (IsCachingTokenCredential<TCred>::value) {
        return credential;
    } else {
        return CachingTokenCredential<TCred>::Create(std::move(credential));
    }
}

// The targets carry the (cached) credential, so every request path sees the concrete credential type.
template <class TCred> struct BasicContainerTarget : ContainerTarget {
    std::shared_ptr<CachedTokenCredential<TCred>> Credential;
};

template <class TCred> struct BasicBlobTarget : BlobTarget {
    std::shared_ptr<CachedTokenCredential<TCred>> Credential;
};

template <TokenCredentialLike TCred>
[[nodiscard]] BasicContainerTarget<TCred> MakeContainerTarget(std::shared_ptr<const ConnectionState> connection,
                                                              std::shared_ptr<CachedTokenCredential<TCred>> credential,
                                                              std::string containerName) {
    return {MakeContainerTarget(std::move(connection), std::move(containerName)), std::move(credential)};
}

template <TokenCredentialLike TCred>
[[nodiscard]] BasicBlobTarget<TCred> MakeBlobTarget(std::shared_ptr<const ConnectionState> connection,
                                                    std::shared_ptr<CachedTokenCredential<TCred>> credential,
                                                    std::string containerName, std::string blobName) {
    return {MakeBlobTarget(std::move(connection), std::move(containerName), std::move(blobName)),
            std::move(credential)};
}

template <TokenCredentialLike TCred>
[[nodiscard]] BasicContainerTarget<TCred> MakeContainerTarget(const BlobContainerClientOptions& options,
                                                              std::shared_ptr<TCred> credential) {
    const bool hasCredential = credential != nullptr;
    return {MakeContainerTarget(options, hasCredential), MakeCachedCredential(std::move(credential))};
}

template <TokenCredentialLike TCred>
[[nodiscard]] BasicBlobTarget<TCred> MakeBlobTarget(const BlobClientOptions& options,
                                                    std::shared_ptr<TCred> credential) {
    const bool hasCredential = credential != nullptr;
    return {MakeBlobTarget(options, hasCredential), MakeCachedCredential(std::move(credential))};
}

inline constexpr std::string_view XMsVersionHeaderName = "x-ms-version";
inline constexpr std::string_view XMsDateHeaderName = "x-ms-date";
inline constexpr std::string_view XMsClientRequestIdHeaderName = "x-ms-client-request-id";
inline constexpr std::string_view XMsRequestIdHeaderName = "x-ms-request-id";
inline constexpr std::string_view XMsErrorCodeHeaderName = "x-ms-error-code";
inline constexpr std::string_view XMsContentCrc64HeaderName = "x-ms-content-crc64";
inline constexpr std::string_view XMsDeleteTypePermanentHeaderName = "x-ms-delete-type-permanent";
inline constexpr std::string_view AuthorizationHeaderName = "Authorization";

// "Bearer <token>" built with a single allocation, without first copying the token into a temporary string.
[[nodiscard]] std::string MakeBearerValue(std::string_view token);
inline constexpr std::string_view XMsMetaHeaderPrefix = "x-ms-meta-";
inline constexpr std::string_view XMsBlobTypeHeaderName = "x-ms-blob-type";
inline constexpr std::string_view XMsBlobContentLengthHeaderName = "x-ms-blob-content-length";
inline constexpr std::string_view XMsPageWriteHeaderName = "x-ms-page-write";
inline constexpr std::string_view XMsRangeHeaderName = "x-ms-range";
inline constexpr std::string_view XMsCopySourceHeaderName = "x-ms-copy-source";
inline constexpr std::string_view XMsSourceRangeHeaderName = "x-ms-source-range";
inline constexpr std::string_view XMsSourceIfMatchHeaderName = "x-ms-source-if-match";
inline constexpr std::string_view XMsCopySourceAuthorizationHeaderName = "x-ms-copy-source-authorization";
// Placeholder value of x-ms-copy-source-authorization that the request pipeline replaces, per attempt, with the
// credential the client itself authorizes with (a fresh token on every retry), or removes when that credential
// is not a bearer token.
inline constexpr std::string_view CopySourceClientCredentialMarker = "{client-credential}";
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

struct RequestFailure {
    std::error_code Error;
    std::optional<BlobStorageError> Details;
};

// Locale-free ASCII folding: the std::locale based algorithms take a process-wide lock on MSVC per character.
[[nodiscard]] constexpr char FoldAscii(char value) noexcept {
    return (value >= 'A' && value <= 'Z') ? static_cast<char>(value | 0x20) : value;
}

// The HTTP client a shared_ptr constructor keeps alive; a null handle is a caller error.
template <class THttp> THttp& RequireHttpClient(const std::shared_ptr<THttp>& httpClient) {
    if (!httpClient) {
        throw std::invalid_argument("The HTTP client must not be null.");
    }
    return *httpClient;
}

[[nodiscard]] constexpr bool IEquals(std::string_view lhs, std::string_view rhs) noexcept {
    return std::ranges::equal(lhs, rhs, {}, FoldAscii, FoldAscii);
}

[[nodiscard]] constexpr bool IStartsWith(std::string_view value, std::string_view prefix) noexcept {
    return value.size() >= prefix.size() && IEquals(value.substr(0, prefix.size()), prefix);
}

[[nodiscard]] std::string ToLowerAscii(std::string_view value);
// True for "localhost", any 127.0.0.0/8 address and ::1.
[[nodiscard]] bool IsLoopbackHost(const boost::urls::url_view_base& url) noexcept;
[[nodiscard]] std::string_view TrimWhitespace(std::string_view value AVEVA_LIFETIMEBOUND) noexcept;
// True if `value` contains CR, LF or NUL (header injection / truncation).
[[nodiscard]] bool HasHeaderControlCharacters(std::string_view value) noexcept;
// Throws std::invalid_argument unless `value` is canonical padded base64; returns the decoded length.
// An empty value is accepted only when `allowEmpty` is set.
[[nodiscard]] std::size_t ValidateBase64AndGetDecodedLength(std::string_view value, bool allowEmpty);
[[nodiscard]] std::string_view FindHeaderValue(const HttpResponse& response AVEVA_LIFETIMEBOUND,
                                               std::string_view headerName) noexcept;
[[nodiscard]] bool ParseBoolHeader(std::string_view value) noexcept;
[[nodiscard]] Models::LeaseStatus ParseLeaseStatus(std::string_view value);
[[nodiscard]] Models::LeaseState ParseLeaseState(std::string_view value);
[[nodiscard]] Models::LeaseDurationType ParseLeaseDurationType(std::string_view value);
[[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseHttpDateHeader(std::string_view value) noexcept;
[[nodiscard]] std::string BuildDateHeaderValue(std::chrono::system_clock::time_point now);
[[nodiscard]] std::string_view TrimTrailingSlashes(std::string_view value AVEVA_LIFETIMEBOUND) noexcept;
[[nodiscard]] std::string_view TrimLeadingQuestionMark(std::string_view value AVEVA_LIFETIMEBOUND) noexcept;
[[nodiscard]] std::string CreateClientRequestId();
// RFC 7231 date name tables shared by the request formatter and the response parsers.
inline constexpr std::array<std::string_view, 12> MonthNames{"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
inline constexpr std::array<std::string_view, 7> ShortWeekdayNames{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
inline constexpr std::array<std::string_view, 7> LongWeekdayNames{"Sunday",   "Monday", "Tuesday", "Wednesday",
                                                                  "Thursday", "Friday", "Saturday"};

[[nodiscard]] std::span<char> AsChars(std::span<std::byte> bytes AVEVA_LIFETIMEBOUND) noexcept;
[[nodiscard]] std::string_view AsChars(std::span<const std::byte> bytes AVEVA_LIFETIMEBOUND) noexcept;
[[nodiscard]] std::string BytesToString(std::span<const std::byte> bytes);
[[nodiscard]] std::string Base64Encode(std::span<const std::byte> bytes);
[[nodiscard]] std::string UrlEncode(std::string_view value, std::string_view extraSafeChars);
// Appends the percent-encoded `value` to `out` without an intermediate string.
void UrlEncodeAppend(std::string& out, std::string_view value, std::string_view extraSafeChars);
[[nodiscard]] std::string BuildQueryString(const std::vector<std::pair<std::string, std::string>>& parameters);
[[nodiscard]] std::string BuildRangeHeaderValue(const Models::BlobByteRange& range);

[[nodiscard]] RequestFailure DetermineBlobStorageFailure(std::error_code transportError, const HttpResponse& response);
// A successful response whose content could not be parsed (BlobStorageErrorCode::InvalidResponse).
[[nodiscard]] RequestFailure MakeInvalidResponseFailure(const HttpResponse& response, std::string message);

struct ParsedContentRange {
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

[[nodiscard]] Models::ListBlobsResult ParseListBlobsResultXml(std::string_view xml);
// Parses a Get Page Ranges response: the <PageList> body plus the ETag, Last-Modified and
// x-ms-blob-content-length headers.
[[nodiscard]] Models::GetPageRangesResult ParseGetPageRangesResult(const HttpResponse& response);

[[nodiscard]] HttpRequest BuildBlobRequest(const BlobTarget& target, HttpMethod method,
                                           std::string_view queryString = {});
[[nodiscard]] std::string BuildBlobUrl(const BlobTarget& target, std::string_view queryString = {});
[[nodiscard]] HttpRequest BuildContainerRequest(const ContainerTarget& target, HttpMethod method,
                                                std::string_view queryString = {});
[[nodiscard]] std::string BuildContainerUrl(const ContainerTarget& target, std::string_view queryString = {});
[[nodiscard]] std::string BuildServiceUrl(const ConnectionState& connection, std::string_view queryString = {});

// Convenience overloads that normalise and validate client options first (tests).
[[nodiscard]] std::string BuildBlobUrl(const BlobClientOptions& options, std::string_view queryString = {});
[[nodiscard]] HttpRequest BuildBlobRequest(const BlobClientOptions& options, HttpMethod method,
                                           std::string_view queryString = {});
[[nodiscard]] std::string BuildContainerUrl(const BlobContainerClientOptions& options,
                                            std::string_view queryString = {});
[[nodiscard]] HttpRequest BuildContainerRequest(const BlobContainerClientOptions& options, HttpMethod method,
                                                std::string_view queryString = {});
[[nodiscard]] std::string BuildServiceUrl(const BlobServiceClientOptions& options, std::string_view queryString = {});

void AddHeader(HttpRequest& request, std::string_view name, std::string_view value);
void AddHeader(HttpRequest& request, std::string_view name, std::string&& value);
void AddHeaderIfNotEmpty(HttpRequest& request, std::string_view name, std::string_view value);
void AddHeaderIfNotEmpty(HttpRequest& request, std::string_view name, std::string&& value);
void ApplyBlobRequestConditions(HttpRequest& request, const Models::BlobRequestConditions& conditions);
void ApplyMetadata(HttpRequest& request, const Models::MetadataMap& metadata);
// Azure requires metadata names to be C# identifiers: [A-Za-z_][A-Za-z0-9_]*.
[[nodiscard]] bool IsValidMetadataName(std::string_view name) noexcept;
// One-off Shared Key signing (constructs a temporary signer); clients use ConnectionState::Signer.
void AuthorizeRequest(const SharedKeyCredentialOptions& options, HttpRequest& request);
void AuthorizeRequest(const SharedKeySigner& signer, HttpRequest& request);

// Maps the exception being handled to a BlobStorageError; must be called from a catch handler.
[[nodiscard]] BlobStorageError MakeFailureFromCurrentException();

// A BlobStorageError carrying only `error` and `message` (defaulting to error.message()).
[[nodiscard]] BlobStorageError MakeClientError(std::error_code error, std::string message = {});

// The failure's service details (or a client error built lazily) with Code set to the failure's error.
[[nodiscard]] inline BlobStorageError ToDetails(RequestFailure&& failure) {
    BlobStorageError details =
        failure.Details.has_value() ? std::move(*failure.Details) : MakeClientError(failure.Error);
    details.Code = failure.Error;
    return details;
}

// Folds a transport/service std::error_code into the single std::expected error channel. Success
// is governed solely by `error`: Exists*/*IfExists* clear it while leaving the suppressed
// diagnostics (e.g. the 404's request id) available via Response<T>::Error().
template <class T>
[[nodiscard]] std::expected<Response<T>, BlobStorageError> ToExpected(std::error_code error, Response<T> response) {
    if (!error) {
        return response;
    }

    if (response.Error().has_value()) {
        BlobStorageError details = std::move(*response.Error());
        details.Code = error;
        return std::unexpected(std::move(details));
    }

    return std::unexpected(MakeClientError(error));
}

// Completion adapter for *IfExists / *IfNotExists: a failure whose code is one of `suppressed` completes as
// success with the real raw response, keeping the suppressed error's details in Response<T>::Error().
template <class TModel, class TCompletion, std::same_as<BlobStorageErrorCode>... TCodes>
[[nodiscard]] auto SuppressErrors(TCompletion completion, TCodes... suppressed) {
    return [completion = std::move(completion),
            codes = std::array<BlobStorageErrorCode, sizeof...(TCodes)>{suppressed...}](
               std::error_code error, Response<TModel> response) mutable {
        if (error && std::ranges::contains(codes, error)) {
            error.clear();
        }
        completion(ToExpected(error, std::move(response)));
    };
}

// A client-side failure (e.g. argument validation) with `message` defaulting to error.message().
template <class T>
[[nodiscard]] std::expected<Response<T>, BlobStorageError> MakeError(std::error_code error, std::string message = {}) {
    return std::unexpected(MakeClientError(error, std::move(message)));
}

// Parses a completed response with `parser` and completes either with
// std::expected<Response<TModel>, BlobStorageError> or, if `completion` takes them, with (std::error_code,
// Response<TModel>).
template <class TModel, class TParser, class TCompletion>
void CompleteParsed(std::error_code error, HttpResponse response, TParser&& parser, TCompletion&& completion) {
    RequestFailure failure = DetermineBlobStorageFailure(error, response);
    TModel model;
    if (!failure.Error) {
        try {
            model = std::forward<TParser>(parser)(response);
        } catch (const std::exception& exception) {
            failure = MakeInvalidResponseFailure(response, exception.what());
        }
    }

    Response<TModel> result{std::move(model), std::move(response), std::move(failure.Details)};
    if constexpr (std::is_invocable_v<TCompletion, std::error_code, Response<TModel>>) {
        std::forward<TCompletion>(completion)(failure.Error, std::move(result));
    } else {
        std::forward<TCompletion>(completion)(ToExpected(failure.Error, std::move(result)));
    }
}

template <class T>
    requires requires(T result) {
        result.ETag;
        result.LastModified;
    }
void ApplyETagAndLastModified(T& result, const HttpResponse& response) {
    result.ETag = std::string{FindHeaderValue(response, ETagHeaderName)};
    const std::string_view lastModified = FindHeaderValue(response, LastModifiedHeaderName);
    if (!lastModified.empty()) {
        const auto parsed = ParseHttpDateHeader(lastModified);
        if (!parsed.has_value()) {
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
[[nodiscard]] T ParseETagAndLastModified(const HttpResponse& response) {
    T result;
    ApplyETagAndLastModified(result, response);
    return result;
}

// Authorization material captured (by value) for the lifetime of a single logical request,
// including all of its retry attempts. Exactly one of SAS (in URL), SharedKey or TokenCredential, or none;
// otherwise construction of the client throws std::invalid_argument.
struct RequestAuth {
    enum class Kind : std::uint8_t { None, SharedKey, Token };

    Kind Type = Kind::None;
    std::shared_ptr<const SharedKeySigner> Signer;
    std::shared_ptr<const std::vector<std::string>> TokenScopes;
};

// `hasTokenCredential` selects Kind::Token when no SAS or Shared Key applies.
[[nodiscard]] RequestAuth MakeRequestAuth(const ConnectionState& connection, bool hasTokenCredential);

// True for HTTP statuses and transport errors that Azure Storage documents as transient.
[[nodiscard]] bool IsRetriableTransportError(std::error_code error) noexcept;
// True for transport errors raised after the request may have reached the service (the outcome is unknown), as
// opposed to failures that happened before anything was sent (resolve, connect).
[[nodiscard]] bool IsAmbiguousTransportError(std::error_code error) noexcept;
[[nodiscard]] bool IsRetriableStatus(unsigned int status) noexcept;
[[nodiscard]] bool IsRetriableFailure(std::error_code error, const HttpResponse& response) noexcept;

// Server-provided retry delay (x-ms-retry-after-ms, then Retry-After in seconds), if any.
[[nodiscard]] std::optional<std::chrono::milliseconds> ParseRetryAfter(const HttpResponse& response) noexcept;

// Rebuilds the body (and anything derived from it, such as a nested signature) of each attempt just before it is
// authorized and sent, so a retry never replays a stale date, signature or token. `bearerToken` is the token the
// attempt will carry (empty when it is not bearer-authorized). Throwing fails the request. Any callable taking
// (HttpRequest&, std::string_view) is accepted; the pipeline is templated on its type, so nothing is type-erased.
struct NoRebuild {};

// Sends `request` with per-attempt authorization and retry/backoff according to `retry`.
// All state is owned by the operation (no references to the caller's options are retained);
// only `httpClient` must outlive the operation. A cancellation slot on `requestOptions` cancels
// the in-flight attempt or the pending backoff, completing with std::errc::operation_canceled.
// `retryNotFoundAndGone` additionally retries HTTP 404/410 (managed identity endpoints answer these while
// starting). Defined in RequestPipeline.hpp.
// `completion` is invoked as completion(std::error_code, HttpResponse).
template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler, class TRebuild = NoRebuild>
void SendWithRetryAsync(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
                        HttpRequest request, THandler completion, AzureRequestOptions requestOptions,
                        bool retryNotFoundAndGone = false, TRebuild rebuild = {}, bool replaySafe = false);

// Defined in RequestPipeline.hpp. `replaySafe` marks a request whose caller absorbs the outcome of a replay (the
// *IfExists/*IfNotExists operations), so it may be retried even after an ambiguous transport error.
template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler, class TRebuild = NoRebuild>
void SendAuthorizedRequestAsync(THttp& httpClient, const ConnectionState& connection, std::shared_ptr<TCred> credential,
                                HttpRequest request, THandler completion, AzureRequestOptions requestOptions,
                                TRebuild rebuild = {}, bool replaySafe = false);

template <HttpClientLike THttp, class TCred, class THandler, class TRebuild = NoRebuild>
void SendAuthorizedRequestAsync(THttp& httpClient, const BasicContainerTarget<TCred>& target, HttpRequest request,
                                THandler completion, AzureRequestOptions requestOptions, TRebuild rebuild = {},
                                bool replaySafe = false) {
    SendAuthorizedRequestAsync(httpClient, *target.Connection, target.Credential, std::move(request),
                               std::move(completion), std::move(requestOptions), std::move(rebuild), replaySafe);
}

template <HttpClientLike THttp, class TCred, class THandler>
void SendAuthorizedRequestAsync(THttp& httpClient, const BasicBlobTarget<TCred>& target, HttpRequest request,
                                THandler completion, AzureRequestOptions requestOptions, bool replaySafe = false) {
    SendAuthorizedRequestAsync(httpClient, *target.Connection, target.Credential, std::move(request),
                               std::move(completion), std::move(requestOptions), NoRebuild{}, replaySafe);
}

// Maps the single-request download options onto the chunked engine's options (default chunk
// size and concurrency).
[[nodiscard]] DownloadToOptions ToDownloadToOptions(DownloadBlobOptions options);

// Download engine (defined in BlobDownload.hpp). Each call owns all of its state; only `httpClient`
// and the span/path destination (see below) must outlive the operation. The first request is a
// ranged GET of ChunkSize bytes (a 0-byte blob's 416 falls back to one un-ranged GET); the
// remaining bytes are fetched in ChunkSize ranges, up to Concurrency at a time, with
// If-Match: <first ETag> unless the caller supplied If-Match, and written in order. On failure or
// cancellation the in-flight requests are cancelled and drained before the single completion.
// MaxSize limits the output, not transport buffering: it is checked once the first response (at most
// ChunkSize bytes, already buffered) has revealed the blob size. AzureRequestOptions::SetTotalDeadline is one
// absolute budget for the whole download; each request receives only the time remaining.
template <HttpClientLike THttp, class TCred, class TCompletion>
void DownloadBlobAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                       DownloadBlobOptions operationOptions, TCompletion completion,
                       AzureRequestOptions requestOptions);
// Writes straight into `destination`, which must stay valid until the completion runs. A range larger than
// the buffer fails the download; a shorter blob leaves the tail untouched (see BytesWritten).
template <HttpClientLike THttp, class TCred, class TCompletion>
void DownloadBlobToSpanAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                             DownloadToOptions operationOptions, std::span<char> destination, TCompletion completion,
                             AzureRequestOptions requestOptions);
// Downloads into `<path>.partial-<id>` and renames it over `path` only on success.
template <HttpClientLike THttp, class TCred, class TCompletion>
void DownloadBlobToFileAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                             DownloadToOptions operationOptions, const std::filesystem::path& path,
                             TCompletion completion, AzureRequestOptions requestOptions);

// Sends `request` to `target` and completes with the TModel produced by `parser`. `extra` is forwarded to
// SendAuthorizedRequestAsync (for a blob target, the replay-safety flag).
template <class TModel, HttpClientLike THttp, class TTarget, class TParser, class TCompletion, class... TExtra>
void SendAndParse(THttp& httpClient, const TTarget& target, HttpRequest request, TParser parser,
                  TCompletion&& completion, AzureRequestOptions requestOptions, TExtra&&... extra) {
    SendAuthorizedRequestAsync(
        httpClient, target, std::move(request),
        [parser = std::move(parser),
         completion = std::forward<TCompletion>(completion)](std::error_code error, HttpResponse response) mutable {
            CompleteParsed<TModel>(error, std::move(response), parser, std::move(completion));
        },
        std::move(requestOptions), std::forward<TExtra>(extra)...);
}

template <HttpClientLike THttp, class TCred, class TCompletion>
void DeleteBlobAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options,
                     const DeleteBlobOptions& operationOptions, TCompletion&& completion,
                     AzureRequestOptions requestOptions, bool replaySafe = false) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Delete);
    ApplyBlobRequestConditions(request, operationOptions.Conditions);

    SendAndParse<Models::DeleteBlobResult>(httpClient, options, std::move(request), ParseDeleteBlobResult,
                                           std::forward<TCompletion>(completion), std::move(requestOptions),
                                           replaySafe);
}

template <HttpClientLike THttp, class TCred, class TCompletion>
void GetBlobPropertiesAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options,
                            const GetBlobPropertiesOptions& operationOptions, TCompletion&& completion,
                            AzureRequestOptions requestOptions) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Head);
    ApplyBlobRequestConditions(request, operationOptions.Conditions);

    SendAndParse<Models::BlobProperties>(httpClient, options, std::move(request), ParseBlobProperties,
                                         std::forward<TCompletion>(completion), std::move(requestOptions));
}

template <HttpClientLike THttp, class TCred, class TCompletion>
void SetBlobMetadataAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options,
                          const SetBlobMetadataOptions& operationOptions, TCompletion&& completion,
                          AzureRequestOptions requestOptions) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Put, "comp=metadata");
    ApplyMetadata(request, operationOptions.Metadata);
    ApplyBlobRequestConditions(request, operationOptions.Conditions);

    SendAndParse<Models::SetBlobMetadataResult>(httpClient, options, std::move(request),
                                                ParseETagAndLastModified<Models::SetBlobMetadataResult>,
                                                std::forward<TCompletion>(completion), std::move(requestOptions));
}

// Completes `completion` with an invalid_argument failure carrying `message`.
template <class TModel, HttpClientLike THttp, class THandler>
void FailInvalidArgument(THttp& httpClient, THandler&& completion, std::string message) {
    PostCompletion(httpClient, std::forward<THandler>(completion),
                   MakeError<TModel>(std::make_error_code(std::errc::invalid_argument), std::move(message)));
}

// If `validationError` is set, completes `completion` with it as an invalid_argument failure.
// Returns true when the caller must stop. `completion` is only consumed on failure, so pass it as std::move(...).
template <class TModel, HttpClientLike THttp, class THandler>
[[nodiscard]] bool ValidateOrFail(THttp& httpClient, THandler&& completion,
                                  const std::optional<std::string>& validationError) {
    if (!validationError.has_value()) {
        return false;
    }
    FailInvalidArgument<TModel>(httpClient, std::forward<THandler>(completion), *validationError);
    return true;
}

// The lease id of a lease request comes from the options' LeaseId; Conditions.LeaseId is ignored.
void ApplyLeaseRequestConditions(HttpRequest& request, Models::BlobRequestConditions conditions);

// Lease requests (Lease Blob).
[[nodiscard]] HttpRequest BuildLeaseRequest(const BlobTarget& target, std::string_view action);

template <HttpClientLike THttp, class TTarget, class TCompletion>
void AcquireLeaseAsync(THttp& httpClient, const TTarget& target, AcquireLeaseOptions operationOptions,
                       TCompletion&& completion, AzureRequestOptions requestOptions) {
    if (operationOptions.Duration.has_value() &&
        (*operationOptions.Duration < MinFixedLeaseDuration || *operationOptions.Duration > MaxFixedLeaseDuration)) {
        PostCompletion(httpClient, std::forward<TCompletion>(completion),
                       MakeError<Models::AcquireBlobLeaseResult>(
                           std::make_error_code(std::errc::invalid_argument),
                           "Lease duration must be between 15 and 60 seconds, or infinite."));
        return;
    }

    HttpRequest request = BuildLeaseRequest(target, "acquire");
    AddHeader(request, XMsLeaseDurationHeaderName,
              operationOptions.Duration.has_value() ? std::to_string(operationOptions.Duration->count())
                                                    : std::string{"-1"});
    // A server-chosen lease ID would be lost if the response is, and the retry would hit 409 LeaseAlreadyPresent.
    if (operationOptions.ProposedLeaseId.empty()) {
        operationOptions.ProposedLeaseId = CreateClientRequestId();
    }
    AddHeader(request, XMsProposedLeaseIdHeaderName, operationOptions.ProposedLeaseId);
    ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
    SendAndParse<Models::AcquireBlobLeaseResult>(httpClient, target, std::move(request), ParseAcquireBlobLeaseResult,
                                                 std::forward<TCompletion>(completion), std::move(requestOptions));
}

template <HttpClientLike THttp, class TTarget, class TCompletion>
void RenewLeaseAsync(THttp& httpClient, const TTarget& target, RenewLeaseOptions operationOptions,
                     TCompletion&& completion, AzureRequestOptions requestOptions) {
    if (operationOptions.LeaseId.empty()) {
        PostCompletion(httpClient, std::forward<TCompletion>(completion),
                       MakeError<Models::RenewBlobLeaseResult>(std::make_error_code(std::errc::invalid_argument),
                                                               "LeaseId must not be empty."));
        return;
    }

    HttpRequest request = BuildLeaseRequest(target, "renew");
    AddHeader(request, XMsLeaseIdHeaderName, operationOptions.LeaseId);
    ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
    SendAndParse<Models::RenewBlobLeaseResult>(httpClient, target, std::move(request), ParseRenewBlobLeaseResult,
                                               std::forward<TCompletion>(completion), std::move(requestOptions));
}

template <HttpClientLike THttp, class TTarget, class TCompletion>
void ReleaseLeaseAsync(THttp& httpClient, const TTarget& target, ReleaseLeaseOptions operationOptions,
                       TCompletion&& completion, AzureRequestOptions requestOptions) {
    if (operationOptions.LeaseId.empty()) {
        PostCompletion(httpClient, std::forward<TCompletion>(completion),
                       MakeError<Models::ReleaseBlobLeaseResult>(std::make_error_code(std::errc::invalid_argument),
                                                                 "LeaseId must not be empty."));
        return;
    }

    HttpRequest request = BuildLeaseRequest(target, "release");
    AddHeader(request, XMsLeaseIdHeaderName, operationOptions.LeaseId);
    ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
    // A release replayed after the first attempt's outcome was lost may find the lease already gone; counting
    // replay after an ambiguous outcome lets that 409 be told apart from a genuine one (including one that follows
    // a plain token-refresh repeat).
    auto replayed = std::make_shared<bool>(false);
    auto absorb = [replayed, completion = std::forward<TCompletion>(completion)](
                      std::error_code error, Response<Models::ReleaseBlobLeaseResult> response) mutable {
        if (error && *replayed && error == BlobStorageErrorCode::LeaseNotPresentWithLeaseOperation) {
            error.clear();
        }
        completion(ToExpected(error, std::move(response)));
    };
    SendAuthorizedRequestAsync(
        httpClient, *target.Connection, target.Credential, std::move(request),
        [absorb = std::move(absorb)](std::error_code error, HttpResponse response) mutable {
            CompleteParsed<Models::ReleaseBlobLeaseResult>(error, std::move(response), ParseReleaseBlobLeaseResult,
                                                           absorb);
        },
        std::move(requestOptions),
        [replayed](HttpRequest&, std::string_view, bool replayAfterAmbiguous) { *replayed = replayAfterAmbiguous; },
        true);
}

template <HttpClientLike THttp, class TCred, class TCompletion>
void ExistsBlobAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options, TCompletion&& completion,
                     AzureRequestOptions requestOptions) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Head);

    SendAuthorizedRequestAsync(
        httpClient, options, std::move(request),
        [completion = std::forward<TCompletion>(completion)](std::error_code error, HttpResponse response) mutable {
            RequestFailure failure = DetermineBlobStorageFailure(error, response);
            bool exists = false;
            if (!failure.Error) {
                exists = true;
            } else if (!error && response.GetStatus() == 404) {
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
