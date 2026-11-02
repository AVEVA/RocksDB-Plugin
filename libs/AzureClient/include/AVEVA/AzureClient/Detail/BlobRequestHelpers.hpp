// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Outcomes.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <AVEVA/HttpClient/HttpStatusCode.hpp>

#include <algorithm>
#include <boost/asio/append.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
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
/// <summary>
/// Parses a base-10 integer that must span the whole text (no sign for unsigned types, no padding, no trailing data).
/// </summary>
/// <typeparam name="T">The integral type to parse into.</typeparam>
/// <param name="text">The text to parse.</param>
/// <returns>The value, or nothing when the text is not exactly one integer that fits
/// <typeparamref name="T"/>.</returns>
template <std::integral T> [[nodiscard]] constexpr std::optional<T> ParseExact(std::string_view text) noexcept {
    T value{};
    const char* const end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(text.data(), end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

/// <summary>The shortest fixed lease duration defined by the Blob service.</summary>
inline constexpr std::chrono::seconds MinFixedLeaseDuration{15};
/// <summary>The longest fixed lease duration defined by the Blob service.</summary>
inline constexpr std::chrono::seconds MaxFixedLeaseDuration{60};

/// <summary>
/// Gives an otherwise-plain string-like parameter a distinct type (tagged by <typeparamref name="Tag"/>, an empty
/// caller-defined struct) so it is never adjacent-and-same-type with another string-like
/// parameter in a function signature (see bugprone-easily-swappable-parameters). Implicitly
/// constructible from anything that already implicitly converts to std::string_view at the call
/// site, so callers are unaffected.
/// </summary>
/// <typeparam name="Tag">The empty struct that makes this label a distinct type.</typeparam>
template <class Tag> struct AVEVA_GSL_POINTER StringLabel {
    std::string_view Value;

    /// <summary>Creates a label holding an empty view.</summary>
    constexpr StringLabel() noexcept = default;

    /// <summary>Labels a null-terminated string, which must outlive the label.</summary>
    /// <param name="value">The text to label.</param>
    constexpr StringLabel(gsl::czstring value AVEVA_LIFETIMEBOUND) noexcept : Value(value) {}

    /// <summary>Labels a view, whose characters must outlive the label.</summary>
    /// <param name="value">The text to label.</param>
    constexpr StringLabel(std::string_view value AVEVA_LIFETIMEBOUND) noexcept : Value(value) {}

    /// <summary>Labels a string, which must outlive the label.</summary>
    /// <param name="value">The text to label.</param>
    StringLabel(const std::string& value AVEVA_LIFETIMEBOUND) noexcept : Value(value) {}
};

/// <summary>Same purpose as <see cref="StringLabel"/>, but for a numeric parameter of type
/// <typeparamref name="T"/>.</summary>
/// <typeparam name="T">The numeric type of the labelled value.</typeparam>
/// <typeparam name="Tag">The empty struct that makes this label a distinct type.</typeparam>
template <class T, class Tag> struct NumericLabel {
    T Value;

    /// <summary>Labels a value.</summary>
    /// <param name="value">The value to label.</param>
    constexpr NumericLabel(T value) noexcept : Value(value) {}
};

/// <summary>
/// Shared Key request signer, created once per connection: the account key is decoded and
/// an HMAC-SHA256 context keyed with it is initialised at construction, so each signature only
/// duplicates that context instead of re-decoding the key and re-fetching the algorithm (which
/// takes global locks in OpenSSL 3). Thread-safe; shared by every client of a connection.
/// </summary>
class SharedKeySigner {
  public:
    /// <summary>Decodes the account key and keys the signing context with it.</summary>
    /// <param name="accountName">The storage account name, which is part of every string-to-sign.</param>
    /// <param name="accountKeyBase64">The base64 account key.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="accountKeyBase64"/> is not valid
    /// base64.</exception>
    /// <exception cref="std::runtime_error">Thrown when the signing algorithm cannot be initialised.</exception>
    SharedKeySigner(std::string accountName, std::string_view accountKeyBase64);
    /// <summary>Releases the signing context.</summary>
    ~SharedKeySigner();
    /// <summary>Not copyable.</summary>
    SharedKeySigner(const SharedKeySigner&) = delete;
    /// <summary>Not copyable.</summary>
    SharedKeySigner& operator=(const SharedKeySigner&) = delete;
    /// <summary>Not movable.</summary>
    SharedKeySigner(SharedKeySigner&&) = delete;
    /// <summary>Not movable.</summary>
    SharedKeySigner& operator=(SharedKeySigner&&) = delete;

    /// <summary>Gets the storage account name the signatures are made for.</summary>
    /// <returns>The account name, valid for as long as the signer is.</returns>
    [[nodiscard]] const std::string& AccountName() const noexcept AVEVA_LIFETIMEBOUND { return m_accountName; }

    /// <summary>Base64 HMAC-SHA256 of <paramref name="stringToSign"/>.</summary>
    /// <param name="stringToSign">The text to sign.</param>
    /// <returns>The base64 signature.</returns>
    /// <exception cref="std::runtime_error">Thrown when the signature cannot be computed.</exception>
    [[nodiscard]] std::string Sign(std::string_view stringToSign) const;
    /// <summary>The full "SharedKey &lt;account&gt;:&lt;signature&gt;" Authorization header     value for
        /// <paramref name="request"/>.</summary>
    /// <param name="request">The request to sign.</param>
    /// <returns>The Authorization header value.</returns>
    /// <exception cref="std::runtime_error">Thrown when the signature cannot be computed.</exception>
    [[nodiscard]] std::string Authorize(const HttpRequest& request) const;

  private:
    struct Impl;
    std::string m_accountName;
    std::unique_ptr<Impl> m_impl;
};

/// <summary>The Shared Key string-to-sign for <paramref name="request"/> (Blob service, version 2009-09-19 and
/// later).</summary>
/// <param name="accountName">The storage account name.</param>
/// <param name="request">The request to build the string-to-sign for.</param>
/// <returns>The text that is signed.</returns>
[[nodiscard]] std::string BuildSharedKeyStringToSign(std::string_view accountName, const HttpRequest& request);

/// <summary>
/// Normalised, validated, immutable connection settings shared by a service client and every
/// container/blob client derived from it.
/// </summary>
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

/// <summary>A container within a connection, with its request URL prefix precomputed.</summary>
struct ContainerTarget {
    std::shared_ptr<const ConnectionState> Connection;
    std::string ContainerName;
    // "<scheme://host[:port]><basePath>/<encoded container>", built once so each request only appends its query.
    std::string EncodedResource;
};

/// <summary>A blob within a connection, with its request URL prefix precomputed.</summary>
struct BlobTarget {
    std::shared_ptr<const ConnectionState> Connection;
    std::string ContainerName;
    std::string BlobName;
    // "<scheme://host[:port]><basePath>/<encoded container>/<encoded blob>", built once so each request only
    // appends its query string.
    std::string EncodedResource;
};

/// <summary>
/// Normalise and validate client options (throw std::invalid_argument on invalid settings).
/// <paramref name="tokenCredential"/> tells the validation whether the client was also given a token credential (which
/// lives in the
/// client targets, not in the connection), so the "at most one credential type" rule covers it.
/// </summary>
/// <param name="options">The service client's options; the account key in this copy is wiped before returning.</param>
/// <param name="tokenCredential">Whether the client was also given a token credential.</param>
/// <returns>The shared connection settings.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobServiceClientOptions options,
                                                                         CredentialPresence tokenCredential);
/// <summary>Normalises and validates a container client's options in the same way as the service client
/// overload.</summary>
/// <param name="options">The container client's options; the account key in this copy is wiped before
/// returning.</param>
/// <param name="tokenCredential">Whether the client was also given a token credential.</param>
/// <returns>The shared connection settings.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobContainerClientOptions options,
                                                                         CredentialPresence tokenCredential);
/// <summary>Normalises and validates a blob client's options in the same way as the service client overload.</summary>
/// <param name="options">The blob client's options; the account key in this copy is wiped before returning.</param>
/// <param name="tokenCredential">Whether the client was also given a token credential.</param>
/// <returns>The shared connection settings.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] std::shared_ptr<const ConnectionState> MakeConnectionState(BlobClientOptions options,
                                                                         CredentialPresence tokenCredential);
/// <summary>Addresses a container of an existing connection.</summary>
/// <param name="connection">The connection the container belongs to.</param>
/// <param name="containerName">The container name, which is validated against the service's naming rules.</param>
/// <returns>The target, with its encoded resource URL built once.</returns>
/// <exception cref="std::invalid_argument">Thrown when <paramref name="containerName"/> is not a valid container
/// name.</exception>
[[nodiscard]] ContainerTarget MakeContainerTarget(std::shared_ptr<const ConnectionState> connection,
                                                  std::string containerName);
/// <summary>Addresses a blob of an existing connection.</summary>
/// <param name="connection">The connection the blob belongs to.</param>
/// <param name="containerName">The container name, which is validated against the service's naming rules.</param>
/// <param name="blobName">The blob name, which must not be empty.</param>
/// <returns>The target, with its encoded resource URL built once.</returns>
/// <exception cref="std::invalid_argument">Thrown when either name is invalid.</exception>
[[nodiscard]] BlobTarget MakeBlobTarget(std::shared_ptr<const ConnectionState> connection, std::string containerName,
                                        std::string blobName);
/// <summary>Validates the options, then addresses the container they name.</summary>
/// <param name="options">The container client's options.</param>
/// <param name="tokenCredential">Whether the client was also given a token credential.</param>
/// <returns>The target.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options or the container name are invalid.</exception>
[[nodiscard]] ContainerTarget MakeContainerTarget(const BlobContainerClientOptions& options,
                                                  CredentialPresence tokenCredential);
/// <summary>Validates the options, then addresses the blob they name.</summary>
/// <param name="options">The blob client's options.</param>
/// <param name="tokenCredential">Whether the client was also given a token credential.</param>
/// <returns>The target.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options or either name are invalid.</exception>
[[nodiscard]] BlobTarget MakeBlobTarget(const BlobClientOptions& options, CredentialPresence tokenCredential);

/// <summary>Tells whether a credential handle holds a credential.</summary>
/// <typeparam name="T">The credential type.</typeparam>
/// <param name="credential">The handle to inspect.</param>
/// <returns>Present when the handle is not null.</returns>
template <class T>
[[nodiscard]] constexpr CredentialPresence PresenceOf(const std::shared_ptr<T>& credential) noexcept {
    return credential != nullptr ? CredentialPresence::Present : CredentialPresence::Absent;
}

/// <summary>
/// The credential the clients actually authorize with: the caller's credential wrapped in a token cache (so
/// identity-provider credentials do not fetch a token per request), or null when none was given.
/// </summary>
/// <typeparam name="TCred">The caller's credential type.</typeparam>
/// <param name="credential">The caller's credential, which may be null.</param>
/// <returns>The cached credential; the input itself when it already caches.</returns>
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

/// <summary>A container target that also carries the (cached) credential, so every request path sees the concrete
/// credential type.</summary>
/// <typeparam name="TCred">The credential type the target authorizes with.</typeparam>
template <class TCred> struct BasicContainerTarget : ContainerTarget {
    std::shared_ptr<CachedTokenCredential<TCred>> Credential;
};

/// <summary>A blob target that also carries the (cached) credential, so every request path sees the concrete credential
/// type.</summary>
/// <typeparam name="TCred">The credential type the target authorizes with.</typeparam>
template <class TCred> struct BasicBlobTarget : BlobTarget {
    std::shared_ptr<CachedTokenCredential<TCred>> Credential;
};

/// <summary>Addresses a container of an existing connection with an already cached credential.</summary>
/// <typeparam name="TCred">The credential type the target authorizes with.</typeparam>
/// <param name="connection">The connection the container belongs to.</param>
/// <param name="credential">The cached credential to authorize with.</param>
/// <param name="containerName">The container name, which is validated against the service's naming rules.</param>
/// <returns>The target.</returns>
/// <exception cref="std::invalid_argument">Thrown when <paramref name="containerName"/> is not a valid container
/// name.</exception>
template <TokenCredentialLike TCred>
[[nodiscard]] BasicContainerTarget<TCred> MakeContainerTarget(std::shared_ptr<const ConnectionState> connection,
                                                              std::shared_ptr<CachedTokenCredential<TCred>> credential,
                                                              std::string containerName) {
    return {MakeContainerTarget(std::move(connection), std::move(containerName)), std::move(credential)};
}

/// <summary>Addresses a blob of an existing connection with an already cached credential.</summary>
/// <typeparam name="TCred">The credential type the target authorizes with.</typeparam>
/// <param name="connection">The connection the blob belongs to.</param>
/// <param name="credential">The cached credential to authorize with.</param>
/// <param name="containerName">The container name, which is validated against the service's naming rules.</param>
/// <param name="blobName">The blob name, which must not be empty.</param>
/// <returns>The target.</returns>
/// <exception cref="std::invalid_argument">Thrown when either name is invalid.</exception>
template <TokenCredentialLike TCred>
[[nodiscard]] BasicBlobTarget<TCred> MakeBlobTarget(std::shared_ptr<const ConnectionState> connection,
                                                    std::shared_ptr<CachedTokenCredential<TCred>> credential,
                                                    std::string containerName, std::string blobName) {
    return {MakeBlobTarget(std::move(connection), std::move(containerName), std::move(blobName)),
            std::move(credential)};
}

/// <summary>Validates the options, then addresses their container, wrapping the caller's credential in a token
/// cache.</summary>
/// <typeparam name="TCred">The caller's credential type.</typeparam>
/// <param name="options">The container client's options.</param>
/// <param name="credential">The caller's credential, which may be null.</param>
/// <returns>The target.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options or the container name are invalid.</exception>
template <TokenCredentialLike TCred>
[[nodiscard]] BasicContainerTarget<TCred> MakeContainerTarget(const BlobContainerClientOptions& options,
                                                              std::shared_ptr<TCred> credential) {
    return {MakeContainerTarget(options, PresenceOf(credential)), MakeCachedCredential(std::move(credential))};
}

/// <summary>Validates the options, then addresses their blob, wrapping the caller's credential in a token
/// cache.</summary>
/// <typeparam name="TCred">The caller's credential type.</typeparam>
/// <param name="options">The blob client's options.</param>
/// <param name="credential">The caller's credential, which may be null.</param>
/// <returns>The target.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options or either name are invalid.</exception>
template <TokenCredentialLike TCred>
[[nodiscard]] BasicBlobTarget<TCred> MakeBlobTarget(const BlobClientOptions& options,
                                                    std::shared_ptr<TCred> credential) {
    return {MakeBlobTarget(options, PresenceOf(credential)), MakeCachedCredential(std::move(credential))};
}

/// <summary>The header naming the service API version a request is made against.</summary>
inline constexpr std::string_view XMsVersionHeaderName = "x-ms-version";
/// <summary>The header carrying the request time, which Shared Key signatures cover.</summary>
inline constexpr std::string_view XMsDateHeaderName = "x-ms-date";
/// <summary>The header carrying a client-chosen id that the service echoes in its logs.</summary>
inline constexpr std::string_view XMsClientRequestIdHeaderName = "x-ms-client-request-id";
/// <summary>The response header carrying the id the service gave the request.</summary>
inline constexpr std::string_view XMsRequestIdHeaderName = "x-ms-request-id";
/// <summary>The response header carrying the service's error code.</summary>
inline constexpr std::string_view XMsErrorCodeHeaderName = "x-ms-error-code";
/// <summary>The header carrying a CRC64 checksum of the content.</summary>
inline constexpr std::string_view XMsContentCrc64HeaderName = "x-ms-content-crc64";
/// <summary>The response header telling whether a delete removed the blob permanently.</summary>
inline constexpr std::string_view XMsDeleteTypePermanentHeaderName = "x-ms-delete-type-permanent";
/// <summary>The header carrying the request's credentials.</summary>
inline constexpr std::string_view AuthorizationHeaderName = "Authorization";

/// <summary>
/// "Bearer &lt;token&gt;" built with a single allocation, without first copying the token into a temporary string.
/// </summary>
/// <param name="token">The bearer token.</param>
/// <returns>The Authorization header value.</returns>
[[nodiscard]] std::string MakeBearerValue(std::string_view token);
/// <summary>The prefix of the headers that carry blob metadata.</summary>
inline constexpr std::string_view XMsMetaHeaderPrefix = "x-ms-meta-";
/// <summary>The header naming the type of a blob.</summary>
inline constexpr std::string_view XMsBlobTypeHeaderName = "x-ms-blob-type";
/// <summary>The header carrying the size of a page blob.</summary>
inline constexpr std::string_view XMsBlobContentLengthHeaderName = "x-ms-blob-content-length";
/// <summary>The header selecting whether a Put Page updates or clears a range.</summary>
inline constexpr std::string_view XMsPageWriteHeaderName = "x-ms-page-write";
/// <summary>The header carrying a byte range, used where the standard Range header is not.</summary>
inline constexpr std::string_view XMsRangeHeaderName = "x-ms-range";
/// <summary>The header naming the source of a copy.</summary>
inline constexpr std::string_view XMsCopySourceHeaderName = "x-ms-copy-source";
/// <summary>The header carrying the byte range to read from a copy source.</summary>
inline constexpr std::string_view XMsSourceRangeHeaderName = "x-ms-source-range";
/// <summary>The header carrying the ETag a copy source must match.</summary>
inline constexpr std::string_view XMsSourceIfMatchHeaderName = "x-ms-source-if-match";
/// <summary>The header carrying the credentials for reading a copy source.</summary>
inline constexpr std::string_view XMsCopySourceAuthorizationHeaderName = "x-ms-copy-source-authorization";
/// <summary>
/// Placeholder value of x-ms-copy-source-authorization that the request pipeline replaces, per attempt, with the
/// credential the client itself authorizes with (a fresh token on every retry), or removes when that credential
/// is not a bearer token.
/// </summary>
inline constexpr std::string_view CopySourceClientCredentialMarker = "{client-credential}";
/// <summary>The header carrying the id of a lease.</summary>
inline constexpr std::string_view XMsLeaseIdHeaderName = "x-ms-lease-id";
/// <summary>The header naming the lease operation to perform.</summary>
inline constexpr std::string_view XMsLeaseActionHeaderName = "x-ms-lease-action";
/// <summary>The header carrying the duration of a lease, in seconds, or -1 for an infinite one.</summary>
inline constexpr std::string_view XMsLeaseDurationHeaderName = "x-ms-lease-duration";
/// <summary>The header carrying the lease id the caller proposes.</summary>
inline constexpr std::string_view XMsProposedLeaseIdHeaderName = "x-ms-proposed-lease-id";
/// <summary>The header carrying the version identifier of a resource.</summary>
inline constexpr std::string_view ETagHeaderName = "ETag";
/// <summary>The header carrying the time a resource was last modified.</summary>
inline constexpr std::string_view LastModifiedHeaderName = "Last-Modified";
/// <summary>The header carrying the size of a message body.</summary>
inline constexpr std::string_view ContentLengthHeaderName = "Content-Length";
/// <summary>The header carrying the media type of a message body.</summary>
inline constexpr std::string_view ContentTypeHeaderName = "Content-Type";
/// <summary>The header carrying the MD5 hash of the content.</summary>
inline constexpr std::string_view ContentMd5HeaderName = "Content-MD5";
/// <summary>The header carrying a blob's caching directives.</summary>
inline constexpr std::string_view CacheControlHeaderName = "Cache-Control";
/// <summary>The header carrying a blob's content encoding.</summary>
inline constexpr std::string_view ContentEncodingHeaderName = "Content-Encoding";
/// <summary>The header carrying a blob's content language.</summary>
inline constexpr std::string_view ContentLanguageHeaderName = "Content-Language";
/// <summary>The header carrying the byte range to read.</summary>
inline constexpr std::string_view RangeHeaderName = "Range";
/// <summary>The header making a request conditional on the resource's ETag matching.</summary>
inline constexpr std::string_view IfMatchHeaderName = "If-Match";
/// <summary>The header making a request conditional on the resource's ETag not matching.</summary>
inline constexpr std::string_view IfNoneMatchHeaderName = "If-None-Match";
/// <summary>The header making a request conditional on the resource having changed since a time.</summary>
inline constexpr std::string_view IfModifiedSinceHeaderName = "If-Modified-Since";
/// <summary>The header making a request conditional on the resource being unchanged since a time.</summary>
inline constexpr std::string_view IfUnmodifiedSinceHeaderName = "If-Unmodified-Since";

/// <summary>A failed request's error code together with the service details, when there are any.</summary>
struct RequestFailure {
    std::error_code Error;
    std::optional<BlobStorageError> Details;
};

/// <summary>Locale-free ASCII folding: the std::locale based algorithms take a process-wide lock on MSVC per
/// character.</summary>
/// <param name="value">The character to fold.</param>
/// <returns>The lowercase form of an ASCII letter, otherwise <paramref name="value"/> unchanged.</returns>
[[nodiscard]] constexpr char FoldAscii(char value) noexcept {
    return (value >= 'A' && value <= 'Z') ? static_cast<char>(value | 0x20) : value;
}

/// <summary>The HTTP client a shared_ptr constructor keeps alive; a null handle is a caller error.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <param name="httpClient">The handle to dereference.</param>
/// <returns>The HTTP client the handle points at.</returns>
/// <exception cref="std::invalid_argument">Thrown when <paramref name="httpClient"/> is null.</exception>
template <class THttp> THttp& RequireHttpClient(const std::shared_ptr<THttp>& httpClient) {
    if (!httpClient) {
        throw std::invalid_argument("The HTTP client must not be null.");
    }
    return *httpClient;
}

/// <summary>Compares two texts, ignoring ASCII case.</summary>
/// <param name="lhs">The first text.</param>
/// <param name="rhs">The second text.</param>
/// <returns>Match when the texts are equal apart from ASCII case.</returns>
[[nodiscard]] constexpr StringMatch IEquals(std::string_view lhs, std::string_view rhs) noexcept {
    return std::ranges::equal(lhs, rhs, {}, FoldAscii, FoldAscii) ? StringMatch::Match : StringMatch::Mismatch;
}

/// <summary>Checks, ignoring ASCII case, whether a text begins with a prefix.</summary>
/// <param name="value">The text to inspect.</param>
/// <param name="prefix">The prefix to look for.</param>
/// <returns>Match when <paramref name="value"/> begins with <paramref name="prefix"/>.</returns>
[[nodiscard]] constexpr StringMatch IStartsWith(std::string_view value, std::string_view prefix) noexcept {
    return value.size() >= prefix.size() ? IEquals(value.substr(0, prefix.size()), prefix) : StringMatch::Mismatch;
}

/// <summary>Lowercases the ASCII letters of a text, leaving every other character alone.</summary>
/// <param name="value">The text to convert.</param>
/// <returns>The converted copy.</returns>
[[nodiscard]] std::string ToLowerAscii(std::string_view value);
/// <summary>"localhost", any 127.0.0.0/8 address and ::1 are loopback.</summary>
/// <param name="url">The URL whose host is classified.</param>
/// <returns>Loopback for those hosts, otherwise Remote.</returns>
[[nodiscard]] HostLocality LocalityOf(const boost::urls::url_view_base& url) noexcept;
/// <summary>Removes leading and trailing whitespace.</summary>
/// <param name="value">The text to trim, which the result points into.</param>
/// <returns>The trimmed view; empty when nothing but whitespace remains.</returns>
[[nodiscard]] std::string_view TrimWhitespace(std::string_view value AVEVA_LIFETIMEBOUND) noexcept;
/// <summary>
/// Whether <paramref name="value"/> contains CR, LF or NUL (header injection / truncation). Every other C0 control
/// except HTAB, and DEL, count too, as they are illegal in a header field as well.
/// </summary>
/// <param name="value">The text to inspect.</param>
/// <returns>Present when such a character occurs.</returns>
[[nodiscard]] ControlCharacters ControlCharactersIn(std::string_view value) noexcept;
/// <summary>
/// A header is valid when it has a non-empty name without ':' and neither name nor value has control characters, so it
/// cannot be re-parsed differently on the wire, in a signature or in a multipart body.
/// </summary>
/// <param name="header">The header to check.</param>
/// <returns>Valid when the header can be sent as is.</returns>
[[nodiscard]] Validity WireValidityOf(const HttpHeader& header) noexcept;
/// <summary>
/// Throws std::invalid_argument unless <paramref name="value"/> is canonical padded base64; returns the decoded length.
/// An empty value is accepted only when <paramref name="emptyValue"/> is Allow.
/// </summary>
/// <param name="value">The text to validate.</param>
/// <param name="emptyValue">Whether an empty value is acceptable.</param>
/// <returns>The number of bytes the text decodes to.</returns>
/// <exception cref="std::invalid_argument">Thrown when the text is not canonical padded base64, or is empty and
/// rejected.</exception>
[[nodiscard]] std::size_t ValidateBase64AndGetDecodedLength(std::string_view value, EmptyValue emptyValue);
/// <summary>Finds a response header by name, ignoring ASCII case; the first occurrence wins.</summary>
/// <param name="response">The response to search.</param>
/// <param name="headerName">The name of the header.</param>
/// <returns>The header's value, or an empty view when there is no such header.</returns>
[[nodiscard]] std::string_view FindHeaderValue(const HttpResponse& response AVEVA_LIFETIMEBOUND,
                                               std::string_view headerName) noexcept;

/// <summary>Parses a "true"/"false" header or element into a two-valued enum whose second enumerator stands for
/// true.</summary>
/// <typeparam name="TFlag">The two-valued enum to produce.</typeparam>
/// <param name="value">The text to parse.</param>
/// <returns>The second enumerator for "true" (ignoring case), otherwise the first.</returns>
template <class TFlag>
    requires(std::is_enum_v<TFlag> && std::is_same_v<std::underlying_type_t<TFlag>, std::uint8_t>)
[[nodiscard]] constexpr TFlag ParseFlag(std::string_view value) noexcept {
    return static_cast<TFlag>(IEquals(value, "true") == StringMatch::Match ? 1U : 0U);
}
/// <summary>Parses a lease status header or element.</summary>
/// <param name="value">The text to parse.</param>
/// <returns>The status, or Unknown when the text is empty or not recognised.</returns>
[[nodiscard]] Models::LeaseStatus ParseLeaseStatus(std::string_view value);
/// <summary>Parses a lease state header or element.</summary>
/// <param name="value">The text to parse.</param>
/// <returns>The state, or Unknown when the text is empty or not recognised.</returns>
[[nodiscard]] Models::LeaseState ParseLeaseState(std::string_view value);
/// <summary>Parses a lease duration type header or element.</summary>
/// <param name="value">The text to parse.</param>
/// <returns>The duration type, or Unknown when the text is empty or not recognised.</returns>
[[nodiscard]] Models::LeaseDurationType ParseLeaseDurationType(std::string_view value);
/// <summary>Parses an HTTP-date in any of the RFC 1123, RFC 850 or asctime formats.</summary>
/// <param name="value">The header value, which may have surrounding whitespace.</param>
/// <returns>The time, or nothing when the text is not a valid HTTP-date.</returns>
[[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseHttpDateHeader(std::string_view value) noexcept;
/// <summary>Formats a time as an RFC 1123 HTTP-date in GMT, truncated to whole seconds.</summary>
/// <param name="now">The time to format.</param>
/// <returns>The header value.</returns>
[[nodiscard]] std::string BuildDateHeaderValue(std::chrono::system_clock::time_point now);
/// <summary>Removes every trailing '/'.</summary>
/// <param name="value">The text to trim, which the result points into.</param>
/// <returns>The trimmed view.</returns>
[[nodiscard]] std::string_view TrimTrailingSlashes(std::string_view value AVEVA_LIFETIMEBOUND) noexcept;
/// <summary>Removes one leading '?', if there is one.</summary>
/// <param name="value">The text to trim, which the result points into.</param>
/// <returns>The trimmed view.</returns>
[[nodiscard]] std::string_view TrimLeadingQuestionMark(std::string_view value AVEVA_LIFETIMEBOUND) noexcept;
/// <summary>Creates a random UUID to identify one request in the service's logs.</summary>
/// <returns>The UUID in its canonical text form.</returns>
[[nodiscard]] std::string CreateClientRequestId();
/// <summary>RFC 7231 date name tables shared by the request formatter and the response parsers; this one lists the
/// three-letter month names, January first.</summary>
inline constexpr std::array<std::string_view, 12> MonthNames{"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
/// <summary>The three-letter weekday names, Sunday first; see <see cref="MonthNames"/>.</summary>
inline constexpr std::array<std::string_view, 7> ShortWeekdayNames{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
/// <summary>The full weekday names, Sunday first; see <see cref="MonthNames"/>.</summary>
inline constexpr std::array<std::string_view, 7> LongWeekdayNames{"Sunday",   "Monday", "Tuesday", "Wednesday",
                                                                  "Thursday", "Friday", "Saturday"};

/// <summary>Views bytes as characters without copying.</summary>
/// <param name="bytes">The bytes to view.</param>
/// <returns>A mutable view of the same memory.</returns>
[[nodiscard]] std::span<char> AsChars(std::span<std::byte> bytes AVEVA_LIFETIMEBOUND) noexcept;
/// <summary>Views bytes as text without copying.</summary>
/// <param name="bytes">The bytes to view.</param>
/// <returns>A view of the same memory.</returns>
[[nodiscard]] std::string_view AsChars(std::span<const std::byte> bytes AVEVA_LIFETIMEBOUND) noexcept;
/// <summary>Copies bytes into a string.</summary>
/// <param name="bytes">The bytes to copy.</param>
/// <returns>The string.</returns>
[[nodiscard]] std::string BytesToString(std::span<const std::byte> bytes);
/// <summary>Encodes bytes as padded base64.</summary>
/// <param name="bytes">The bytes to encode.</param>
/// <returns>The base64 text; empty for empty input.</returns>
/// <exception cref="gsl::narrowing_error">Thrown when the input is too large for the encoder.</exception>
[[nodiscard]] std::string Base64Encode(std::span<const std::byte> bytes);
/// <summary>Percent-encodes everything but unreserved characters and <paramref name="extraSafeChars"/>.</summary>
/// <param name="value">The text to encode.</param>
/// <param name="extraSafeChars">Further characters to leave as they are.</param>
/// <returns>The encoded text.</returns>
[[nodiscard]] std::string UrlEncode(std::string_view value, std::string_view extraSafeChars);
/// <summary>Appends the percent-encoded <paramref name="value"/> to <paramref name="out"/> without an intermediate
/// string.</summary>
/// <param name="out">The string to append to.</param>
/// <param name="value">The text to encode.</param>
/// <param name="extraSafeChars">Further characters, besides the unreserved ones, to leave as they are.</param>
void UrlEncodeAppend(std::string& out, std::string_view value, std::string_view extraSafeChars);
/// <summary>Joins encoded name=value pairs with '&amp;', without a leading '?'.</summary>
/// <param name="parameters">The names and values, which are percent-encoded.</param>
/// <returns>The query string.</returns>
[[nodiscard]] std::string BuildQueryString(const std::vector<std::pair<std::string, std::string>>& parameters);
/// <summary>Formats a byte range as a Range header value, "bytes=first-last" or "bytes=first-" when
/// open-ended.</summary>
/// <param name="range">The range to format.</param>
/// <returns>The header value.</returns>
/// <exception cref="std::invalid_argument">Thrown when the length is zero or the range's end does not fit in 64
/// bits.</exception>
[[nodiscard]] std::string BuildRangeHeaderValue(const Models::BlobByteRange& range);

/// <summary>Decides whether a request failed, and why.</summary>
/// <param name="transportError">The transport error of the request, if any.</param>
/// <param name="response">The response received; a status of 300 or above is a failure.</param>
/// <returns>An empty failure on success, otherwise the error code with the details the service reported.</returns>
[[nodiscard]] RequestFailure DetermineBlobStorageFailure(std::error_code transportError, const HttpResponse& response);
/// <summary>A successful response whose content could not be parsed (BlobStorageErrorCode::InvalidResponse).</summary>
/// <param name="response">The response that could not be parsed.</param>
/// <param name="message">What was wrong with it.</param>
/// <returns>The failure.</returns>
[[nodiscard]] RequestFailure MakeInvalidResponseFailure(const HttpResponse& response, std::string message);

/// <summary>The parts of a Content-Range header.</summary>
struct ParsedContentRange {
    std::uint64_t Start = 0;
    std::uint64_t End = 0;
    std::optional<std::uint64_t> Total;
    // Whether this is "bytes */total" (an unsatisfied range); Start and End carry no meaning then.
    RangeSatisfaction Satisfaction = RangeSatisfaction::Satisfied;
};

/// <summary>Parses a "bytes start-end/total" or "bytes */total" Content-Range header.</summary>
/// <param name="header">The header value.</param>
/// <returns>The parsed range, or nothing when the header is malformed.</returns>
[[nodiscard]] std::optional<ParsedContentRange> ParseContentRange(std::string_view header) noexcept;

/// <summary>Reads a blob's properties and metadata from response headers.</summary>
/// <param name="response">The response to read.</param>
/// <returns>The properties; the content length comes from Content-Range when that is present.</returns>
/// <exception cref="std::invalid_argument">Thrown when a header that is present cannot be parsed.</exception>
[[nodiscard]] Models::BlobProperties ParseBlobProperties(const HttpResponse& response);
/// <summary>Reads the outcome of a delete from response headers.</summary>
/// <param name="response">The response to read.</param>
/// <returns>The result.</returns>
[[nodiscard]] Models::DeleteBlobResult ParseDeleteBlobResult(const HttpResponse& response);
/// <summary>Reads the outcome of acquiring a lease from response headers.</summary>
/// <param name="response">The response to read.</param>
/// <returns>The result.</returns>
/// <exception cref="std::invalid_argument">Thrown when Last-Modified is not a valid HTTP-date.</exception>
[[nodiscard]] Models::AcquireBlobLeaseResult ParseAcquireBlobLeaseResult(const HttpResponse& response);
/// <summary>Reads the outcome of releasing a lease from response headers.</summary>
/// <param name="response">The response to read.</param>
/// <returns>The result.</returns>
/// <exception cref="std::invalid_argument">Thrown when Last-Modified is not a valid HTTP-date.</exception>
[[nodiscard]] Models::ReleaseBlobLeaseResult ParseReleaseBlobLeaseResult(const HttpResponse& response);
/// <summary>Reads the outcome of renewing a lease from response headers.</summary>
/// <param name="response">The response to read.</param>
/// <returns>The result.</returns>
/// <exception cref="std::invalid_argument">Thrown when Last-Modified is not a valid HTTP-date.</exception>
[[nodiscard]] Models::RenewBlobLeaseResult ParseRenewBlobLeaseResult(const HttpResponse& response);
/// <summary>Parses "YYYY-MM-DDThh:mm:ss[.fffffff]Z"; std::nullopt if malformed.</summary>
/// <param name="v">The text to parse.</param>
/// <returns>The time, or nothing when the text is malformed.</returns>
[[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseIso8601Utc(std::string_view value) noexcept;

/// <summary>Reads a List Blobs response body.</summary>
/// <param name="xml">The response body.</param>
/// <returns>The listed blobs and the markers for the next page.</returns>
/// <exception cref="std::invalid_argument">Thrown when the body is not the expected XML document.</exception>
[[nodiscard]] Models::ListBlobsResult ParseListBlobsResultXml(std::string_view xml);
/// <summary>
/// Parses a Get Page Ranges response: the &lt;PageList&gt; body plus the ETag, Last-Modified and
/// x-ms-blob-content-length headers.
/// </summary>
/// <param name="response">The response to read.</param>
/// <returns>The result.</returns>
/// <exception cref="std::invalid_argument">Thrown when the body or a header cannot be parsed.</exception>
[[nodiscard]] Models::GetPageRangesResult ParseGetPageRangesResult(const HttpResponse& response);

/// <summary>Starts a request for a blob, with the API version and a fresh client request id headers set.</summary>
/// <param name="target">The blob to address.</param>
/// <param name="method">The HTTP method.</param>
/// <param name="queryString">Query parameters to add; the connection's SAS token is appended to them.</param>
/// <returns>The request.</returns>
[[nodiscard]] HttpRequest BuildBlobRequest(const BlobTarget& target, HttpMethod method,
                                           std::string_view queryString = {});
/// <summary>Builds the URL of a blob.</summary>
/// <param name="target">The blob to address.</param>
/// <param name="queryString">Query parameters to add; the connection's SAS token is appended to them.</param>
/// <returns>The URL.</returns>
[[nodiscard]] std::string BuildBlobUrl(const BlobTarget& target, std::string_view queryString = {});
/// <summary>Starts a request for a container, with the API version and a fresh client request id headers set.</summary>
/// <param name="target">The container to address.</param>
/// <param name="method">The HTTP method.</param>
/// <param name="queryString">Query parameters to add after restype=container; the SAS token is appended too.</param>
/// <returns>The request.</returns>
[[nodiscard]] HttpRequest BuildContainerRequest(const ContainerTarget& target, HttpMethod method,
                                                std::string_view queryString = {});
/// <summary>Builds the URL of a container, which always carries restype=container.</summary>
/// <param name="target">The container to address.</param>
/// <param name="queryString">Query parameters to add after restype=container; the SAS token is appended too.</param>
/// <returns>The URL.</returns>
[[nodiscard]] std::string BuildContainerUrl(const ContainerTarget& target, std::string_view queryString = {});
/// <summary>Builds the URL of the storage account's service endpoint.</summary>
/// <param name="connection">The connection whose endpoint is addressed.</param>
/// <param name="queryString">Query parameters to add; the connection's SAS token is appended to them.</param>
/// <returns>The URL.</returns>
[[nodiscard]] std::string BuildServiceUrl(const ConnectionState& connection, std::string_view queryString = {});

/// <summary>Convenience overloads that normalise and validate client options first (tests).</summary>
/// <param name="options">The blob client's options.</param>
/// <param name="queryString">Query parameters to add; the SAS token is appended to them.</param>
/// <returns>The URL.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] std::string BuildBlobUrl(const BlobClientOptions& options, std::string_view queryString = {});
/// <summary>Starts a request for the blob the options name, after validating them.</summary>
/// <param name="options">The blob client's options.</param>
/// <param name="method">The HTTP method.</param>
/// <param name="queryString">Query parameters to add; the SAS token is appended to them.</param>
/// <returns>The request.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] HttpRequest BuildBlobRequest(const BlobClientOptions& options, HttpMethod method,
                                           std::string_view queryString = {});
/// <summary>Builds the URL of the container the options name, after validating them.</summary>
/// <param name="options">The container client's options.</param>
/// <param name="queryString">Query parameters to add after restype=container; the SAS token is appended too.</param>
/// <returns>The URL.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] std::string BuildContainerUrl(const BlobContainerClientOptions& options,
                                            std::string_view queryString = {});
/// <summary>Starts a request for the container the options name, after validating them.</summary>
/// <param name="options">The container client's options.</param>
/// <param name="method">The HTTP method.</param>
/// <param name="queryString">Query parameters to add after restype=container; the SAS token is appended too.</param>
/// <returns>The request.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] HttpRequest BuildContainerRequest(const BlobContainerClientOptions& options, HttpMethod method,
                                                std::string_view queryString = {});
/// <summary>Builds the URL of the service endpoint the options name, after validating them.</summary>
/// <param name="options">The service client's options.</param>
/// <param name="queryString">Query parameters to add; the SAS token is appended to them.</param>
/// <returns>The URL.</returns>
/// <exception cref="std::invalid_argument">Thrown when the options are invalid.</exception>
[[nodiscard]] std::string BuildServiceUrl(const BlobServiceClientOptions& options, std::string_view queryString = {});

/// <summary>Adds a header, copying its value.</summary>
/// <param name="request">The request to add to.</param>
/// <param name="name">The header name.</param>
/// <param name="value">The header value.</param>
void AddHeader(HttpRequest& request, std::string_view name, std::string_view value);
/// <summary>Adds a header, taking over its value.</summary>
/// <param name="request">The request to add to.</param>
/// <param name="name">The header name.</param>
/// <param name="value">The header value, which is moved from.</param>
void AddHeader(HttpRequest& request, std::string_view name, std::string&& value);
/// <summary>Adds a header, copying its value, unless the value is empty.</summary>
/// <param name="request">The request to add to.</param>
/// <param name="name">The header name.</param>
/// <param name="value">The header value.</param>
void AddHeaderIfNotEmpty(HttpRequest& request, std::string_view name, std::string_view value);
/// <summary>Adds a header, taking over its value, unless the value is empty.</summary>
/// <param name="request">The request to add to.</param>
/// <param name="name">The header name.</param>
/// <param name="value">The header value, which is moved from only when it is added.</param>
void AddHeaderIfNotEmpty(HttpRequest& request, std::string_view name, std::string&& value);
/// <summary>Adds the headers that make a request conditional; empty conditions add nothing.</summary>
/// <param name="request">The request to add to.</param>
/// <param name="conditions">The conditions to apply.</param>
void ApplyBlobRequestConditions(HttpRequest& request, const Models::BlobRequestConditions& conditions);
/// <summary>Adds one x-ms-meta- header per metadata entry.</summary>
/// <param name="request">The request to add to.</param>
/// <param name="metadata">The metadata to add.</param>
void ApplyMetadata(HttpRequest& request, const Models::MetadataMap& metadata);
/// <summary>Azure requires metadata names to be C# identifiers: [A-Za-z_][A-Za-z0-9_]*.</summary>
/// <param name="name">The metadata name.</param>
/// <returns>Valid when the name is acceptable.</returns>
[[nodiscard]] Validity ValidityOfMetadataName(std::string_view name) noexcept;
/// <summary>One-off Shared Key signing (constructs a temporary signer); clients use ConnectionState::Signer.</summary>
/// <param name="options">The account name and key to sign with.</param>
/// <param name="request">The request to add the Authorization header to.</param>
/// <exception cref="std::invalid_argument">Thrown when the account key is not valid base64.</exception>
void AuthorizeRequest(const SharedKeyCredentialOptions& options, HttpRequest& request);
/// <summary>Signs a request with the connection's signer and adds the Authorization header.</summary>
/// <param name="signer">The signer to use.</param>
/// <param name="request">The request to add the Authorization header to.</param>
void AuthorizeRequest(const SharedKeySigner& signer, HttpRequest& request);

/// <summary>Maps the exception being handled to a BlobStorageError; must be called from a catch handler.</summary>
/// <returns>The failure that describes the exception.</returns>
[[nodiscard]] BlobStorageError MakeFailureFromCurrentException();

/// <summary>A BlobStorageError carrying only <paramref name="error"/> and <paramref name="message"/> (defaulting to
/// error.message()).</summary>
/// <param name="error">The error to report.</param>
/// <param name="message">The message; empty to use the error's own.</param>
/// <returns>The failure.</returns>
[[nodiscard]] BlobStorageError MakeClientError(std::error_code error, std::string message = {});

/// <summary>The failure's service details (or a client error built lazily) with Code set to the failure's
/// error.</summary>
/// <param name="failure">The failure to convert; its details are moved from.</param>
/// <returns>The details.</returns>
[[nodiscard]] inline BlobStorageError ToDetails(RequestFailure&& failure) {
    BlobStorageError details =
        failure.Details.has_value() ? std::move(*failure.Details) : MakeClientError(failure.Error);
    details.Code = failure.Error;
    return details;
}

/// <summary>
/// Folds a transport/service std::error_code into the single std::expected error channel. Success
/// is governed solely by <paramref name="error"/>: Exists*/*IfExists* clear it while leaving the suppressed
/// diagnostics (e.g. the 404's request id) available via Response&lt;T&gt;::Error().
/// </summary>
/// <typeparam name="T">The response's value type.</typeparam>
/// <param name="error">The error, if any.</param>
/// <param name="response">The response, whose diagnostics become the failure's details.</param>
/// <returns>The response on success, otherwise the failure.</returns>
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

/// <summary>A client-side failure (e.g. argument validation) with <paramref name="message"/> defaulting to
/// error.message().</summary>
/// <typeparam name="T">The response's value type.</typeparam>
/// <param name="error">The error to report.</param>
/// <param name="message">The message; empty to use the error's own.</param>
/// <returns>The failure.</returns>
template <class T>
[[nodiscard]] std::expected<Response<T>, BlobStorageError> MakeError(std::error_code error, std::string message = {}) {
    return std::unexpected(MakeClientError(error, std::move(message)));
}

/// <summary>
/// Parses a completed response with <paramref name="parser"/>. A failure whose code is one of
/// <paramref name="suppressed"/> (the *IfExists / *IfNotExists operations) becomes success with the real raw
/// response, keeping the suppressed error's details in Response&lt;T&gt;::Error(); <paramref name="absorbed"/> is
/// one more such code, chosen per call.
/// </summary>
/// <typeparam name="TModel">The model the parser produces.</typeparam>
/// <typeparam name="TParser">The parser's callable type.</typeparam>
/// <param name="sent">The transport outcome.</param>
/// <param name="parser">Turns a successful response into the model; an exception it throws becomes an InvalidResponse
/// failure.</param>
/// <param name="suppressed">The error codes that count as success.</param>
/// <param name="absorbed">One more error code that counts as success.</param>
/// <returns>The response with its model, or the failure.</returns>
template <class TModel, class TParser>
[[nodiscard]] std::expected<Response<TModel>, BlobStorageError>
ParseOutcome(HttpResult sent, TParser&& parser, std::span<const BlobStorageErrorCode> suppressed = {},
             std::optional<BlobStorageErrorCode> absorbed = std::nullopt) {
    std::error_code error;
    HttpResponse response;
    if (sent) {
        response = std::move(*sent);
    } else {
        error = sent.error();
    }
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
    if (failure.Error &&
        (std::ranges::contains(suppressed, failure.Error) || (absorbed.has_value() && failure.Error == *absorbed))) {
        failure.Error.clear();
    }
    return ToExpected(failure.Error, std::move(result));
}

/// <summary>Fills in the ETag and Last-Modified of a result from the response headers.</summary>
/// <typeparam name="T">The result type, which has ETag and LastModified members.</typeparam>
/// <param name="result">The result to fill in.</param>
/// <param name="response">The response to read.</param>
/// <exception cref="std::invalid_argument">Thrown when Last-Modified is present but not a valid HTTP-date.</exception>
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

/// <summary>Reads a result that consists of the ETag and Last-Modified response headers.</summary>
/// <typeparam name="T">The result type, which has ETag and LastModified members.</typeparam>
/// <param name="response">The response to read.</param>
/// <returns>The result.</returns>
/// <exception cref="std::invalid_argument">Thrown when Last-Modified is present but not a valid HTTP-date.</exception>
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

/// <summary>
/// Authorization material captured (by value) for the lifetime of a single logical request,
/// including all of its retry attempts. Exactly one of SAS (in URL), SharedKey or TokenCredential, or none;
/// otherwise construction of the client throws std::invalid_argument.
/// </summary>
struct RequestAuth {
    /// <summary>How a request is authorized: None (the SAS token in the URL, or no credential at all), SharedKey
    /// (signed with the connection's signer) or Token (a bearer token from the token credential).</summary>
    enum class Kind : std::uint8_t { None, SharedKey, Token };

    Kind Type = Kind::None;
    std::shared_ptr<const SharedKeySigner> Signer;
    std::shared_ptr<const std::vector<std::string>> TokenScopes;
};

/// <summary><paramref name="tokenCredential"/> selects Kind::Token when no SAS or Shared Key applies.</summary>
/// <param name="connection">The connection whose SAS token, signer and scopes decide.</param>
/// <param name="tokenCredential">Whether a token credential is available.</param>
/// <returns>The authorization to use for a request.</returns>
[[nodiscard]] RequestAuth MakeRequestAuth(const ConnectionState& connection, CredentialPresence tokenCredential);

/// <summary>Whether Azure Storage documents the transport error as transient.</summary>
/// <param name="error">The transport error.</param>
/// <returns>Retriable when it is.</returns>
[[nodiscard]] Retriability RetriabilityOfTransportError(std::error_code error) noexcept;
/// <summary>
/// Whether the transport error was raised after the request may have reached the service (the outcome is unknown), as
/// opposed to failures that happened before anything was sent (resolve, connect).
/// </summary>
/// <param name="error">The transport error.</param>
/// <returns>Ambiguous when the request may have been delivered.</returns>
[[nodiscard]] DeliveryCertainty DeliveryCertaintyOfTransportError(std::error_code error) noexcept;
/// <summary>Whether a response status is one the client repeats the request for.</summary>
/// <param name="status">The HTTP status.</param>
/// <returns>Retriable for 408, 429, 500, 502, 503 and 504.</returns>
[[nodiscard]] Retriability RetriabilityOfStatus(HttpStatusCode status) noexcept;
/// <summary>Whether a failed request is worth repeating, judged by its transport error or else its status.</summary>
/// <param name="error">The transport error, if any.</param>
/// <param name="response">The response, consulted when there is no transport error.</param>
/// <returns>Retriable when it is.</returns>
[[nodiscard]] Retriability RetriabilityOfFailure(std::error_code error, const HttpResponse& response) noexcept;

/// <summary>Server-provided retry delay (x-ms-retry-after-ms, then Retry-After in seconds), if any.</summary>
/// <param name="response">The response to read.</param>
/// <returns>The delay, capped at 24 hours, or nothing when no valid header is present.</returns>
[[nodiscard]] std::optional<std::chrono::milliseconds> ParseRetryAfter(const HttpResponse& response) noexcept;

/// <summary>
/// Called before each attempt of a request is authorized and sent; see RequestPipeline.hpp. This type stands for
/// no such hook.
/// </summary>
struct NoRebuild {};

/// <summary>Defined in RequestPipeline.hpp.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <typeparam name="TRebuild">The type of the hook run before each attempt, or NoRebuild.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="auth">How the request is authorized.</param>
/// <param name="credential">The credential that supplies bearer tokens, if the request uses them.</param>
/// <param name="retry">The client's retry settings.</param>
/// <param name="request">The request to send.</param>
/// <param name="requestOptions">The options this request runs with.</param>
/// <param name="notFoundRetry">Whether 404 and 410 responses are retried.</param>
/// <param name="rebuild">The hook run before each attempt.</param>
/// <param name="replaySafety">Whether the request may be repeated after an ambiguous failure.</param>
/// <returns>The transport outcome of the last attempt.</returns>
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild = NoRebuild>
boost::asio::awaitable<HttpResult>
SendWithRetry(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
              HttpRequest request, AzureRequestOptions requestOptions,
              NotFoundRetry notFoundRetry = NotFoundRetry::Fail, TRebuild rebuild = {},
              ReplaySafety replaySafety = ReplaySafety::Unsafe);

/// <summary>
/// Defined in RequestPipeline.hpp. <paramref name="replaySafety"/> marks a request whose caller absorbs the outcome of
/// a replay (the
/// *IfExists/*IfNotExists operations), so it may be retried even after an ambiguous transport error.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <typeparam name="TRebuild">The type of the hook run before each attempt, or NoRebuild.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="connection">The connection whose settings authorize and retry the request.</param>
/// <param name="credential">The credential that supplies bearer tokens, if the request uses them.</param>
/// <param name="request">The request to send.</param>
/// <param name="requestOptions">The options this request runs with.</param>
/// <param name="rebuild">The hook run before each attempt.</param>
/// <param name="replaySafety">Whether the request may be repeated after an ambiguous failure.</param>
/// <returns>The transport outcome of the last attempt.</returns>
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild = NoRebuild>
boost::asio::awaitable<HttpResult> SendAuthorizedRequest(THttp& httpClient, const ConnectionState& connection,
                                                         std::shared_ptr<TCred> credential, HttpRequest request,
                                                         AzureRequestOptions requestOptions, TRebuild rebuild = {},
                                                         ReplaySafety replaySafety = ReplaySafety::Unsafe);

/// <summary>Authorizes and sends a request to a container with the retry policy of its connection.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <typeparam name="TRebuild">The type of the hook run before each attempt, or NoRebuild.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="target">The container the request is for.</param>
/// <param name="request">The request to send.</param>
/// <param name="requestOptions">The options this request runs with.</param>
/// <param name="rebuild">The hook run before each attempt.</param>
/// <param name="replaySafety">Whether the request may be repeated after an ambiguous failure.</param>
/// <returns>The transport outcome of the last attempt.</returns>
template <HttpClientLike THttp, class TCred, class TRebuild = NoRebuild>
boost::asio::awaitable<HttpResult> SendAuthorizedRequest(THttp& httpClient, const BasicContainerTarget<TCred>& target,
                                                         HttpRequest request, AzureRequestOptions requestOptions,
                                                         TRebuild rebuild = {},
                                                         ReplaySafety replaySafety = ReplaySafety::Unsafe) {
    co_return co_await SendAuthorizedRequest(httpClient, *target.Connection, target.Credential, std::move(request),
                                             std::move(requestOptions), std::move(rebuild), replaySafety);
}

/// <summary>Authorizes and sends a request to a blob with the retry policy of its connection.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <typeparam name="TRebuild">The type of the hook run before each attempt, or NoRebuild.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="target">The blob the request is for.</param>
/// <param name="request">The request to send.</param>
/// <param name="requestOptions">The options this request runs with.</param>
/// <param name="rebuild">The hook run before each attempt.</param>
/// <param name="replaySafety">Whether the request may be repeated after an ambiguous failure.</param>
/// <returns>The transport outcome of the last attempt.</returns>
template <HttpClientLike THttp, class TCred, class TRebuild = NoRebuild>
boost::asio::awaitable<HttpResult> SendAuthorizedRequest(THttp& httpClient, const BasicBlobTarget<TCred>& target,
                                                         HttpRequest request, AzureRequestOptions requestOptions,
                                                         TRebuild rebuild = {},
                                                         ReplaySafety replaySafety = ReplaySafety::Unsafe) {
    co_return co_await SendAuthorizedRequest(httpClient, *target.Connection, target.Credential, std::move(request),
                                             std::move(requestOptions), std::move(rebuild), replaySafety);
}

/// <summary>
/// Maps the single-request download options onto the chunked engine's options (default chunk
/// size and concurrency).
/// </summary>
/// <param name="options">The single-request download options.</param>
/// <returns>The engine's options.</returns>
[[nodiscard]] DownloadToOptions ToDownloadToOptions(DownloadBlobOptions options);

/// <summary>
/// Download engine (defined in BlobDownload.hpp). Each call owns all of its state; only <paramref name="httpClient"/>
/// and the span/path destination (see below) must outlive the operation. The first request is a
/// ranged GET of ChunkSize bytes (a 0-byte blob's 416 falls back to one un-ranged GET); the
/// remaining bytes are fetched in ChunkSize ranges, up to Concurrency at a time, with
/// If-Match: &lt;first ETag&gt; unless the caller supplied If-Match, and written in order. On failure or
/// cancellation the in-flight requests are cancelled and drained before the single completion.
/// MaxSize limits the output, not transport buffering: it is checked once the first response (at most
/// ChunkSize bytes, already buffered) has revealed the blob size. AzureRequestOptions::SetTotalDeadline is one
/// absolute budget for the whole download; each request receives only the time remaining.
/// Cancelling the awaiting coroutine cancels the in-flight requests; the coroutine returns once they have drained.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="options">The blob to download.</param>
/// <param name="operationOptions">The range, conditions and limits of the download.</param>
/// <param name="requestOptions">The options every request starts from.</param>
/// <returns>The content and properties, or the failure.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DownloadBlobResult>, BlobStorageError>>
DownloadBlobAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                  DownloadBlobOptions operationOptions, AzureRequestOptions requestOptions);
/// <summary>
/// Writes straight into <paramref name="destination"/>, which must stay valid until the coroutine completes. A range
/// larger than
/// the buffer fails the download; a shorter blob leaves the tail untouched (see BytesWritten).
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="options">The blob to download.</param>
/// <param name="operationOptions">The range, conditions, chunking and limits of the download.</param>
/// <param name="destination">The buffer to copy into.</param>
/// <param name="requestOptions">The options every request starts from.</param>
/// <returns>The bytes written and properties, or the failure.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>>
DownloadBlobToSpanAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                        DownloadToOptions operationOptions, std::span<char> destination,
                        AzureRequestOptions requestOptions);
/// <summary>Downloads into <c>&lt;path&gt;.partial-&lt;id&gt;</c> and renames it over <paramref name="path"/> only on
/// success.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="options">The blob to download.</param>
/// <param name="operationOptions">The range, conditions, chunking, limits and write scheduler of the download.</param>
/// <param name="path">The file to publish the download to.</param>
/// <param name="requestOptions">The options every request starts from.</param>
/// <returns>The bytes written and properties, or the failure.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>>
DownloadBlobToFileAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                        DownloadToOptions operationOptions, std::filesystem::path path,
                        AzureRequestOptions requestOptions);

/// <summary>
/// Sends <paramref name="request"/> to <paramref name="target"/> and returns the TModel produced by
/// <paramref name="parser"/>.
/// <paramref name="suppressed"/> lists the service error
/// codes that count as success (the *IfExists/*IfNotExists operations); <paramref name="replaySafety"/> allows replay
/// after an
/// ambiguous transport failure.
/// </summary>
/// <typeparam name="TModel">The model the parser produces.</typeparam>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TTarget">The container or blob target type.</typeparam>
/// <typeparam name="TParser">The parser's callable type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="target">The target the request is for.</param>
/// <param name="request">The request to send.</param>
/// <param name="parser">Turns a successful response into the model.</param>
/// <param name="requestOptions">The options this request runs with.</param>
/// <param name="replaySafety">Whether the request may be repeated after an ambiguous failure.</param>
/// <param name="suppressed">The error codes that count as success.</param>
/// <returns>The response with its model, or the failure.</returns>
template <class TModel, HttpClientLike THttp, class TTarget, class TParser>
boost::asio::awaitable<std::expected<Response<TModel>, BlobStorageError>>
SendAndParse(THttp& httpClient, const TTarget& target, HttpRequest request, TParser parser,
             AzureRequestOptions requestOptions, ReplaySafety replaySafety = ReplaySafety::Unsafe,
             std::vector<BlobStorageErrorCode> suppressed = {}) {
    HttpResult sent = co_await SendAuthorizedRequest(httpClient, target, std::move(request), std::move(requestOptions),
                                                     NoRebuild{}, replaySafety);
    co_return ParseOutcome<TModel>(std::move(sent), parser, suppressed);
}

/// <summary>Deletes a blob.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="options">The blob to delete.</param>
/// <param name="operationOptions">The conditions the delete is made under.</param>
/// <param name="requestOptions">The options the request runs with.</param>
/// <param name="replaySafety">Whether the request may be repeated after an ambiguous failure.</param>
/// <param name="suppressed">The error codes that count as success.</param>
/// <returns>The result of the delete, or the failure.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DeleteBlobResult>, BlobStorageError>>
DeleteBlobAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options, DeleteBlobOptions operationOptions,
                AzureRequestOptions requestOptions, ReplaySafety replaySafety = ReplaySafety::Unsafe,
                std::vector<BlobStorageErrorCode> suppressed = {}) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Delete);
    ApplyBlobRequestConditions(request, operationOptions.Conditions);

    co_return co_await SendAndParse<Models::DeleteBlobResult>(httpClient, options, std::move(request),
                                                              ParseDeleteBlobResult, std::move(requestOptions),
                                                              replaySafety, std::move(suppressed));
}

/// <summary>Reads a blob's properties with a HEAD request.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="options">The blob to inspect.</param>
/// <param name="operationOptions">The conditions the request is made under.</param>
/// <param name="requestOptions">The options the request runs with.</param>
/// <returns>The properties, or the failure.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::BlobProperties>, BlobStorageError>>
GetBlobPropertiesAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options,
                       GetBlobPropertiesOptions operationOptions, AzureRequestOptions requestOptions) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Head);
    ApplyBlobRequestConditions(request, operationOptions.Conditions);

    co_return co_await SendAndParse<Models::BlobProperties>(httpClient, options, std::move(request),
                                                            ParseBlobProperties, std::move(requestOptions));
}

/// <summary>Replaces a blob's metadata.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="options">The blob to update.</param>
/// <param name="operationOptions">The new metadata and the conditions of the update.</param>
/// <param name="requestOptions">The options the request runs with.</param>
/// <returns>The new ETag and modification time, or the failure.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::SetBlobMetadataResult>, BlobStorageError>>
SetBlobMetadataAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options, SetBlobMetadataOptions operationOptions,
                     AzureRequestOptions requestOptions) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Put, "comp=metadata");
    ApplyMetadata(request, operationOptions.Metadata);
    ApplyBlobRequestConditions(request, operationOptions.Conditions);

    co_return co_await SendAndParse<Models::SetBlobMetadataResult>(
        httpClient, options, std::move(request), ParseETagAndLastModified<Models::SetBlobMetadataResult>,
        std::move(requestOptions));
}

/// <summary>A failed argument validation as an invalid_argument failure carrying <paramref name="message"/>.</summary>
/// <param name="message">What was wrong with the argument.</param>
/// <returns>The failure.</returns>
[[nodiscard]] inline std::unexpected<BlobStorageError> InvalidArgument(std::string message) {
    return std::unexpected(MakeClientError(std::make_error_code(std::errc::invalid_argument), std::move(message)));
}

/// <summary>The lease id of a lease request comes from the options' LeaseId; Conditions.LeaseId is ignored.</summary>
/// <param name="request">The request to add to.</param>
/// <param name="conditions">The conditions to apply, apart from the lease id.</param>
void ApplyLeaseRequestConditions(HttpRequest& request, Models::BlobRequestConditions conditions);

/// <summary>Lease requests (Lease Blob).</summary>
/// <param name="target">The blob to lease.</param>
/// <param name="action">The lease operation: acquire, renew or release.</param>
/// <returns>The request, without the headers specific to the action.</returns>
[[nodiscard]] HttpRequest BuildLeaseRequest(const BlobTarget& target, std::string_view action);

/// <summary>Acquires a lease on a blob.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TTarget">The blob target type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="target">The blob to lease.</param>
/// <param name="operationOptions">The duration, the proposed lease id and the conditions.</param>
/// <param name="requestOptions">The options the request runs with.</param>
/// <returns>The lease id and blob version, or the failure; a fixed duration outside 15 to 60 seconds fails without a
/// request.</returns>
template <HttpClientLike THttp, class TTarget>
boost::asio::awaitable<std::expected<Response<Models::AcquireBlobLeaseResult>, BlobStorageError>>
AcquireLeaseAsync(THttp& httpClient, const TTarget& target, AcquireLeaseOptions operationOptions,
                  AzureRequestOptions requestOptions) {
    if (operationOptions.Duration.has_value() &&
        (*operationOptions.Duration < MinFixedLeaseDuration || *operationOptions.Duration > MaxFixedLeaseDuration)) {
        co_return InvalidArgument("Lease duration must be between 15 and 60 seconds, or infinite.");
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
    co_return co_await SendAndParse<Models::AcquireBlobLeaseResult>(
        httpClient, target, std::move(request), ParseAcquireBlobLeaseResult, std::move(requestOptions));
}

/// <summary>Renews a lease on a blob.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TTarget">The blob target type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="target">The blob that is leased.</param>
/// <param name="operationOptions">The lease id and the conditions.</param>
/// <param name="requestOptions">The options the request runs with.</param>
/// <returns>The lease id and blob version, or the failure; an empty lease id fails without a request.</returns>
template <HttpClientLike THttp, class TTarget>
boost::asio::awaitable<std::expected<Response<Models::RenewBlobLeaseResult>, BlobStorageError>>
RenewLeaseAsync(THttp& httpClient, const TTarget& target, RenewLeaseOptions operationOptions,
                AzureRequestOptions requestOptions) {
    if (operationOptions.LeaseId.empty()) {
        co_return InvalidArgument("LeaseId must not be empty.");
    }

    HttpRequest request = BuildLeaseRequest(target, "renew");
    AddHeader(request, XMsLeaseIdHeaderName, operationOptions.LeaseId);
    ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
    co_return co_await SendAndParse<Models::RenewBlobLeaseResult>(httpClient, target, std::move(request),
                                                                  ParseRenewBlobLeaseResult, std::move(requestOptions));
}

/// <summary>Releases a lease on a blob.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TTarget">The blob target type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="target">The blob that is leased.</param>
/// <param name="operationOptions">The lease id and the conditions.</param>
/// <param name="requestOptions">The options the request runs with.</param>
/// <returns>The blob version, or the failure; an empty lease id fails without a request.</returns>
template <HttpClientLike THttp, class TTarget>
boost::asio::awaitable<std::expected<Response<Models::ReleaseBlobLeaseResult>, BlobStorageError>>
ReleaseLeaseAsync(THttp& httpClient, const TTarget& target, ReleaseLeaseOptions operationOptions,
                  AzureRequestOptions requestOptions) {
    if (operationOptions.LeaseId.empty()) {
        co_return InvalidArgument("LeaseId must not be empty.");
    }

    HttpRequest request = BuildLeaseRequest(target, "release");
    AddHeader(request, XMsLeaseIdHeaderName, operationOptions.LeaseId);
    ApplyLeaseRequestConditions(request, std::move(operationOptions.Conditions));
    // A release replayed after the first attempt's outcome was lost may find the lease already gone; counting
    // replay after an ambiguous outcome lets that 409 be told apart from a genuine one (including one that follows
    // a plain token-refresh repeat).
    auto replayed = std::make_shared<AttemptKind>(AttemptKind::Initial);
    HttpResult sent = co_await SendAuthorizedRequest(
        httpClient, *target.Connection, target.Credential, std::move(request), std::move(requestOptions),
        [replayed](HttpRequest&, std::string_view, AttemptKind attemptKind) { *replayed = attemptKind; },
        ReplaySafety::Safe);
    std::optional<BlobStorageErrorCode> absorbed;
    if (*replayed == AttemptKind::AfterAmbiguousFailure) {
        absorbed = BlobStorageErrorCode::LeaseNotPresentWithLeaseOperation;
    }
    co_return ParseOutcome<Models::ReleaseBlobLeaseResult>(std::move(sent), ParseReleaseBlobLeaseResult, {}, absorbed);
}

/// <summary>Checks whether a blob exists with a HEAD request; a 404 is an answer, not an error.</summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="httpClient">The HTTP client to send through.</param>
/// <param name="options">The blob to look for.</param>
/// <param name="requestOptions">The options the request runs with.</param>
/// <returns>Whether the blob exists, or the failure.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Existence>, BlobStorageError>>
ExistsBlobAsync(THttp& httpClient, const BasicBlobTarget<TCred>& options, AzureRequestOptions requestOptions) {
    HttpRequest request = BuildBlobRequest(options, HttpMethod::Head);

    HttpResult sent =
        co_await SendAuthorizedRequest(httpClient, options, std::move(request), std::move(requestOptions));
    std::error_code error;
    HttpResponse response;
    if (sent) {
        response = std::move(*sent);
    } else {
        error = sent.error();
    }
    RequestFailure failure = DetermineBlobStorageFailure(error, response);
    Existence exists = Existence::Absent;
    if (!failure.Error) {
        exists = Existence::Present;
    } else if (!error && response.GetStatus() == HttpStatus::NotFound) {
        // A HEAD 404 carries no body, so the error code may be absent; any 404 (including a missing
        // container) means the blob does not exist.
        failure.Error = {};
    }

    co_return ToExpected(failure.Error, Response<Existence>{exists, std::move(response), std::move(failure.Details)});
}
} // namespace AVEVA::AzureClient::Private
