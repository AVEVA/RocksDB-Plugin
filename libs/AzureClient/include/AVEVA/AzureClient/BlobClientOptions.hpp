// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/Outcomes.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace AVEVA::AzureClient {
/// <summary>
/// The Blob service REST API version sent with requests when none is configured.
/// </summary>
inline constexpr std::string_view DefaultApiVersion = "2023-11-03";

/// <summary>
/// Storage account name and key for Shared Key authorization.
/// </summary>
struct SharedKeyCredentialOptions {
    /// <summary>
    /// The storage account name.
    /// </summary>
    std::string AccountName;

    /// <summary>
    /// Base64 account key. The client decodes it once into an HMAC context and wipes the decoded bytes, but this
    /// caller-owned string (and any copy of the options the caller keeps) is not cleared; clear or destroy it
    /// after constructing the client if that matters.
    /// </summary>
    std::string AccountKey;
};

/// <summary>
/// The default delay before the first retry; later retries back off exponentially from it. See
/// <see cref="RetryOptions"/>.
/// </summary>
inline constexpr std::chrono::milliseconds DefaultRetryInitialDelay{800};

/// <summary>
/// The default cap on the exponential backoff delay. See <see cref="RetryOptions"/>.
/// </summary>
inline constexpr std::chrono::milliseconds DefaultRetryMaxDelay{std::chrono::minutes{1}};

/// <summary>
/// Automatic retry of transient failures (HTTP 408/429/500/502/503/504 and transport errors such as
/// resolve/connect/read/write failures and timeouts). Each retry re-signs the request with a fresh
/// x-ms-date (same x-ms-client-request-id) after an exponential backoff with jitter
/// (<c>InitialDelay * 2^n</c>, capped at <c>MaxDelay</c>); a server Retry-After / x-ms-retry-after-ms hint takes
/// precedence and is honoured up to the larger of <c>MaxDelay</c> and one minute (a bogus hint cannot stall a
/// request for hours). Cancellation stops retrying immediately.
/// </summary>
struct RetryOptions {
    /// <summary>
    /// Number of retry attempts after the initial attempt (0 disables retries).
    /// </summary>
    int MaxRetries = 3;

    /// <summary>
    /// The backoff before the first retry, doubled for each further one.
    /// </summary>
    std::chrono::milliseconds InitialDelay = DefaultRetryInitialDelay;

    /// <summary>
    /// The upper bound of the exponential backoff delay.
    /// </summary>
    std::chrono::milliseconds MaxDelay = DefaultRetryMaxDelay;
};

/// <summary>
/// The OAuth scope requested for storage tokens when none is configured.
/// </summary>
inline constexpr std::string_view DefaultStorageScope = "https://storage.azure.com/.default";

/// <summary>
/// Connection settings shared by the service, container and blob client options.
/// </summary>
struct ConnectionOptions {
    /// <summary>
    /// The base URL of the storage account's Blob service.
    /// </summary>
    std::string ServiceEndpoint;

    /// <summary>
    /// Already URL-encoded <c>key=value</c> pairs joined by '&amp;'; one leading '?' is tolerated and stripped.
    /// </summary>
    std::string SasToken;

    /// <summary>
    /// The OAuth scopes requested when authorizing with a TokenCredential.
    /// Supply exactly one of <c>SasToken</c>, a shared key or a TokenCredential, or none (anonymous); supplying more
    /// than one makes construction throw <c>std::invalid_argument</c>.
    /// TokenCredential requires an https <c>ServiceEndpoint</c> (an http endpoint is rejected with
    /// <c>std::invalid_argument</c>) unless the host is loopback (localhost, 127.0.0.1, [::1]).
    /// </summary>
    std::vector<std::string> TokenScopes{std::string(DefaultStorageScope)};

    /// <summary>
    /// The account name and key used for Shared Key authorization.
    /// </summary>
    SharedKeyCredentialOptions SharedKey;

    /// <summary>
    /// The retry policy for transient failures.
    /// </summary>
    RetryOptions Retry;

    /// <summary>
    /// The request options the client starts from for its operations.
    /// </summary>
    AzureRequestOptions DefaultRequestOptions;

    /// <summary>
    /// The Blob service REST API version sent with requests.
    /// </summary>
    std::string ApiVersion = std::string(DefaultApiVersion);

    /// <summary>
    /// http endpoints are rejected with <c>std::invalid_argument</c> unless this is <c>Allow</c> (intended for
    /// Azurite and other local emulators). Credentials additionally still require a loopback host over http.
    /// </summary>
    InsecureHttpPolicy InsecureHttp = InsecureHttpPolicy::Reject;
};

/// <summary>
/// Identifies a storage account; carries only the connection settings.
/// </summary>
struct BlobServiceClientOptions : ConnectionOptions {};

/// <summary>
/// Identifies a container within an account.
/// </summary>
struct BlobContainerClientOptions : ConnectionOptions {
    /// <summary>
    /// The name of the container.
    /// </summary>
    std::string ContainerName;
};

/// <summary>
/// Identifies a single blob (block blob or page blob) within a container.
/// </summary>
struct BlobClientOptions : ConnectionOptions {
    /// <summary>
    /// The name of the container that holds the blob.
    /// </summary>
    std::string ContainerName;

    /// <summary>
    /// The name of the blob within the container.
    /// </summary>
    std::string BlobName;
};
} // namespace AVEVA::AzureClient
