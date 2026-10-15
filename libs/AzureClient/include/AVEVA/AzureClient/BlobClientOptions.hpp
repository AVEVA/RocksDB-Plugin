// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace AVEVA::AzureClient {
inline constexpr std::string_view DefaultApiVersion = "2023-11-03";

struct SharedKeyCredentialOptions {
    std::string AccountName;
    // Base64 account key. The client decodes it once into an HMAC context and wipes the decoded bytes, but this
    // caller-owned string (and any copy of the options the caller keeps) is not cleared; clear or destroy it
    // after constructing the client if that matters.
    std::string AccountKey;
};

// Automatic retry of transient failures (HTTP 408/429/500/502/503/504 and transport errors such as
// resolve/connect/read/write failures and timeouts). Each retry re-signs the request with a fresh
// x-ms-date (same x-ms-client-request-id) after an exponential backoff with jitter
// (InitialDelay * 2^n, capped at MaxDelay); a server Retry-After / x-ms-retry-after-ms hint takes
// precedence and is honoured up to the larger of MaxDelay and one minute (a bogus hint cannot stall a request
// for hours). Cancellation stops retrying immediately. Default retry backoff bounds.
inline constexpr std::chrono::milliseconds DefaultRetryInitialDelay{800};
inline constexpr std::chrono::milliseconds DefaultRetryMaxDelay{std::chrono::minutes{1}};

struct RetryOptions {
    // Number of retry attempts after the initial attempt (0 disables retries).
    int MaxRetries = 3;
    std::chrono::milliseconds InitialDelay = DefaultRetryInitialDelay;
    std::chrono::milliseconds MaxDelay = DefaultRetryMaxDelay;
};

// Identifies a single blob (block blob or page blob) within a container, and
// carries the same connection settings as BlobContainerClientOptions.
struct BlobClientOptions {
    std::string ServiceEndpoint;
    std::string ContainerName;
    std::string BlobName;
    // Already URL-encoded `key=value` pairs joined by '&'; one leading '?' is tolerated and stripped.
    std::string SasToken;

    // Supply exactly one of SasToken, a shared key or a TokenCredential, or none (anonymous); supplying more than one
    // makes construction throw std::invalid_argument.
    // TokenCredential requires an https ServiceEndpoint (an http endpoint is rejected with
    // std::invalid_argument) unless the host is loopback (localhost, 127.0.0.1, [::1]).
    std::vector<std::string> TokenScopes{"https://storage.azure.com/.default"};
    SharedKeyCredentialOptions SharedKey;

    RetryOptions Retry;
    AzureRequestOptions DefaultRequestOptions;

    std::string ApiVersion = std::string(DefaultApiVersion);

    // http endpoints are rejected with std::invalid_argument unless this is set (intended for Azurite and
    // other local emulators). Credentials additionally still require a loopback host over http.
    bool AllowInsecureHttp = false;
};

struct BlobContainerClientOptions {
    std::string ServiceEndpoint;
    std::string ContainerName;
    // Exactly one of SasToken, a shared key or a TokenCredential, or none; otherwise construction throws
    // std::invalid_argument (see BlobClientOptions).
    // TokenCredential requires https unless the host is loopback (see BlobClientOptions).
    std::string SasToken;
    std::vector<std::string> TokenScopes{"https://storage.azure.com/.default"};
    SharedKeyCredentialOptions SharedKey;
    std::string ApiVersion = std::string(DefaultApiVersion);

    RetryOptions Retry;
    AzureRequestOptions DefaultRequestOptions;

    // See BlobClientOptions::AllowInsecureHttp.
    bool AllowInsecureHttp = false;
};

struct BlobServiceClientOptions {
    std::string ServiceEndpoint;
    // Exactly one of SasToken, a shared key or a TokenCredential, or none; otherwise construction throws
    // std::invalid_argument (see BlobClientOptions).
    // TokenCredential requires https unless the host is loopback (see BlobClientOptions).
    std::string SasToken;
    std::vector<std::string> TokenScopes{"https://storage.azure.com/.default"};
    SharedKeyCredentialOptions SharedKey;
    std::string ApiVersion = std::string(DefaultApiVersion);

    RetryOptions Retry;
    AzureRequestOptions DefaultRequestOptions;

    // See BlobClientOptions::AllowInsecureHttp.
    bool AllowInsecureHttp = false;
};
} // namespace AVEVA::AzureClient
