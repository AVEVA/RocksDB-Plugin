#pragma once
#include <AVEVA/AzureClient/ITokenCredential.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace AVEVA::AzureClient
{
    inline constexpr std::string_view DefaultApiVersion = "2023-11-03";

    struct SharedKeyCredentialOptions
    {
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
    // precedence and is honoured up to 24 hours, even above MaxDelay (which caps only the computed backoff).
    // Cancellation stops retrying immediately. Note that a retried Append Block without an append-position condition
    // may append twice if the first attempt reached the service. Default retry backoff bounds.
    inline constexpr std::chrono::milliseconds DefaultRetryInitialDelay{800};
    inline constexpr std::chrono::milliseconds DefaultRetryMaxDelay{std::chrono::minutes{1}};

    struct RetryOptions
    {
        // Number of retry attempts after the initial attempt (0 disables retries).
        int MaxRetries = 3;
        std::chrono::milliseconds InitialDelay = DefaultRetryInitialDelay;
        std::chrono::milliseconds MaxDelay = DefaultRetryMaxDelay;
    };

    // Identifies a single blob (block blob, page blob, or append blob) within a container, and
    // carries the same connection settings as BlobContainerClientOptions.
    struct BlobClientOptions
    {
        std::string ServiceEndpoint;
        std::string ContainerName;
        std::string BlobName;
        std::string SasToken;

        // Authorization precedence is SAS > SharedKey > TokenCredential > BearerToken.
        // BearerToken/TokenCredential require an https ServiceEndpoint (an http endpoint is rejected with
        // std::invalid_argument) unless the host is loopback (localhost, 127.0.0.1, [::1]).
        std::string BearerToken;
        std::shared_ptr<ITokenCredential> TokenCredential;
        std::vector<std::string> TokenScopes{"https://storage.azure.com/.default"};
        SharedKeyCredentialOptions SharedKey;

        RetryOptions Retry;
        HttpRequestOptions DefaultRequestOptions;

        std::string ApiVersion = std::string(DefaultApiVersion);
        std::string Snapshot;
        // Targets a blob version (?versionid=); mutually exclusive with Snapshot.
        std::string VersionId;
    };
} // namespace AVEVA::AzureClient
