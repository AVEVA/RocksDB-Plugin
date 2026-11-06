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
