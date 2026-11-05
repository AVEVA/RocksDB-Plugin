// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>

namespace AVEVA::AzureClient {
/// <summary>
/// Forward declaration of the client for operations common to every blob type.
/// </summary>
/// <remarks>
/// Every client is templated on its HTTP transport and on the token credential it authorizes with. Clients
/// that use SAS, Shared Key or a static bearer token keep the default NoTokenCredential. The defaults live
/// here, in the only first declaration.
/// </remarks>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicBlobClient;

/// <summary>
/// Forward declaration of the client for page blob operations.
/// </summary>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicPageBlobClient;

/// <summary>
/// Forward declaration of the client for container operations.
/// </summary>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicBlobContainerClient;

/// <summary>
/// Forward declaration of the client for account-level operations.
/// </summary>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicBlobServiceClient;

namespace Private {
/// <summary>
/// Forward declaration of the immutable description of the blob a client operates on.
/// </summary>
/// <typeparam name="TCred">The token credential the target authorizes with.</typeparam>
template <class TCred> struct BasicBlobTarget;

/// <summary>
/// Forward declaration of the immutable description of the container a client operates on.
/// </summary>
/// <typeparam name="TCred">The token credential the target authorizes with.</typeparam>
template <class TCred> struct BasicContainerTarget;
} // namespace Private
} // namespace AVEVA::AzureClient
