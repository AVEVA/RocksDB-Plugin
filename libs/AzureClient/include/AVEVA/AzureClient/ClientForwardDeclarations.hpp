// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>

namespace AVEVA::AzureClient {
// Every client is templated on its HTTP transport and on the token credential it authorizes with. Clients
// that use SAS, Shared Key or a static bearer token keep the default NoTokenCredential. The defaults live
// here, in the only first declaration.
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicBlobClient;
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicBlockBlobClient;
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicPageBlobClient;
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicBlobContainerClient;
template <HttpClientLike THttp, TokenCredentialLike TCred = NoTokenCredential> class BasicBlobServiceClient;

namespace Private {
template <class TCred> struct BasicBlobTarget;
template <class TCred> struct BasicContainerTarget;
} // namespace Private
} // namespace AVEVA::AzureClient
