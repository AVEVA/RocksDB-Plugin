// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Explicit instantiations matching the extern template declarations in the client headers.

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

namespace AVEVA::AzureClient {
template class BasicBlobClient<HttpClient>;
template class BasicPageBlobClient<HttpClient>;
template class BasicBlobContainerClient<HttpClient>;
template class BasicBlobServiceClient<HttpClient>;
} // namespace AVEVA::AzureClient
