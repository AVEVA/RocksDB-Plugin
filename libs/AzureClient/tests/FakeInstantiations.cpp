// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Instantiates the client templates for the fake transport; the definitions live in private headers.

#include "FakeHttpClient.hpp"

#include "../src/BlobClientImpl.hpp"
#include "../src/BlobContainerClientImpl.hpp"
#include "../src/BlobServiceClientImpl.hpp"
#include "../src/BlockBlobClientImpl.hpp"
#include "../src/CredentialsImpl.hpp"
#include "../src/PageBlobClientImpl.hpp"

namespace AVEVA::AzureClient {
template class BasicBlobClient<Tests::FakeHttpClient>;
template class BasicBlockBlobClient<Tests::FakeHttpClient>;
template class BasicPageBlobClient<Tests::FakeHttpClient>;
template class BasicBlobContainerClient<Tests::FakeHttpClient>;
template class BasicBlobServiceClient<Tests::FakeHttpClient>;
template class BasicClientSecretCredential<Tests::FakeHttpClient>;
template class BasicWorkloadIdentityCredential<Tests::FakeHttpClient>;
template class BasicManagedIdentityCredential<Tests::FakeHttpClient>;
} // namespace AVEVA::AzureClient
