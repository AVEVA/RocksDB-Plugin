// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Instantiates the client templates for the fake transport.

#include "FakeHttpClient.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

namespace AVEVA::AzureClient {
template class BasicBlobClient<Tests::FakeHttpClient>;
template class BasicBlockBlobClient<Tests::FakeHttpClient>;
template class BasicPageBlobClient<Tests::FakeHttpClient>;
template class BasicBlobContainerClient<Tests::FakeHttpClient>;
template class BasicBlobServiceClient<Tests::FakeHttpClient>;
template class BasicBlobClient<Tests::FakeHttpClient, Tests::ScriptedTokenCredential>;
template class BasicBlockBlobClient<Tests::FakeHttpClient, Tests::ScriptedTokenCredential>;
template class BasicPageBlobClient<Tests::FakeHttpClient, Tests::ScriptedTokenCredential>;
template class BasicBlobContainerClient<Tests::FakeHttpClient, Tests::ScriptedTokenCredential>;
template class BasicBlobServiceClient<Tests::FakeHttpClient, Tests::ScriptedTokenCredential>;
template class BasicBlobClient<Tests::FakeHttpClient, StaticTokenCredential>;
template class BasicBlockBlobClient<Tests::FakeHttpClient, StaticTokenCredential>;
template class BasicPageBlobClient<Tests::FakeHttpClient, StaticTokenCredential>;
template class BasicBlobContainerClient<Tests::FakeHttpClient, StaticTokenCredential>;
template class BasicBlobServiceClient<Tests::FakeHttpClient, StaticTokenCredential>;
template class BasicClientSecretCredential<Tests::FakeHttpClient>;
template class BasicWorkloadIdentityCredential<Tests::FakeHttpClient>;
template class BasicManagedIdentityCredential<Tests::FakeHttpClient>;
template class BasicAzurePipelinesCredential<Tests::FakeHttpClient>;
} // namespace AVEVA::AzureClient
