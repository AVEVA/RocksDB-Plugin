// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace {
using AVEVA::AzureClient::ClientSecretCredentialOptions;
using AVEVA::AzureClient::ManagedIdentityCredentialOptions;
using AVEVA::AzureClient::Tests::BlobContainerClient;
using AVEVA::AzureClient::Tests::BlobServiceClient;
using AVEVA::AzureClient::Tests::ClientSecretCredential;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;
using AVEVA::AzureClient::Tests::ManagedIdentityCredential;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RunAwaitable;

// A transport nobody but the clients under test refers to, so `Expired` tells whether any of them still holds it.
struct SharedTransport {
    std::shared_ptr<FakeHttpClient> Http = std::make_shared<FakeHttpClient>();
    std::weak_ptr<FakeHttpClient> Weak = Http;

    [[nodiscard]] std::shared_ptr<FakeHttpClient> Release() { return std::exchange(Http, nullptr); }
    [[nodiscard]] bool Expired() const { return Weak.expired(); }
};
} // namespace

TEST(SharedHttpOwnershipTests, BlobClientKeepsTheTransportAliveUntilItIsDestroyed) {
    SharedTransport transport;
    std::optional<PageBlobClient> client;
    client.emplace(transport.Release(), MakeBlobClientOptions());

    EXPECT_FALSE(transport.Expired());
    (void)RunAwaitable(*transport.Weak.lock(), client->DeleteIfExistsAsync());
    EXPECT_EQ(1U, transport.Weak.lock()->RequestCount());

    client.reset();
    EXPECT_TRUE(transport.Expired());
}

TEST(SharedHttpOwnershipTests, BlobClientsFromAContainerOutliveTheContainer) {
    SharedTransport transport;
    std::optional<BlobContainerClient> container;
    container.emplace(transport.Release(), MakeBlobContainerClientOptions());
    auto blob = container->GetPageBlobClient("a.sst");

    container.reset();
    EXPECT_FALSE(transport.Expired());

    (void)RunAwaitable(*transport.Weak.lock(), blob.DeleteIfExistsAsync());
    EXPECT_EQ(1U, transport.Weak.lock()->RequestCount());
}

TEST(SharedHttpOwnershipTests, DroppingEveryClientReleasesTheTransport) {
    SharedTransport transport;
    {
        BlobContainerClient container{transport.Release(), MakeBlobContainerClientOptions()};
        const auto page = container.GetPageBlobClient("b.sst");
        EXPECT_FALSE(transport.Expired());
    }
    EXPECT_TRUE(transport.Expired());
}

TEST(SharedHttpOwnershipTests, ServiceClientSharesTheTransportWithItsContainers) {
    SharedTransport transport;
    std::optional<BlobServiceClient> service;
    service.emplace(transport.Release(), MakeBlobServiceClientOptions());
    std::optional<BlobContainerClient> container{service->GetBlobContainerClient("files")};

    service.reset();
    EXPECT_FALSE(transport.Expired());

    container.reset();
    EXPECT_TRUE(transport.Expired());
}

TEST(SharedHttpOwnershipTests, CredentialsKeepTheTransportAlive) {
    SharedTransport secret;
    std::optional<ClientSecretCredential> secretCredential;
    secretCredential.emplace(secret.Release(), ClientSecretCredentialOptions{});
    EXPECT_FALSE(secret.Expired());
    secretCredential.reset();
    EXPECT_TRUE(secret.Expired());

    SharedTransport managed;
    std::optional<ManagedIdentityCredential> managedCredential;
    managedCredential.emplace(managed.Release(), ManagedIdentityCredentialOptions{});
    EXPECT_FALSE(managed.Expired());
    managedCredential.reset();
    EXPECT_TRUE(managed.Expired());
}

TEST(SharedHttpOwnershipTests, ReferenceConstructorsDoNotTakeOwnership) {
    const auto http = std::make_shared<FakeHttpClient>();
    {
        BlobContainerClient container{*http, MakeBlobContainerClientOptions()};
        const auto blob = container.GetPageBlobClient("a.sst");
        EXPECT_EQ(1, http.use_count());
    }
    EXPECT_EQ(1, http.use_count());
}

TEST(SharedHttpOwnershipTests, NullTransportIsRejected) {
    const std::shared_ptr<FakeHttpClient> none;

    EXPECT_THROW(PageBlobClient(none, MakeBlobClientOptions()), std::invalid_argument);
    EXPECT_THROW(BlobContainerClient(none, MakeBlobContainerClientOptions()), std::invalid_argument);
    EXPECT_THROW(BlobServiceClient(none, MakeBlobServiceClientOptions()), std::invalid_argument);
    EXPECT_THROW(ClientSecretCredential(none, ClientSecretCredentialOptions{}), std::invalid_argument);
}
