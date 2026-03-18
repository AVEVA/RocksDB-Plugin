// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <azure/identity/chained_token_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/environment_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>

#include <cassert>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    static const std::string g_sizeMetadata = "filesize";
    static void CreateIfNotExistsWithRetry(::Azure::Storage::Blobs::BlobContainerClient& client, int maxRetries = 5)
    {
        int retries = 1;
        while (retries <= maxRetries)
        {
            try
            {
                client.CreateIfNotExists();
                break;
            }
            catch (...)
            {
                if (retries == maxRetries)
                {
                    throw;
                }

                retries++;
                std::this_thread::sleep_for(std::chrono::seconds(retries));
            }
        }
    }

    void BlobHelpers::SetFileSize(const ::Azure::Storage::Blobs::PageBlobClient& client, int64_t size)
    {
        ::Azure::Storage::Metadata metadata;
        metadata.emplace(g_sizeMetadata, std::to_string(size));
        client.SetMetadata(std::move(metadata));
    }

    int64_t BlobHelpers::GetFileSize(const ::Azure::Storage::Blobs::PageBlobClient& client)
    {
        const auto props = client.GetProperties();
        auto metaIter = props.Value.Metadata.find(g_sizeMetadata);
        return metaIter != props.Value.Metadata.end()
            ? static_cast<int64_t>(std::stoll(metaIter->second))
            : 0;
    }

    int64_t BlobHelpers::GetBlobCapacity(const ::Azure::Storage::Blobs::PageBlobClient& client)
    {
        auto props = client.GetProperties();
        assert(props.Value.BlobSize >= 0);
        return static_cast<int64_t>(props.Value.BlobSize);
    }

    std::pair<int64_t, int64_t> BlobHelpers::RoundToEndOfNearestPage(int64_t size)
    {
        auto [partialPageSize, roundedSize] = RoundToBeginningOfNearestPage(size);
        if (partialPageSize != 0)
        {
            roundedSize += Configuration::PageBlob::PageSize;
        }

        return std::make_pair(partialPageSize, roundedSize);
    }

    std::pair<int64_t, int64_t> BlobHelpers::RoundToBeginningOfNearestPage(int64_t size)
    {
        const auto partialPageSize = size % static_cast<int64_t>(Configuration::PageBlob::PageSize);
        auto pages = (size / static_cast<int64_t>(Configuration::PageBlob::PageSize));
        const auto roundedSize = pages * static_cast<int64_t>(Configuration::PageBlob::PageSize);
        return std::make_pair(partialPageSize, roundedSize);
    }

    ::Azure::Storage::Blobs::BlobClientOptions BlobHelpers::CreateBlobClientOptions()
    {
        auto opts = ::Azure::Storage::Blobs::BlobClientOptions();
        opts.Retry.MaxRetries = Configuration::MaxClientRetries;
        return opts;
    }

    ::Azure::Identity::ClientSecretCredentialOptions BlobHelpers::CreateClientSecretCredentialOptions()
    {
        auto opts = ::Azure::Identity::ClientSecretCredentialOptions();
        opts.Retry.MaxRetries = Configuration::MaxClientRetries;
        return opts;
    }

    ::Azure::Identity::AzurePipelinesCredentialOptions BlobHelpers::CreatePipelinesCredentialOptions()
    {
        auto opts = ::Azure::Identity::AzurePipelinesCredentialOptions();
        opts.Retry.MaxRetries = Configuration::MaxClientRetries;
        return opts;
    }

    ::Azure::Storage::Blobs::BlobServiceClient BlobHelpers::CreateServiceClient(const Models::ServicePrincipalStorageInfo& servicePrincipal)
    {
        auto clientSecretOptions = CreateClientSecretCredentialOptions();
        auto cred = std::make_shared<::Azure::Identity::ClientSecretCredential>(servicePrincipal.GetTenantId(),
            servicePrincipal.GetServicePrincipalId(),
            servicePrincipal.GetServicePrincipalSecret(),
            clientSecretOptions);

        auto blobOptions = CreateBlobClientOptions();
        return ::Azure::Storage::Blobs::BlobServiceClient
        {
            servicePrincipal.GetStorageAccountUrl(),
            std::move(cred),
            blobOptions
        };
    }

    ::Azure::Storage::Blobs::BlobServiceClient BlobHelpers::CreateServiceClient(const Models::ChainedCredentialInfo& chainedCredential)
    {
        ::Azure::Identity::ChainedTokenCredential::Sources credSources;
        ::Azure::Identity::ManagedIdentityCredentialOptions managedOptions;
        managedOptions.Retry.MaxRetries = Configuration::MaxClientRetries;
        auto clientSecretOptions = CreateClientSecretCredentialOptions();

        // Try to use user specified credentials to try to authenticate first.
        credSources.push_back(std::make_shared<::Azure::Identity::ClientSecretCredential>(chainedCredential.GetTenantId(),
            chainedCredential.GetServicePrincipalId(),
            chainedCredential.GetServicePrincipalSecret(),
            clientSecretOptions));
        if (chainedCredential.GetManagedIdentityId())
        {
            managedOptions.IdentityId.FromUserAssignedClientId(std::string(*chainedCredential.GetManagedIdentityId()));
            credSources.push_back(std::make_shared<::Azure::Identity::ManagedIdentityCredential>(clientSecretOptions));
        }

        // EnvironmentCredential is a ClientSecret Credential but constructed through environment variable values. 
        credSources.push_back(std::make_shared<::Azure::Identity::EnvironmentCredential>(clientSecretOptions));

        // Workload credential constructed from environment variables
        credSources.push_back(std::make_shared<::Azure::Identity::WorkloadIdentityCredential>(clientSecretOptions));

        auto cred = std::make_shared<::Azure::Identity::ChainedTokenCredential>(credSources);

        auto blobOptions = CreateBlobClientOptions();
        return ::Azure::Storage::Blobs::BlobServiceClient
        {
            chainedCredential.GetStorageAccountUrl(),
            std::move(cred),
            blobOptions
        };
    }

    ::Azure::Storage::Blobs::BlobContainerClient BlobHelpers::GetContainerClient(::Azure::Storage::Blobs::BlobServiceClient& blobServiceClient, const std::string& name)
    {
        auto blobContainerClient = blobServiceClient.GetBlobContainerClient(name);
        CreateIfNotExistsWithRetry(blobContainerClient);
        return blobContainerClient;
    }
}
