#pragma once
// #include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"

#include <azure/storage/blobs/page_blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/azure_pipelines_credential.hpp>

#include <cstdint>
#include <string>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    struct BlobHelpers
    {
        static void SetFileSize(const ::Azure::Storage::Blobs::PageBlobClient& client, int64_t size);
        static int64_t GetFileSize(const ::Azure::Storage::Blobs::PageBlobClient& client);
        static int64_t GetBlobCapacity(const ::Azure::Storage::Blobs::PageBlobClient& client);
        static std::pair<int64_t, int64_t> RoundToEndOfNearestPage(int64_t size);
        static std::pair<int64_t, int64_t> RoundToBeginningOfNearestPage(int64_t size);
        static ::Azure::Storage::Blobs::BlobClientOptions CreateBlobClientOptions();
        static ::Azure::Identity::ClientSecretCredentialOptions CreateClientSecretCredentialOptions();
        static ::Azure::Identity::AzurePipelinesCredentialOptions CreatePipelinesCredentialOptions();
        static ::Azure::Storage::Blobs::BlobServiceClient CreateServiceClient(const Models::ServicePrincipalStorageInfo& servicePrincipal);
        static ::Azure::Storage::Blobs::BlobServiceClient CreateServiceClient(const Models::ChainedCredentialInfo& servicePrincipal);
        static ::Azure::Storage::Blobs::BlobContainerClient GetContainerClient(::Azure::Storage::Blobs::BlobServiceClient& blobServiceClient, const std::string& name);
    };
}
