#pragma once

#include <AVEVA/AzureClient/Models/BlobModels.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace AVEVA::AzureClient::Models
{
    enum class PublicAccessType : std::uint8_t
    {
        Unknown,
        None,
        BlobContainer,
        Blob
    };

    struct CreateBlobContainerResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct DeleteBlobContainerResult
    {
    };

    struct BlobContainerProperties
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
        MetadataMap Metadata;
        PublicAccessType AccessType = PublicAccessType::Unknown;
        bool HasImmutabilityPolicy = false;
        bool HasLegalHold = false;
        LeaseStatus Status = LeaseStatus::Unknown;
        LeaseState State = LeaseState::Unknown;
        LeaseDurationType DurationType = LeaseDurationType::Unknown;
        std::string DefaultEncryptionScope;
        bool PreventEncryptionScopeOverride = false;
    };

    struct BlobContainerItem
    {
        std::string Name;
        BlobContainerProperties Properties;
    };

    struct ListBlobContainersResult
    {
        std::vector<BlobContainerItem> Containers;
        std::string Prefix;
        std::string Marker;
        std::string NextMarker;
    };
} // namespace AVEVA::AzureClient::Models
