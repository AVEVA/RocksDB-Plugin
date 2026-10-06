#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace AVEVA::AzureClient::Models
{
    // Get User Delegation Key; pass to BlobSasBuilder::ToSasQueryParameters to sign a user delegation SAS.
    struct UserDelegationKey
    {
        std::string SignedObjectId;
        std::string SignedTenantId;
        std::chrono::system_clock::time_point SignedStartsOn;
        std::chrono::system_clock::time_point SignedExpiresOn;
        std::string SignedService;
        std::string SignedVersion;
        std::string Value; // base64 key
    };
} // namespace AVEVA::AzureClient::Models
