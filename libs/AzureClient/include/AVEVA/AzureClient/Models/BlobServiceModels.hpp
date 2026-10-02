#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace AVEVA::AzureClient::Models
{
    // Get Account Information.
    struct AccountInfo
    {
        std::string SkuName;     // e.g. "Standard_LRS"
        std::string AccountKind; // e.g. "StorageV2"
        bool IsHierarchicalNamespaceEnabled = false;
    };

    struct RetentionPolicy
    {
        bool Enabled = false;
        std::optional<std::int32_t> Days;
    };

    struct AnalyticsLogging
    {
        std::string Version;
        bool Delete = false;
        bool Read = false;
        bool Write = false;
        Models::RetentionPolicy RetentionPolicy;
    };

    struct Metrics
    {
        std::string Version;
        bool Enabled = false;
        std::optional<bool> IncludeApis;
        Models::RetentionPolicy RetentionPolicy;
    };

    struct CorsRule
    {
        std::string AllowedOrigins;
        std::string AllowedMethods;
        std::string AllowedHeaders;
        std::string ExposedHeaders;
        std::int32_t MaxAgeInSeconds = 0;
    };

    struct StaticWebsite
    {
        bool Enabled = false;
        std::string IndexDocument;
        std::string ErrorDocument404Path;
        std::string DefaultIndexDocumentPath;
    };

    // Get Blob Service Properties.
    struct BlobServiceProperties
    {
        AnalyticsLogging Logging;
        Metrics HourMetrics;
        Metrics MinuteMetrics;
        std::vector<CorsRule> Cors;
        std::string DefaultServiceVersion;
        Models::RetentionPolicy DeleteRetentionPolicy;
        Models::StaticWebsite StaticWebsite;
    };

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
