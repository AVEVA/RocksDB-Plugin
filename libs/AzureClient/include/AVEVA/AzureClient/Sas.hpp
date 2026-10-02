#pragma once

#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/Models/BlobServiceModels.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace AVEVA::AzureClient::Sas
{
    // The signed version (sv) of every SAS produced here.
    inline constexpr std::string_view SasVersion = "2023-11-03";

    enum class SasProtocol : std::uint8_t
    {
        HttpsOnly,
        HttpsAndHttp
    };

    // Service SAS for a container (BlobName empty), blob, blob snapshot or blob version. The returned query
    // string has no leading '?' and can be used directly as BlobClientOptions::SasToken.
    //
    // Permissions must be given in the service's canonical order ("racwdxyltmeopi" subset, e.g. "rcw").
    // ExpiresOn and Permissions may be omitted only when Identifier names a stored access policy that
    // supplies them. ToSasQueryParameters throws std::invalid_argument for missing or conflicting fields.
    struct BlobSasBuilder
    {
        std::string ContainerName;
        std::string BlobName;
        // Signs for a snapshot (sr=bs) or version (sr=bv); the request URL must carry the same value.
        std::string Snapshot;
        std::string BlobVersionId;
        std::string Permissions;
        std::optional<std::chrono::system_clock::time_point> StartsOn;
        std::optional<std::chrono::system_clock::time_point> ExpiresOn;
        std::string Identifier; // stored access policy (Shared Key SAS only)
        std::string IPRange;    // "a.b.c.d" or "a.b.c.d-e.f.g.h"
        SasProtocol Protocol = SasProtocol::HttpsOnly;
        std::string EncryptionScope;
        // Response header overrides (rscc/rscd/rsce/rscl/rsct).
        std::string CacheControl;
        std::string ContentDisposition;
        std::string ContentEncoding;
        std::string ContentLanguage;
        std::string ContentType;

        // Signs with the account key.
        [[nodiscard]] std::string ToSasQueryParameters(const SharedKeyCredentialOptions& credential) const;
        // Signs a user delegation SAS with a key from BlobServiceClient::GetUserDelegationKeyAsync.
        [[nodiscard]] std::string ToSasQueryParameters(const Models::UserDelegationKey& key,
            std::string_view accountName) const;
    };

    // Time semantics (both builders): StartsOn / ExpiresOn are serialized in UTC as "YYYY-MM-DDTHH:MM:SSZ"
    // with whole-second precision (sub-second parts are floored). An expiry already in the past is not
    // rejected; the service will simply refuse the resulting token.
    //
    // Account SAS. Services is a subset of "bfqt" and ResourceTypes a subset of "sco"; Permissions uses the
    // canonical order "rwdxylacuptfi". ToSasQueryParameters requires a non-empty account name in the
    // credential and non-empty Services, ResourceTypes and Permissions plus a set ExpiresOn (a
    // value-initialized time_point counts as unset); otherwise it throws std::invalid_argument.
    struct AccountSasBuilder
    {
        std::string Services = "b";
        std::string ResourceTypes;
        std::string Permissions;
        std::optional<std::chrono::system_clock::time_point> StartsOn;
        std::chrono::system_clock::time_point ExpiresOn;
        std::string IPRange;
        SasProtocol Protocol = SasProtocol::HttpsOnly;
        std::string EncryptionScope;

        [[nodiscard]] std::string ToSasQueryParameters(const SharedKeyCredentialOptions& credential) const;
    };
} // namespace AVEVA::AzureClient::Sas
