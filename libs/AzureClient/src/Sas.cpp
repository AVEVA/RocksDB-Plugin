#include <AVEVA/AzureClient/Sas.hpp>

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobServiceModels.hpp"
#include "BlobRequestHelpers.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Sas
{
    namespace
    {
        using Clock = std::chrono::system_clock;

        [[nodiscard]] std::string FormatTime(const std::optional<Clock::time_point>& value)
        {
            return value.has_value() ? Private::FormatIso8601Utc(*value) : std::string{};
        }

        [[nodiscard]] std::string_view ProtocolValue(SasProtocol protocol) noexcept
        {
            return protocol == SasProtocol::HttpsOnly ? "https" : "https,http";
        }

        // Tags giving the query-builder's key and the SAS expiry their own types so they're never
        // adjacent-and-same-type with another std::string_view parameter.
        struct QueryKeyTag
        {
        };

        using QueryKey = Private::StringLabel<QueryKeyTag>;

        struct SasExpiryTag
        {
        };

        using SasExpiry = Private::StringLabel<SasExpiryTag>;

        class QueryBuilder
        {
          public:
            void Add(QueryKey name, std::string_view value)
            {
                if (value.empty())
                {
                    return;
                }
                m_query += m_query.empty() ? "" : "&";
                m_query += name.Value;
                m_query += '=';
                m_query += Private::UrlEncode(value, {});
            }

            [[nodiscard]] std::string Take() &&
            {
                return std::move(m_query);
            }

          private:
            std::string m_query;
        };

        [[nodiscard]] std::string JoinLines(const std::vector<std::string_view>& lines)
        {
            std::string joined;
            bool firstLine = true;
            for (const std::string_view line : lines)
            {
                if (!firstLine)
                {
                    joined += '\n';
                }
                firstLine = false;
                joined += line;
            }
            return joined;
        }

        struct BlobResource
        {
            std::string_view Resource;     // sr
            std::string_view SnapshotTime; // signed snapshot time / version id
            std::string Canonical;         // /blob/{account}/{container}[/{blob}]
        };

        // Azure signs the decoded names, while the request URL percent-encodes them, so the signed
        // form stays raw; only names that could address a different resource are rejected.
        [[nodiscard]] bool HasControlCharacter(std::string_view value) noexcept
        {
            return std::ranges::any_of(value,
                [](char c)
            {
                return static_cast<unsigned char>(c) < 0x20U || c == 0x7F;
            });
        }

        [[nodiscard]] bool HasDotSegment(std::string_view name) noexcept
        {
            while (true)
            {
                const std::size_t slash = name.find('/');
                const std::string_view segment = name.substr(0, slash);
                if (segment.empty() || segment == "." || segment == "..")
                {
                    return true;
                }
                if (slash == std::string_view::npos)
                {
                    return false;
                }
                name.remove_prefix(slash + 1);
            }
        }

        [[nodiscard]] std::string CanonicalizeSasResource(std::string_view accountName,
            std::string_view containerName,
            std::string_view blobName)
        {
            if (accountName.empty() || accountName.find('/') != std::string_view::npos ||
                HasControlCharacter(accountName))
            {
                throw std::invalid_argument("The account name is required to sign a SAS.");
            }
            if (containerName.find('/') != std::string_view::npos || containerName == "." || containerName == ".." ||
                HasControlCharacter(containerName))
            {
                throw std::invalid_argument("BlobSasBuilder::ContainerName is not a valid container name.");
            }
            std::string canonical = "/blob/" + std::string{accountName} + "/" + std::string{containerName};
            if (!blobName.empty())
            {
                if (HasDotSegment(blobName) || HasControlCharacter(blobName))
                {
                    throw std::invalid_argument("BlobSasBuilder::BlobName is not a valid blob name.");
                }
                canonical += "/";
                canonical += blobName;
            }
            return canonical;
        }

        [[nodiscard]] BlobResource ResolveResource(const BlobSasBuilder& builder, std::string_view accountName)
        {
            if (builder.ContainerName.empty())
            {
                throw std::invalid_argument("BlobSasBuilder::ContainerName is required.");
            }
            if (accountName.empty())
            {
                throw std::invalid_argument("The account name is required to sign a SAS.");
            }
            if (!builder.Snapshot.empty() && !builder.BlobVersionId.empty())
            {
                throw std::invalid_argument("BlobSasBuilder::Snapshot and BlobVersionId are mutually exclusive.");
            }
            BlobResource resource;
            resource.Canonical = CanonicalizeSasResource(accountName, builder.ContainerName, builder.BlobName);
            if (builder.BlobName.empty())
            {
                if (!builder.Snapshot.empty() || !builder.BlobVersionId.empty())
                {
                    throw std::invalid_argument("A snapshot or version SAS requires BlobSasBuilder::BlobName.");
                }
                resource.Resource = "c";
                return resource;
            }
            if (!builder.Snapshot.empty())
            {
                resource.Resource = "bs";
                resource.SnapshotTime = builder.Snapshot;
            }
            else if (!builder.BlobVersionId.empty())
            {
                resource.Resource = "bv";
                resource.SnapshotTime = builder.BlobVersionId;
            }
            else
            {
                resource.Resource = "b";
            }
            return resource;
        }

        void AddCommonBlobQuery(QueryBuilder& query,
            const BlobSasBuilder& builder,
            const BlobResource& resource,
            std::string_view start,
            SasExpiry expiry)
        {
            query.Add("sv", SasVersion);
            query.Add("sp", builder.Permissions);
            query.Add("st", start);
            query.Add("se", expiry.Value);
            query.Add("sip", builder.IPRange);
            query.Add("spr", ProtocolValue(builder.Protocol));
            query.Add("sr", resource.Resource);
            query.Add("ses", builder.EncryptionScope);
            query.Add("rscc", builder.CacheControl);
            query.Add("rscd", builder.ContentDisposition);
            query.Add("rsce", builder.ContentEncoding);
            query.Add("rscl", builder.ContentLanguage);
            query.Add("rsct", builder.ContentType);
        }
    } // namespace

    std::string BlobSasBuilder::ToSasQueryParameters(const SharedKeyCredentialOptions& credential) const
    {
        const BlobResource resource = ResolveResource(*this, credential.AccountName);
        if (Identifier.empty() && (!ExpiresOn.has_value() || Permissions.empty()))
        {
            throw std::invalid_argument(
                "BlobSasBuilder requires ExpiresOn and Permissions unless Identifier names a stored access policy.");
        }
        const std::string start = FormatTime(StartsOn);
        const std::string expiry = FormatTime(ExpiresOn);
        const std::string stringToSign = JoinLines({
            Permissions,
            start,
            expiry,
            resource.Canonical,
            Identifier,
            IPRange,
            ProtocolValue(Protocol),
            SasVersion,
            resource.Resource,
            resource.SnapshotTime,
            EncryptionScope,
            CacheControl,
            ContentDisposition,
            ContentEncoding,
            ContentLanguage,
            ContentType,
        });

        QueryBuilder query;
        AddCommonBlobQuery(query, *this, resource, start, expiry);
        query.Add("si", Identifier);
        query.Add("sig", Private::SharedKeySigner{credential.AccountName, credential.AccountKey}.Sign(stringToSign));
        return std::move(query).Take();
    }

    std::string BlobSasBuilder::ToSasQueryParameters(const Models::UserDelegationKey& key,
        std::string_view accountName) const
    {
        const BlobResource resource = ResolveResource(*this, accountName);
        if (!ExpiresOn.has_value() || Permissions.empty())
        {
            throw std::invalid_argument("A user delegation SAS requires ExpiresOn and Permissions.");
        }
        if (!Identifier.empty())
        {
            throw std::invalid_argument("A user delegation SAS cannot use a stored access policy Identifier.");
        }
        const std::string start = FormatTime(StartsOn);
        const std::string expiry = FormatTime(ExpiresOn);
        const std::string keyStart = Private::FormatIso8601Utc(key.SignedStartsOn);
        const std::string keyExpiry = Private::FormatIso8601Utc(key.SignedExpiresOn);
        const std::string stringToSign = JoinLines({
            Permissions,
            start,
            expiry,
            resource.Canonical,
            key.SignedObjectId,
            key.SignedTenantId,
            keyStart,
            keyExpiry,
            key.SignedService,
            key.SignedVersion,
            {}, // signedAuthorizedUserObjectId
            {}, // signedUnauthorizedUserObjectId
            {}, // signedCorrelationId
            IPRange,
            ProtocolValue(Protocol),
            SasVersion,
            resource.Resource,
            resource.SnapshotTime,
            EncryptionScope,
            CacheControl,
            ContentDisposition,
            ContentEncoding,
            ContentLanguage,
            ContentType,
        });

        QueryBuilder query;
        AddCommonBlobQuery(query, *this, resource, start, expiry);
        query.Add("skoid", key.SignedObjectId);
        query.Add("sktid", key.SignedTenantId);
        query.Add("skt", keyStart);
        query.Add("ske", keyExpiry);
        query.Add("sks", key.SignedService);
        query.Add("skv", key.SignedVersion);
        query.Add("sig", Private::SharedKeySigner{std::string{accountName}, key.Value}.Sign(stringToSign));
        return std::move(query).Take();
    }

    std::string AccountSasBuilder::ToSasQueryParameters(const SharedKeyCredentialOptions& credential) const
    {
        if (credential.AccountName.empty())
        {
            throw std::invalid_argument("The account name is required to sign a SAS.");
        }
        if (Services.empty() || ResourceTypes.empty() || Permissions.empty() || ExpiresOn == Clock::time_point{})
        {
            throw std::invalid_argument(
                "AccountSasBuilder requires Services, ResourceTypes, Permissions and ExpiresOn.");
        }
        const std::string start = FormatTime(StartsOn);
        const std::string expiry = Private::FormatIso8601Utc(ExpiresOn);
        // The account SAS string-to-sign ends with a newline after the encryption scope.
        const std::string stringToSign = JoinLines({
                                             credential.AccountName,
                                             Permissions,
                                             Services,
                                             ResourceTypes,
                                             start,
                                             expiry,
                                             IPRange,
                                             ProtocolValue(Protocol),
                                             SasVersion,
                                             EncryptionScope,
                                         }) +
                                         "\n";

        QueryBuilder query;
        query.Add("sv", SasVersion);
        query.Add("ss", Services);
        query.Add("srt", ResourceTypes);
        query.Add("sp", Permissions);
        query.Add("st", start);
        query.Add("se", expiry);
        query.Add("sip", IPRange);
        query.Add("spr", ProtocolValue(Protocol));
        query.Add("ses", EncryptionScope);
        query.Add("sig", Private::SharedKeySigner{credential.AccountName, credential.AccountKey}.Sign(stringToSign));
        return std::move(query).Take();
    }
} // namespace AVEVA::AzureClient::Sas