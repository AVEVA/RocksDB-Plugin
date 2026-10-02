#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobServiceModels.hpp"
#include <AVEVA/AzureClient/Sas.hpp>

#include <cstddef>
#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>

namespace
{
    using AVEVA::AzureClient::SharedKeyCredentialOptions;
    using AVEVA::AzureClient::Models::UserDelegationKey;
    using AVEVA::AzureClient::Sas::AccountSasBuilder;
    using AVEVA::AzureClient::Sas::BlobSasBuilder;
    using AVEVA::AzureClient::Sas::SasProtocol;
    using namespace std::chrono;

    // Expected signatures were produced independently with the azure-storage-blob Python SDK
    // (BlobSharedAccessSignature / SharedAccessSignature with x_ms_version 2023-11-03) and, for the user
    // delegation SAS, with Python's hmac over the documented string-to-sign.
    [[nodiscard]] const SharedKeyCredentialOptions& Credential()
    {
        static const SharedKeyCredentialOptions CredentialValue{.AccountName = "myaccount",
            .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="};
        return CredentialValue;
    }

    constexpr sys_seconds At(int day, int hour)
    {
        return sys_days{year{2024} / May / day} + hours{hour};
    }

    std::string Decode(std::string_view value)
    {
        std::string decoded;
        for (std::size_t i = 0; i < value.size(); ++i)
        {
            if (value.at(i) == '%' && i + 2 < value.size())
            {
                decoded += static_cast<char>(std::stoi(std::string{value.substr(i + 1, 2)}, nullptr, 16));
                i += 2;
            }
            else
            {
                decoded += value.at(i);
            }
        }
        return decoded;
    }

    std::map<std::string, std::string> ParseQuery(std::string_view query)
    {
        std::map<std::string, std::string> result;
        while (!query.empty())
        {
            const auto amp = query.find('&');
            const std::string_view pair = query.substr(0, amp);
            const auto eq = pair.find('=');
            EXPECT_NE(eq, std::string_view::npos);
            const auto [it, inserted] = result.emplace(std::string{pair.substr(0, eq)}, Decode(pair.substr(eq + 1)));
            EXPECT_TRUE(inserted) << "duplicate parameter " << it->first;
            query = amp == std::string_view::npos ? std::string_view{} : query.substr(amp + 1);
        }
        return result;
    }
} // namespace

TEST(SasTests, BlobSasMatchesReferenceSignature)
{
    BlobSasBuilder builder;
    builder.ContainerName = "images";
    builder.BlobName = "dir/photo one.jpg";
    builder.Permissions = "rcw";
    builder.StartsOn = At(1, 10);
    builder.ExpiresOn = At(2, 10);
    builder.IPRange = "168.1.5.60-168.1.5.70";
    builder.CacheControl = "no-cache";
    builder.ContentType = "image/jpeg";

    const std::string token = builder.ToSasQueryParameters(Credential());
    EXPECT_FALSE(token.starts_with('?'));
    const auto query = ParseQuery(token);
    const std::map<std::string, std::string> expected{
        {"sv", "2023-11-03"},
        {"sp", "rcw"},
        {"st", "2024-05-01T10:00:00Z"},
        {"se", "2024-05-02T10:00:00Z"},
        {"sip", "168.1.5.60-168.1.5.70"},
        {"spr", "https"},
        {"sr", "b"},
        {"rscc", "no-cache"},
        {"rsct", "image/jpeg"},
        {"sig", "r4OJpw+1kkvWwnvCwrkiGvejZQdp3RQUCdJzcx7yxH8="},
    };
    EXPECT_EQ(query, expected);
}

TEST(SasTests, TimestampsAreSerializedAsUtcWholeSeconds)
{
    BlobSasBuilder builder;
    builder.ContainerName = "images";
    builder.Permissions = "rl";
    builder.StartsOn = At(1, 10) + milliseconds{999};
    builder.ExpiresOn = At(2, 10) + milliseconds{1};

    const auto query = ParseQuery(builder.ToSasQueryParameters(Credential()));
    EXPECT_EQ(query.at("st"), "2024-05-01T10:00:00Z");
    EXPECT_EQ(query.at("se"), "2024-05-02T10:00:00Z");
}

TEST(SasTests, ContainerSasMatchesReferenceSignature)
{
    BlobSasBuilder builder;
    builder.ContainerName = "images";
    builder.Permissions = "rl";
    builder.ExpiresOn = At(2, 10);

    const auto query = ParseQuery(builder.ToSasQueryParameters(Credential()));
    EXPECT_EQ(query.at("sr"), "c");
    EXPECT_FALSE(query.contains("st"));
    EXPECT_EQ(query.at("sig"), "nAcRIjXRz6EDwUgJL6mSczGWgQVMtU+FVdYIxzVm6Ro=");
}

TEST(SasTests, SnapshotSasSignsSnapshotTime)
{
    BlobSasBuilder builder;
    builder.ContainerName = "images";
    builder.BlobName = "a.txt";
    builder.Snapshot = "2024-05-01T10:00:00.0000000Z";
    builder.Permissions = "r";
    builder.ExpiresOn = At(2, 10);

    const auto query = ParseQuery(builder.ToSasQueryParameters(Credential()));
    EXPECT_EQ(query.at("sr"), "bs");
    EXPECT_EQ(query.at("sig"), "FSMWwwsc8lNWxQYyHtdzSyMsc7I16145dVWPYa+RreM=");
}

TEST(SasTests, VersionSasUsesBvResource)
{
    BlobSasBuilder builder;
    builder.ContainerName = "images";
    builder.BlobName = "a.txt";
    builder.BlobVersionId = "2024-05-01T10:00:00.0000000Z";
    builder.Permissions = "r";
    builder.ExpiresOn = At(2, 10);

    BlobSasBuilder snapshotBuilder = builder;
    snapshotBuilder.BlobVersionId.clear();
    snapshotBuilder.Snapshot = builder.BlobVersionId;

    const auto query = ParseQuery(builder.ToSasQueryParameters(Credential()));
    EXPECT_EQ(query.at("sr"), "bv");
    // Same signed time but a different signed resource must change the signature.
    EXPECT_NE(query.at("sig"), ParseQuery(snapshotBuilder.ToSasQueryParameters(Credential())).at("sig"));
}

TEST(SasTests, StoredAccessPolicySasOmitsPermissionsAndExpiry)
{
    BlobSasBuilder builder;
    builder.ContainerName = "images";
    builder.Identifier = "policy-1";

    const auto query = ParseQuery(builder.ToSasQueryParameters(Credential()));
    EXPECT_EQ(query.at("si"), "policy-1");
    EXPECT_FALSE(query.contains("sp"));
    EXPECT_FALSE(query.contains("se"));
    builder.Protocol = SasProtocol::HttpsAndHttp;
    EXPECT_EQ(ParseQuery(builder.ToSasQueryParameters(Credential())).at("spr"), "https,http");
}

TEST(SasTests, AccountSasMatchesReferenceSignature)
{
    AccountSasBuilder builder;
    builder.ResourceTypes = "sco";
    builder.Permissions = "rwdlac";
    builder.StartsOn = At(1, 10);
    builder.ExpiresOn = At(2, 10);

    const auto query = ParseQuery(builder.ToSasQueryParameters(Credential()));
    const std::map<std::string, std::string> expected{
        {"sv", "2023-11-03"},
        {"ss", "b"},
        {"srt", "sco"},
        {"sp", "rwdlac"},
        {"st", "2024-05-01T10:00:00Z"},
        {"se", "2024-05-02T10:00:00Z"},
        {"spr", "https"},
        {"sig", "Ge/Jm5nj+8D03idh9odgfqZ9wOQrfMR0INawjKyE2fs="},
    };
    EXPECT_EQ(query, expected);
}

TEST(SasTests, UserDelegationSasMatchesReferenceSignature)
{
    UserDelegationKey key;
    key.SignedObjectId = "oid";
    key.SignedTenantId = "tid";
    key.SignedStartsOn = At(1, 9);
    key.SignedExpiresOn = At(3, 9);
    key.SignedService = "b";
    key.SignedVersion = "2023-11-03";
    key.Value = "dXNlci1kZWxlZ2F0aW9uLWtleS1ieXRlcy0wMDAwMDA=";

    BlobSasBuilder builder;
    builder.ContainerName = "images";
    builder.BlobName = "a.txt";
    builder.Permissions = "r";
    builder.ExpiresOn = At(2, 10);

    const auto query = ParseQuery(builder.ToSasQueryParameters(key, "myaccount"));
    EXPECT_EQ(query.at("skoid"), "oid");
    EXPECT_EQ(query.at("sktid"), "tid");
    EXPECT_EQ(query.at("skt"), "2024-05-01T09:00:00Z");
    EXPECT_EQ(query.at("ske"), "2024-05-03T09:00:00Z");
    EXPECT_EQ(query.at("sks"), "b");
    EXPECT_EQ(query.at("skv"), "2023-11-03");
    EXPECT_EQ(query.at("sr"), "b");
    EXPECT_EQ(query.at("sig"), "DOJJCnspOy3EDJZDM+ayZ2ux0lClS8P82olshKupw/k=");
}

TEST(SasTests, InvalidBuildersThrow)
{
    BlobSasBuilder noContainer;
    noContainer.Permissions = "r";
    noContainer.ExpiresOn = At(2, 10);
    EXPECT_THROW((void)noContainer.ToSasQueryParameters(Credential()), std::invalid_argument);

    BlobSasBuilder noExpiry;
    noExpiry.ContainerName = "c";
    noExpiry.Permissions = "r";
    EXPECT_THROW((void)noExpiry.ToSasQueryParameters(Credential()), std::invalid_argument);

    BlobSasBuilder snapshotWithoutBlob;
    snapshotWithoutBlob.ContainerName = "c";
    snapshotWithoutBlob.Snapshot = "s";
    snapshotWithoutBlob.Permissions = "r";
    snapshotWithoutBlob.ExpiresOn = At(2, 10);
    EXPECT_THROW((void)snapshotWithoutBlob.ToSasQueryParameters(Credential()), std::invalid_argument);

    BlobSasBuilder both = snapshotWithoutBlob;
    both.BlobName = "b";
    both.BlobVersionId = "v";
    EXPECT_THROW((void)both.ToSasQueryParameters(Credential()), std::invalid_argument);

    BlobSasBuilder policyWithUdk;
    policyWithUdk.ContainerName = "c";
    policyWithUdk.Permissions = "r";
    policyWithUdk.ExpiresOn = At(2, 10);
    policyWithUdk.Identifier = "p";
    UserDelegationKey key;
    key.Value = "dXNlci1kZWxlZ2F0aW9uLWtleS1ieXRlcy0wMDAwMDA=";
    EXPECT_THROW((void)policyWithUdk.ToSasQueryParameters(key, "myaccount"), std::invalid_argument);

    AccountSasBuilder account;
    account.Permissions = "r";
    account.ExpiresOn = At(2, 10);
    EXPECT_THROW((void)account.ToSasQueryParameters(Credential()), std::invalid_argument);

    BlobSasBuilder badKey;
    badKey.ContainerName = "c";
    badKey.Permissions = "r";
    badKey.ExpiresOn = At(2, 10);
    EXPECT_THROW(
        (void)badKey.ToSasQueryParameters(SharedKeyCredentialOptions{.AccountName = "a", .AccountKey = "not base64!"}),
        std::invalid_argument);
}

TEST(SasTests, UnusualBlobNamesAreSignedAsDecodedNames)
{
    for (const std::string name : {"a b", "100%", "q?x", "h#x", "d/e", "caf\xC3\xA9", "%41"})
    {
        BlobSasBuilder builder;
        builder.ContainerName = "c";
        builder.BlobName = name;
        builder.Permissions = "r";
        builder.ExpiresOn = At(2, 10);
        EXPECT_NO_THROW(static_cast<void>(builder.ToSasQueryParameters(Credential()))) << name;
    }
    BlobSasBuilder a;
    a.ContainerName = "c";
    a.BlobName = "%41";
    a.Permissions = "r";
    a.ExpiresOn = At(2, 10);
    BlobSasBuilder b = a;
    b.BlobName = "A";
    EXPECT_NE(ParseQuery(a.ToSasQueryParameters(Credential())).at("sig"),
        ParseQuery(b.ToSasQueryParameters(Credential())).at("sig"));
}

TEST(SasTests, AmbiguousResourceNamesAreRejected)
{
    const auto sign = [](std::string container, std::string blob)
    {
        BlobSasBuilder builder;
        builder.ContainerName = std::move(container);
        builder.BlobName = std::move(blob);
        builder.Permissions = "r";
        builder.ExpiresOn = At(2, 10);
        return builder.ToSasQueryParameters(Credential());
    };
    EXPECT_THROW(static_cast<void>(sign("c", "a/../b")), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(sign("c", "./b")), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(sign("c", "a//b")), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(sign("c/d", "b")), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(sign("..", "b")), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(sign("c", "a\nb")), std::invalid_argument);
}
