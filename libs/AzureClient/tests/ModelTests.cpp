#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "BlobRequestHelpers.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <cstdint>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <initializer_list>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace
{
    using AVEVA::HttpHeader;
    using AVEVA::HttpMethod;
    using AVEVA::HttpRequest;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::AcquireLeaseOptions;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::BreakLeaseOptions;
    using AVEVA::AzureClient::DeleteBlobOptions;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::ResizePageBlobOptions;
    using AVEVA::AzureClient::SetBlobAccessTierOptions;
    using AVEVA::AzureClient::SetBlobHttpHeadersOptions;
    using AVEVA::AzureClient::UploadBlockBlobOptions;
    using AVEVA::AzureClient::Models::AccessTier;
    using AVEVA::AzureClient::Models::CopyStatus;
    using AVEVA::AzureClient::Models::DeleteSnapshotsOption;
    using AVEVA::AzureClient::Models::LeaseDurationType;
    using AVEVA::AzureClient::Models::LeaseState;
    using AVEVA::AzureClient::Models::LeaseStatus;
    using AVEVA::AzureClient::Models::PublicAccessType;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    namespace Private = AVEVA::AzureClient::Private;
    using namespace std::chrono_literals;

    constexpr auto IgnoreResult = [](auto&&) {};

    [[nodiscard]] bool HasHeader(const HttpRequest& request, std::string_view name)
    {
        return std::ranges::any_of(request.GetHeaders(),
            [name](const auto& header)
        {
            return Private::IEquals(header.GetName(), name);
        });
    }

    [[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name)
    {
        return FakeHttpClient::FindHeaderValue(request, name);
    }

    void ExpectMalformedExtendedHeaderIsRejected(std::string_view name, std::string_view value)
    {
        EXPECT_THROW(static_cast<void>(Private::ParseBlobProperties(
                         HttpResponse{200, {{std::string{name}, std::string{value}}}, ""})),
            std::invalid_argument)
            << name;
    }

    void ExpectMalformedExtendedHeadersAreRejected()
    {
        for (const auto& [name, value] :
            std::vector<std::pair<std::string, std::string>>{{"x-ms-creation-time", "yesterday"},
                {"x-ms-server-encrypted", "maybe"},
                {"x-ms-access-tier-inferred", "1"},
                {"x-ms-blob-sequence-number", "-1"},
                {"x-ms-blob-committed-block-count", "seven"},
                {"x-ms-is-current-version", "yes"}})
        {
            ExpectMalformedExtendedHeaderIsRejected(name, value);
        }
    }

    void ExpectAcquireLeaseRejectsInvalidDurations(BlockBlobClient& client,
        FakeHttpClient& httpClient,
        std::initializer_list<std::chrono::seconds> durations)
    {
        for (const auto duration : durations)
        {
            std::optional<std::error_code> error;
            client.AcquireLeaseAsync(AcquireLeaseOptions{.Conditions = {}, .ProposedLeaseId = {}, .Duration = duration},
                [&](auto result)
            {
                ASSERT_FALSE(result.has_value());
                error = result.error().Code;
            });
            httpClient.Poll();
            EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument)) << duration.count();
        }
    }
} // namespace

TEST(ModelTests, ExtensibleEnumsCompareWithConstantsAndStringsAndKeepUnknownValues)
{
    AccessTier tier = "Hot";
    EXPECT_EQ(tier, AccessTier::Hot());
    EXPECT_TRUE(tier == "Hot");
    EXPECT_FALSE(tier == AccessTier::Cool());
    EXPECT_EQ(AccessTier::P80().ToString(), "P80");

    const AccessTier future{std::string{"SuperCool"}};
    EXPECT_EQ(future.ToString(), "SuperCool");
    EXPECT_TRUE(AccessTier{}.empty());

    EXPECT_EQ(DeleteSnapshotsOption::Include().ToString(), "include");
    EXPECT_EQ(DeleteSnapshotsOption::OnlySnapshots().ToString(), "only");
    EXPECT_EQ(CopyStatus::Pending().ToString(), "pending");
    EXPECT_EQ(CopyStatus::Success().ToString(), "success");
    EXPECT_EQ(CopyStatus::Aborted().ToString(), "aborted");
    EXPECT_EQ(CopyStatus::Failed().ToString(), "failed");
}

TEST(ModelTests, GetPropertiesParsesExtendedHeaders)
{
    const HttpResponse response{200,
        {{"ETag", "\"0x1\""},
            {"Content-Length", "10"},
            {"Content-Encoding", "gzip"},
            {"Content-Language", "en-GB"},
            {"Content-Disposition", "attachment; filename=a.txt"},
            {"x-ms-blob-type", "AppendBlob"},
            {"x-ms-creation-time", "Wed, 01 Oct 2025 10:00:00 GMT"},
            {"x-ms-access-tier", "Cool"},
            {"x-ms-access-tier-inferred", "true"},
            {"x-ms-lease-status", "locked"},
            {"x-ms-lease-state", "leased"},
            {"x-ms-lease-duration", "infinite"},
            {"x-ms-copy-id", "copy-1"},
            {"x-ms-copy-status", "pending"},
            {"x-ms-copy-source", "https://src/blob"},
            {"x-ms-copy-progress", "5/10"},
            {"x-ms-server-encrypted", "true"},
            {"x-ms-version-id", "2025-10-01T10:00:00.0000000Z"},
            {"x-ms-is-current-version", "false"},
            {"x-ms-blob-committed-block-count", "7"},
            {"x-ms-blob-sequence-number", "42"}},
        ""};

    const auto properties = Private::ParseBlobProperties(response);

    EXPECT_EQ(properties.ContentEncoding, "gzip");
    EXPECT_EQ(properties.ContentLanguage, "en-GB");
    EXPECT_EQ(properties.ContentDisposition, "attachment; filename=a.txt");
    ASSERT_TRUE(properties.CreatedOn.has_value());
    EXPECT_EQ(*properties.CreatedOn, *Private::ParseHttpDateHeader("Wed, 01 Oct 2025 10:00:00 GMT"));
    EXPECT_EQ(properties.AccessTier, AccessTier::Cool());
    EXPECT_EQ(properties.AccessTierInferred, std::optional<bool>{true});
    EXPECT_EQ(properties.LeaseStatus, LeaseStatus::Locked);
    EXPECT_EQ(properties.LeaseState, LeaseState::Leased);
    EXPECT_EQ(properties.LeaseDuration, LeaseDurationType::Infinite);
    EXPECT_EQ(properties.CopyId, "copy-1");
    EXPECT_EQ(properties.CopyStatus, CopyStatus::Pending());
    EXPECT_EQ(properties.CopySource, "https://src/blob");
    EXPECT_EQ(properties.CopyProgress, "5/10");
    EXPECT_EQ(properties.ServerEncrypted, std::optional<bool>{true});
    EXPECT_EQ(properties.VersionId, "2025-10-01T10:00:00.0000000Z");
    EXPECT_EQ(properties.IsCurrentVersion, std::optional<bool>{false});
    EXPECT_EQ(properties.CommittedBlockCount, std::optional<std::uint64_t>{7});
    EXPECT_EQ(properties.SequenceNumber, std::optional<std::uint64_t>{42});
}

TEST(ModelTests, GetPropertiesLeavesAbsentExtendedFieldsUnset)
{
    const auto properties = Private::ParseBlobProperties(HttpResponse{200, {{"ETag", "\"0x1\""}}, ""});

    EXPECT_FALSE(properties.CreatedOn.has_value());
    EXPECT_TRUE(properties.AccessTier.empty());
    EXPECT_FALSE(properties.AccessTierInferred.has_value());
    EXPECT_EQ(properties.LeaseStatus, LeaseStatus::Unknown);
    EXPECT_TRUE(properties.CopyStatus.empty());
    EXPECT_FALSE(properties.ServerEncrypted.has_value());
    EXPECT_FALSE(properties.IsCurrentVersion.has_value());
    EXPECT_FALSE(properties.CommittedBlockCount.has_value());
    EXPECT_FALSE(properties.SequenceNumber.has_value());
}

TEST(ModelTests, GetPropertiesRejectsMalformedExtendedHeaders)
{
    ExpectMalformedExtendedHeadersAreRejected();
}

TEST(ModelTests, ListBlobsParsesExtendedPropertiesAndVersionSiblings)
{
    const auto result = Private::ParseListBlobsResultXml(R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults><Blobs><Blob>
  <Name>a.txt</Name>
  <VersionId>v1</VersionId>
  <IsCurrentVersion>true</IsCurrentVersion>
  <Properties>
    <Creation-Time>Wed, 01 Oct 2025 10:00:00 GMT</Creation-Time>
    <Content-Encoding>gzip</Content-Encoding>
    <Content-Language>de</Content-Language>
    <Content-Disposition>inline</Content-Disposition>
    <x-ms-blob-sequence-number>3</x-ms-blob-sequence-number>
    <BlobType>PageBlob</BlobType>
    <AccessTier>Archive</AccessTier>
    <AccessTierInferred>false</AccessTierInferred>
    <LeaseStatus>unlocked</LeaseStatus>
    <LeaseState>available</LeaseState>
    <CopyId>c</CopyId>
    <CopyStatus>success</CopyStatus>
    <ServerEncrypted>true</ServerEncrypted>
  </Properties>
</Blob></Blobs></EnumerationResults>)");

    ASSERT_EQ(result.Blobs.size(), 1U);
    const auto& properties = result.Blobs.at(0).Properties;
    EXPECT_EQ(properties.VersionId, "v1");
    EXPECT_EQ(properties.IsCurrentVersion, std::optional<bool>{true});
    EXPECT_TRUE(properties.CreatedOn.has_value());
    EXPECT_EQ(properties.ContentEncoding, "gzip");
    EXPECT_EQ(properties.ContentLanguage, "de");
    EXPECT_EQ(properties.ContentDisposition, "inline");
    EXPECT_EQ(properties.SequenceNumber, std::optional<std::uint64_t>{3});
    EXPECT_EQ(properties.AccessTier, AccessTier::Archive());
    EXPECT_EQ(properties.AccessTierInferred, std::optional<bool>{false});
    EXPECT_EQ(properties.LeaseStatus, LeaseStatus::Unlocked);
    EXPECT_EQ(properties.LeaseState, LeaseState::Available);
    EXPECT_EQ(properties.CopyStatus, CopyStatus::Success());
    EXPECT_EQ(properties.ServerEncrypted, std::optional<bool>{true});
}

TEST(ModelTests, ListBlobContainersParsesLeaseAndPublicAccessProperties)
{
    const auto result = Private::ParseListBlobContainersResultXml(R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults><Containers>
  <Container><Name>public</Name><Properties>
    <Last-Modified>Wed, 01 Oct 2025 10:00:00 GMT</Last-Modified><Etag>"0x1"</Etag>
    <LeaseStatus>locked</LeaseStatus><LeaseState>leased</LeaseState><LeaseDuration>fixed</LeaseDuration>
    <PublicAccess>container</PublicAccess><HasImmutabilityPolicy>true</HasImmutabilityPolicy><HasLegalHold>true</HasLegalHold>
    <DefaultEncryptionScope>$account-encryption-key</DefaultEncryptionScope><DenyEncryptionScopeOverride>true</DenyEncryptionScopeOverride>
  </Properties></Container>
  <Container><Name>private</Name><Properties><Etag>"0x2"</Etag></Properties></Container>
</Containers></EnumerationResults>)");

    ASSERT_EQ(result.Containers.size(), 2U);
    const auto& first = result.Containers.at(0).Properties;
    EXPECT_EQ(first.Status, LeaseStatus::Locked);
    EXPECT_EQ(first.State, LeaseState::Leased);
    EXPECT_EQ(first.DurationType, LeaseDurationType::Fixed);
    EXPECT_EQ(first.AccessType, PublicAccessType::BlobContainer);
    EXPECT_TRUE(first.HasImmutabilityPolicy);
    EXPECT_TRUE(first.HasLegalHold);
    EXPECT_EQ(first.DefaultEncryptionScope, "$account-encryption-key");
    EXPECT_TRUE(first.PreventEncryptionScopeOverride);
    EXPECT_EQ(result.Containers.at(1).Properties.AccessType, PublicAccessType::None);
}

TEST(ModelTests, StartCopyResultExposesTypedCopyStatus)
{
    const auto result = Private::ParseStartBlobCopyFromUriResult(
        HttpResponse{202, {{"x-ms-copy-id", "id"}, {"x-ms-copy-status", "pending"}}, ""});
    EXPECT_EQ(result.CopyStatus, CopyStatus::Pending());
}

TEST(ModelTests, UploadSendsContentEncodingLanguageAndDisposition)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    UploadBlockBlobOptions options;
    options.HttpHeaders.ContentEncoding = "gzip";
    options.HttpHeaders.ContentLanguage = "fr";
    options.HttpHeaders.ContentDisposition = "attachment";
    options.AccessTier = AccessTier::Cold();
    client.UploadAsync(std::string{"abc"}, std::move(options), IgnoreResult);
    httpClient.Poll();

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(Header(request, "x-ms-blob-content-encoding"), "gzip");
    EXPECT_EQ(Header(request, "x-ms-blob-content-language"), "fr");
    EXPECT_EQ(Header(request, "x-ms-blob-content-disposition"), "attachment");
    EXPECT_EQ(Header(request, "x-ms-access-tier"), "Cold");

    client.UploadAsync(std::string{"abc"}, IgnoreResult);
    httpClient.Poll();
    EXPECT_FALSE(HasHeader(httpClient.LastRequest(), "x-ms-blob-content-encoding"));
    EXPECT_FALSE(HasHeader(httpClient.LastRequest(), "x-ms-access-tier"));
}

TEST(ModelTests, SetHttpHeadersSendsEveryPropertyHeaderBecauseOmittedOnesAreCleared)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    SetBlobHttpHeadersOptions options;
    options.HttpHeaders.ContentLanguage = "nl";
    client.SetHttpHeadersAsync(std::move(options), IgnoreResult);
    httpClient.Poll();

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(Header(request, "x-ms-blob-content-language"), "nl");
    EXPECT_TRUE(HasHeader(request, "x-ms-blob-content-encoding"));
    EXPECT_TRUE(HasHeader(request, "x-ms-blob-content-disposition"));
}

TEST(ModelTests, TypedAccessTierAndDeleteSnapshotsOptionAreSent)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    client.SetAccessTierAsync(SetBlobAccessTierOptions{.AccessTier = AccessTier::Archive(), .Conditions = {}},
        IgnoreResult);
    httpClient.Poll();
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-access-tier"), "Archive");

    DeleteBlobOptions deleteOptions;
    deleteOptions.DeleteSnapshotsOption = DeleteSnapshotsOption::OnlySnapshots();
    client.DeleteAsync(std::move(deleteOptions), IgnoreResult);
    httpClient.Poll();
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-delete-snapshots"), "only");
}

TEST(ModelTests, SharedKeyStringToSignIncludesContentEncodingAndLanguage)
{
    HttpRequest request;
    request.SetMethod(HttpMethod::Put);
    request.SetUrl("https://account.blob.core.windows.net/c/b");
    request.AddHeader(HttpHeader{"Content-Encoding", "gzip"});
    request.AddHeader(HttpHeader{"Content-Language", "en"});

    EXPECT_TRUE(Private::BuildSharedKeyStringToSign("account", request).starts_with("PUT\ngzip\nen\n\n"));

    HttpRequest plain;
    plain.SetMethod(HttpMethod::Get);
    plain.SetUrl("https://account.blob.core.windows.net/c/b");
    EXPECT_TRUE(Private::BuildSharedKeyStringToSign("account", plain).starts_with("GET\n\n\n\n\n\n\n"));
}

TEST(ModelTests, AcquireLeaseDurationIsInfiniteByDefaultAndValidated)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    client.AcquireLeaseAsync(IgnoreResult);
    httpClient.Poll();
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-lease-duration"), "-1");

    client.AcquireLeaseAsync(AcquireLeaseOptions{.Conditions = {}, .ProposedLeaseId = {}, .Duration = 15s},
        IgnoreResult);
    httpClient.Poll();
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-lease-duration"), "15");
    const std::size_t sent = httpClient.RequestCount();

    ExpectAcquireLeaseRejectsInvalidDurations(client, httpClient, {14s, 61s, 0s});
    EXPECT_EQ(httpClient.RequestCount(), sent);
}

TEST(ModelTests, BreakLeasePeriodIsValidated)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    client.BreakLeaseAsync(BreakLeaseOptions{.Conditions = {}, .BreakPeriod = 0s}, IgnoreResult);
    httpClient.Poll();
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-lease-break-period"), "0");
    const std::size_t sent = httpClient.RequestCount();

    std::optional<std::error_code> error;
    client.BreakLeaseAsync(BreakLeaseOptions{.Conditions = {}, .BreakPeriod = 61s},
        [&](auto result)
    {
        ASSERT_FALSE(result.has_value());
        error = result.error().Code;
    });
    httpClient.Poll();
    EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), sent);
}

TEST(ModelTests, ResizeUsesResizePageBlobOptionsConditions)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, MakeBlobClientOptions()};

    ResizePageBlobOptions options;
    options.Conditions.IfMatch = "\"etag\"";
    client.ResizeAsync(1024U, std::move(options), IgnoreResult);
    httpClient.Poll();

    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-blob-content-length"), "1024");
    EXPECT_EQ(Header(httpClient.LastRequest(), "If-Match"), "\"etag\"");
}

TEST(ModelTests, ExtensibleEnumComparesInBothOrdersAndKeepsEmbeddedNulls)
{
    const AccessTier tier = "Hot";
    EXPECT_TRUE(tier == "Hot");
    EXPECT_TRUE("Hot" == tier);
    EXPECT_FALSE("Cool" == tier);
    EXPECT_TRUE(tier == AccessTier::Hot());

    const AccessTier fromString = std::string{"Cool"};
    EXPECT_EQ(fromString, AccessTier::Cool());

    const AccessTier withNull{std::string_view{"a\0b", 3}};
    EXPECT_EQ(withNull.ToString().size(), 3U);
    const std::string_view embedded{"a\0b", 3};
    EXPECT_TRUE(embedded == withNull);
}
