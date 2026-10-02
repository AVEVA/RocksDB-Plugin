#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/AppendBlobClient.hpp>
#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

namespace
{
    using AVEVA::HttpMethod;
    using AVEVA::HttpRequest;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::AppendBlobClient;
    using AVEVA::AzureClient::BlobClient;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobServiceClient;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::FindBlobsByTagsOptions;
    using AVEVA::AzureClient::GetBlobTagsOptions;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::SetBlobTagsOptions;
    using AVEVA::AzureClient::Models::BlobTags;
    using AVEVA::AzureClient::Models::FindBlobsByTagsResult;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;

    [[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name)
    {
        return FakeHttpClient::FindHeaderValue(request, name);
    }

    [[nodiscard]] std::string Query(const HttpRequest& request)
    {
        const std::string& url = request.GetUrl();
        const auto question = url.find('?');
        std::string query = question == std::string::npos ? std::string{} : url.substr(question + 1);
        // The fixture's SAS token is appended last.
        constexpr std::string_view Sas = "sv=2025-01-05&sig=fakesig";
        EXPECT_TRUE(query.ends_with(Sas)) << query;
        query.resize(query.size() - std::min(query.size(), Sas.size()));
        if (query.ends_with('&'))
        {
            query.pop_back();
        }
        return query;
    }

    constexpr std::string_view FindResponseXml =
        R"(<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="https://myaccount.blob.core.windows.net/">)"
        R"(<Where>"status" = 'ready'</Where><Blobs>)"
        R"(<Blob><Name>a.txt</Name><ContainerName>images</ContainerName><Tags><TagSet><Tag><Key>status</Key><Value>ready</Value></Tag></TagSet></Tags></Blob>)"
        R"(<Blob><Name>dir/b.txt</Name><ContainerName>docs</ContainerName></Blob>)"
        R"(</Blobs><NextMarker>next-1</NextMarker></EnumerationResults>)";

    void ExpectFirstFindResultBlob(const FindBlobsByTagsResult& result)
    {
        EXPECT_EQ(result.Blobs.at(0).BlobName, "a.txt");
        EXPECT_EQ(result.Blobs.at(0).ContainerName, "images");
        EXPECT_EQ(result.Blobs.at(0).Tags, (BlobTags{{"status", "ready"}}));
    }

    void ExpectSecondFindResultBlob(const FindBlobsByTagsResult& result)
    {
        EXPECT_EQ(result.Blobs.at(1).BlobName, "dir/b.txt");
        EXPECT_TRUE(result.Blobs.at(1).Tags.empty());
    }

    void ExpectFindResult(const FindBlobsByTagsResult& result)
    {
        EXPECT_EQ(result.Where, "\"status\" = 'ready'");
        EXPECT_EQ(result.NextMarker, "next-1");
        ASSERT_EQ(result.Blobs.size(), 2U);
        ExpectFirstFindResultBlob(result);
        ExpectSecondFindResultBlob(result);
    }

    [[nodiscard]] BlobTags GetTags(BlobClient& client, FakeHttpClient& httpClient, const GetBlobTagsOptions& options)
    {
        std::optional<BlobTags> tags;
        client.GetTagsAsync(options,
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            tags = result->Value().Tags;
        });
        httpClient.Poll();
        EXPECT_TRUE(tags.has_value());
        return *tags;
    }

    [[nodiscard]] bool SetTags(BlobClient& client,
        FakeHttpClient& httpClient,
        const BlobTags& tagsToSet,
        const SetBlobTagsOptions& options = {})
    {
        bool succeeded = false;
        client.SetTagsAsync(tagsToSet,
            options,
            [&](auto result)
        {
            succeeded = result.has_value();
        });
        httpClient.Poll();
        return succeeded;
    }

    void ExpectSetTagsRejected(BlobClient& client, FakeHttpClient& httpClient, const BlobTags& tags)
    {
        std::optional<std::error_code> error;
        client.SetTagsAsync(tags,
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
        httpClient.Poll();
        ASSERT_TRUE(error.has_value());
        EXPECT_EQ(*error, std::make_error_code(std::errc::invalid_argument));
    }

    [[nodiscard]] BlobTags BuildTooManyTags()
    {
        BlobTags tooMany;
        for (int i = 0; i < 11; ++i)
        {
            tooMany.emplace("k" + std::to_string(i), "v");
        }
        return tooMany;
    }

    [[nodiscard]] BlobTags BuildMaximalTags()
    {
        BlobTags maximal;
        for (int i = 0; i < 9; ++i)
        {
            maximal.emplace("k" + std::to_string(i), "");
        }
        maximal.emplace(std::string(128, 'k'), std::string(256, 'v'));
        return maximal;
    }

    void ExpectInvalidTagsAreRejected(BlobClient& client,
        FakeHttpClient& httpClient,
        const std::vector<BlobTags>& invalid)
    {
        for (const BlobTags& tags : invalid)
        {
            ExpectSetTagsRejected(client, httpClient, tags);
        }
    }

    void ExpectGetTagsQuery(BlobClient& client, FakeHttpClient& httpClient, std::string_view expectedQuery)
    {
        client.GetTagsAsync([](auto) {});
        httpClient.Poll();
        EXPECT_EQ(Query(httpClient.LastRequest()), expectedQuery);
    }

    void ExpectDeleteQuery(BlobClient& client, FakeHttpClient& httpClient, std::string_view expectedQuery)
    {
        client.DeleteAsync([](auto) {});
        httpClient.Poll();
        EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Delete);
        EXPECT_EQ(Query(httpClient.LastRequest()), expectedQuery);
    }

    void ExpectGetPropertiesQuery(BlobClient& client, FakeHttpClient& httpClient, std::string_view expectedQuery)
    {
        client.GetPropertiesAsync([](auto) {});
        httpClient.Poll();
        EXPECT_EQ(Query(httpClient.LastRequest()), expectedQuery);
    }

    [[nodiscard]] FindBlobsByTagsResult FindBlobsByTags(BlobContainerClient& client,
        FakeHttpClient& httpClient,
        std::string_view where)
    {
        std::optional<FindBlobsByTagsResult> found;
        client.FindBlobsByTagsAsync(std::string{where},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            found = result->Value();
        });
        httpClient.Poll();
        EXPECT_TRUE(found.has_value());
        return *found;
    }

    [[nodiscard]] std::error_code FindBlobsByTagsError(BlobContainerClient& client,
        FakeHttpClient& httpClient,
        std::string_view where)
    {
        std::optional<std::error_code> error;
        client.FindBlobsByTagsAsync(std::string{where},
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
        httpClient.Poll();
        EXPECT_TRUE(error.has_value());
        return *error;
    }
} // namespace

TEST(TagsAndVersionsTests, GetTagsSendsConditionsAndParsesTagSet)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200,
        MakeCanonicalSuccessHeaders(),
        R"(<?xml version="1.0" encoding="utf-8"?><Tags><TagSet><Tag><Key>project</Key><Value>alpha</Value></Tag>)"
        R"(<Tag><Key>stage</Key><Value>2</Value></Tag></TagSet></Tags>)"});
    BlobClient client{httpClient, MakeBlobClientOptions()};

    BlobTags tags =
        GetTags(client, httpClient, GetBlobTagsOptions{.LeaseId = "lease-1", .TagConditions = "\"stage\" = '2'"});
    const HttpRequest& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Get);
    EXPECT_EQ(Query(request), "comp=tags");
    EXPECT_EQ(Header(request, "x-ms-lease-id"), "lease-1");
    EXPECT_EQ(Header(request, "x-ms-if-tags"), "\"stage\" = '2'");
    EXPECT_EQ(tags, (BlobTags{{"project", "alpha"}, {"stage", "2"}}));
}

TEST(TagsAndVersionsTests, GetTagsRejectsMalformedXml)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), "<Tags><TagSet>"});
    BlobClient client{httpClient, MakeBlobClientOptions()};

    std::optional<std::error_code> error;
    client.GetTagsAsync([&](auto result)
    {
        ASSERT_FALSE(result.has_value());
        error = result.error().Code;
    });
    httpClient.Poll();
    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(*error, std::error_code{AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse});
}

TEST(TagsAndVersionsTests, SetTagsSendsXmlBodyAndHeaders)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{204, MakeCanonicalSuccessHeaders(), ""});
    BlobClient client{httpClient, MakeBlobClientOptions()};

    const bool succeeded = SetTags(client,
        httpClient,
        BlobTags{{"project", "alpha"}, {"path", "a/b:c=d_e+f-g.h"}},
        SetBlobTagsOptions{.LeaseId = "lease-1",
            .TagConditions = "\"project\" = 'beta'",
            .TransactionalContentMd5 = "bWQ1"});
    const HttpRequest& request = httpClient.LastRequest();
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(request.GetMethod(), HttpMethod::Put);
    EXPECT_EQ(Query(request), "comp=tags");
    EXPECT_EQ(Header(request, "Content-Type"), "application/xml; charset=UTF-8");
    EXPECT_EQ(Header(request, "x-ms-lease-id"), "lease-1");
    EXPECT_EQ(Header(request, "x-ms-if-tags"), "\"project\" = 'beta'");
    EXPECT_EQ(Header(request, "Content-MD5"), "bWQ1");
    EXPECT_EQ(FakeHttpClient::BodyAsString(request),
        R"(<?xml version="1.0" encoding="utf-8"?><Tags><TagSet>)"
        R"(<Tag><Key>path</Key><Value>a/b:c=d_e+f-g.h</Value></Tag><Tag><Key>project</Key><Value>alpha</Value></Tag>)"
        R"(</TagSet></Tags>)");
}

TEST(TagsAndVersionsTests, SetTagsValidatesTagsWithoutSendingARequest)
{
    const std::vector<BlobTags> invalid{
        BuildTooManyTags(),
        BlobTags{{"", "v"}},
        BlobTags{{std::string(129, 'k'), "v"}},
        BlobTags{{"k", std::string(257, 'v')}},
        BlobTags{{"bad<key", "v"}},
        BlobTags{{"k", "bad&value"}},
        BlobTags{{"k", "caf\xC3\xA9"}},
    };

    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    ExpectInvalidTagsAreRejected(client, httpClient, invalid);
    EXPECT_TRUE(httpClient.Requests().empty());

    // Boundary values are accepted.
    BlobTags maximal = BuildMaximalTags();
    httpClient.EnqueueResponse(HttpResponse{204, MakeCanonicalSuccessHeaders(), ""});
    const bool succeeded = SetTags(client, httpClient, maximal);
    EXPECT_TRUE(succeeded);
}

TEST(TagsAndVersionsTests, UndeleteSendsCompUndelete)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    BlobClient client{httpClient, MakeBlobClientOptions()};

    bool succeeded = false;
    client.UndeleteAsync([&](auto result)
    {
        succeeded = result.has_value();
    });
    httpClient.Poll();
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Put);
    EXPECT_EQ(Query(httpClient.LastRequest()), "comp=undelete");
}

TEST(TagsAndVersionsTests, VersionAndSnapshotScopedClientsAddTheirQueryParameter)
{
    FakeHttpClient httpClient;
    BlobClient base{httpClient, MakeBlobClientOptions()};

    BlobClient version = base.WithVersionId("2024-05-01T10:00:00.1234567Z");
    ExpectGetTagsQuery(version, httpClient, "versionid=2024-05-01T10%3A00%3A00.1234567Z&comp=tags");

    ExpectDeleteQuery(version, httpClient, "versionid=2024-05-01T10%3A00%3A00.1234567Z");

    BlobClient snapshot = version.WithSnapshot("2024-05-01T11:00:00.0000000Z");
    ExpectGetPropertiesQuery(snapshot, httpClient, "snapshot=2024-05-01T11%3A00%3A00.0000000Z");

    BlobClient current = snapshot.WithSnapshot({});
    ExpectGetPropertiesQuery(current, httpClient, "");

    auto options = MakeBlobClientOptions();
    options.VersionId = "v1";
    BlobClient fromOptions{httpClient, options};
    ExpectGetPropertiesQuery(fromOptions, httpClient, "versionid=v1");

    options.Snapshot = "s1";
    EXPECT_THROW((BlobClient{httpClient, options}), std::invalid_argument);
}

TEST(TagsAndVersionsTests, DerivedBlobClientsKeepTheirTypeWhenScopedToSnapshotOrVersion)
{
    FakeHttpClient httpClient;
    BlockBlobClient block{httpClient, MakeBlobClientOptions()};
    PageBlobClient page{httpClient, MakeBlobClientOptions()};
    AppendBlobClient append{httpClient, MakeBlobClientOptions()};

    static_assert(std::is_same_v<decltype(block.WithSnapshot("s")), BlockBlobClient>);
    static_assert(std::is_same_v<decltype(page.WithVersionId("v")), PageBlobClient>);
    static_assert(std::is_same_v<decltype(append.WithSnapshot("s")), AppendBlobClient>);

    BlockBlobClient blockSnapshot = block.WithSnapshot("s1");
    ExpectGetPropertiesQuery(blockSnapshot, httpClient, "snapshot=s1");
    PageBlobClient pageVersion = page.WithVersionId("v1");
    ExpectGetPropertiesQuery(pageVersion, httpClient, "versionid=v1");
    AppendBlobClient appendBase = append.WithSnapshot("s1").WithSnapshot({});
    ExpectGetPropertiesQuery(appendBase, httpClient, "");
}

TEST(TagsAndVersionsTests, ServiceFindBlobsByTagsEncodesFilterAndParsesResult)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), std::string{FindResponseXml}});
    BlobServiceClient client{httpClient, MakeBlobServiceClientOptions()};

    std::optional<FindBlobsByTagsResult> found;
    client.FindBlobsByTagsAsync("\"status\" = 'ready'",
        FindBlobsByTagsOptions{.Marker = "m 1", .MaxResults = 5},
        [&](auto result)
    {
        ASSERT_TRUE(result.has_value());
        found = result->Value();
    });
    httpClient.Poll();

    const HttpRequest& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Get);
    EXPECT_EQ(Query(request), "comp=blobs&where=%22status%22%20%3D%20%27ready%27&marker=m%201&maxresults=5");
    ASSERT_TRUE(found.has_value());
    ExpectFindResult(*found);
}

TEST(TagsAndVersionsTests, ContainerFindBlobsByTagsTargetsTheContainer)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), std::string{FindResponseXml}});
    BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

    FindBlobsByTagsResult found = FindBlobsByTags(client, httpClient, "\"status\" = 'ready'");
    const HttpRequest& request = httpClient.LastRequest();
    EXPECT_NE(request.GetUrl().find("/images?"), std::string::npos);
    EXPECT_EQ(Query(request), "restype=container&comp=blobs&where=%22status%22%20%3D%20%27ready%27");
    ExpectFindResult(found);

    EXPECT_EQ(FindBlobsByTagsError(client, httpClient, ""), std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.Requests().size(), 1U);
}
