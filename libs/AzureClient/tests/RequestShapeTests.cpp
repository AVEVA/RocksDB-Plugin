// Table-driven request-shape tests (T27): one row per public operation, asserting the exact HTTP
// method, path, query parameters, operation-specific headers and body of the first request it sends.
#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlockBlobClient.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/PageBlobClient.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <ostream>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    using namespace AVEVA::AzureClient;
    using AVEVA::HttpMethod;
    using AVEVA::HttpRequest;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
    using namespace std::chrono_literals;

    using Pairs = std::vector<std::pair<std::string, std::string>>;

    struct Clients
    {
        FakeHttpClient Http;
        BlobClient Blob{Http, MakeBlobClientOptions()};
        BlockBlobClient Block{Http, MakeBlobClientOptions()};
        PageBlobClient Page{Http, MakeBlobClientOptions()};
        BlobContainerClient Container{Http, MakeBlobContainerClientOptions()};
        std::istringstream Source{"hello"};
        std::ostringstream Sink;
    };

    constexpr auto Ignore = [](auto&&...) {};

    [[nodiscard]] std::chrono::system_clock::time_point Day(int day)
    {
        return std::chrono::sys_days{std::chrono::year{2024} / std::chrono::January / day};
    }

    [[nodiscard]] Models::BlobRequestConditions FullConditions()
    {
        Models::BlobRequestConditions conditions;
        conditions.LeaseId = "lease-1";
        conditions.IfMatch = "\"etag-1\"";
        conditions.IfNoneMatch = "\"etag-2\"";
        conditions.IfModifiedSince = Day(2);
        conditions.IfUnmodifiedSince = Day(3);
        return conditions;
    }

    // Headers produced by FullConditions().
    [[nodiscard]] Pairs ConditionHeaders(bool includeLeaseId = true)
    {
        Pairs headers{
            {"If-Match", "\"etag-1\""},
            {"If-None-Match", "\"etag-2\""},
            {"If-Modified-Since", "Tue, 02 Jan 2024 00:00:00 GMT"},
            {"If-Unmodified-Since", "Wed, 03 Jan 2024 00:00:00 GMT"},
        };
        if (includeLeaseId)
        {
            headers.emplace_back("x-ms-lease-id", "lease-1");
        }
        return headers;
    }

    [[nodiscard]] Models::MetadataMap Metadata()
    {
        Models::MetadataMap metadata;
        metadata.emplace("Project", "alpha");
        return metadata;
    }

    [[nodiscard]] Models::BlobHttpHeaders HttpHeaders()
    {
        Models::BlobHttpHeaders headers;
        headers.ContentType = "text/plain";
        headers.ContentEncoding = "gzip";
        headers.ContentLanguage = "en";
        headers.CacheControl = "no-cache";
        headers.ContentDisposition = "inline";
        headers.ContentMd5 = "bWQ1";
        return headers;
    }

    [[nodiscard]] Pairs Concat(Pairs first, const Pairs& second)
    {
        first.insert(first.end(), second.begin(), second.end());
        return first;
    }

    struct RequestShapeCase
    {
        std::string Name;
        std::function<void(Clients&)> Start;
        HttpMethod Method = HttpMethod::Get;
        std::string Path;
        Pairs Query;   // exact, in order, excluding the trailing SAS parameters
        Pairs Headers; // operation-specific headers that must be present with these values
        std::vector<std::string> AbsentHeaders;
        std::optional<std::string> Body; // exact body, when checked
    };

    void PrintTo(const RequestShapeCase& shape, std::ostream* os)
    {
        *os << shape.Name;
    }

    [[nodiscard]] std::string_view MethodName(HttpMethod method)
    {
        switch (method)
        {
        case HttpMethod::Get:
            return "GET";
        case HttpMethod::Put:
            return "PUT";
        case HttpMethod::Delete:
            return "DELETE";
        case HttpMethod::Head:
            return "HEAD";
        case HttpMethod::Post:
            return "POST";
        default:
            return "?";
        }
    }

    struct ParsedUrl
    {
        std::string Path;
        Pairs Query;
    };

    [[nodiscard]] ParsedUrl ParseUrl(std::string_view url)
    {
        constexpr std::string_view Host = "https://storageaccount.blob.core.windows.net";
        ParsedUrl parsed;
        EXPECT_TRUE(url.starts_with(Host)) << url;
        url.remove_prefix(std::min(url.size(), Host.size()));
        const std::size_t question = url.find('?');
        parsed.Path = std::string{url.substr(0, question)};
        if (question == std::string_view::npos)
        {
            return parsed;
        }
        std::string_view query = url.substr(question + 1U);
        while (!query.empty())
        {
            const std::size_t amp = query.find('&');
            const std::string_view part = query.substr(0, amp);
            const std::size_t eq = part.find('=');
            parsed.Query.emplace_back(std::string{part.substr(0, eq)},
                eq == std::string_view::npos ? std::string{} : std::string{part.substr(eq + 1U)});
            query = amp == std::string_view::npos ? std::string_view{} : query.substr(amp + 1U);
        }
        return parsed;
    }

    [[nodiscard]] bool HasHeader(const HttpRequest& request, std::string_view name)
    {
        return std::ranges::any_of(request.GetHeaders(),
            [name](const auto& header)
        {
            const std::string_view headerName = header.GetName();
            return std::ranges::equal(headerName,
                name,
                [](char a, char b)
            {
                return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
            });
        });
    }

    [[nodiscard]] std::string BodyOf(const FakeHttpClient& http)
    {
        return http.RequestAt(0).Body;
    }

    // --- The table ----------------------------------------------------------------------------------

    std::vector<RequestShapeCase> BlobCases()
    {
        const std::string blob = "/images/photo.png";
        std::vector<RequestShapeCase> cases;

        cases.push_back({.Name = "Blob_Download",
            .Start =
                [](Clients& c)
        {
            DownloadBlobOptions o;
            o.Conditions = FullConditions();
            c.Blob.DownloadAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Get,
            .Path = blob,
            .Query = {},
            .Headers = Concat({{"Range", "bytes=0-4194303"}}, ConditionHeaders()),
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_DownloadRange",
            .Start =
                [](Clients& c)
        {
            DownloadBlobOptions o;
            o.Range = Models::BlobByteRange{.Offset = 10, .Length = 20};
            c.Blob.DownloadAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Get,
            .Path = blob,
            .Query = {},
            .Headers = {{"Range", "bytes=10-29"}},
            .AbsentHeaders = {"If-Match", "x-ms-range"},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_DownloadTo",
            .Start =
                [](Clients& c)
        {
            DownloadToOptions o;
            o.ChunkSize = 1024;
            o.Concurrency = 2;
            c.Blob.DownloadToAsync(c.Sink, std::move(o), Ignore);
        },
            .Method = HttpMethod::Get,
            .Path = blob,
            .Query = {},
            .Headers = {{"Range", "bytes=0-1023"}},
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_DeleteIfExists",
            .Start =
                [](Clients& c)
        {
            c.Blob.DeleteIfExistsAsync(Ignore);
        },
            .Method = HttpMethod::Delete,
            .Path = blob,
            .Query = {},
            .Headers = {},
            .AbsentHeaders = {"x-ms-delete-snapshots"},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_GetProperties",
            .Start =
                [](Clients& c)
        {
            GetBlobPropertiesOptions o;
            o.Conditions = FullConditions();
            c.Blob.GetPropertiesAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Head,
            .Path = blob,
            .Query = {},
            .Headers = ConditionHeaders(),
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_Exists",
            .Start =
                [](Clients& c)
        {
            c.Blob.ExistsAsync(Ignore);
        },
            .Method = HttpMethod::Head,
            .Path = blob,
            .Query = {},
            .Headers = {},
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_SetMetadata",
            .Start =
                [](Clients& c)
        {
            SetBlobMetadataOptions o;
            o.Metadata = Metadata();
            o.Conditions = FullConditions();
            c.Blob.SetMetadataAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "metadata"}},
            .Headers = Concat({{"x-ms-meta-Project", "alpha"}}, ConditionHeaders()),
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_AcquireLease",
            .Start =
                [](Clients& c)
        {
            AcquireLeaseOptions o;
            o.Duration = 30s;
            o.ProposedLeaseId = "proposed-1";
            o.Conditions = FullConditions();
            c.Blob.AcquireLeaseAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "lease"}},
            .Headers = Concat({{"x-ms-lease-action", "acquire"},
                                  {"x-ms-lease-duration", "30"},
                                  {"x-ms-proposed-lease-id", "proposed-1"}},
                ConditionHeaders(false)),
            .AbsentHeaders = {"x-ms-lease-id"},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_AcquireLeaseInfinite",
            .Start =
                [](Clients& c)
        {
            c.Blob.AcquireLeaseAsync(Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "lease"}},
            .Headers = {{"x-ms-lease-action", "acquire"}, {"x-ms-lease-duration", "-1"}},
            .AbsentHeaders = {"x-ms-proposed-lease-id"},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_RenewLease",
            .Start =
                [](Clients& c)
        {
            RenewLeaseOptions o;
            o.LeaseId = "lease-1";
            c.Blob.RenewLeaseAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "lease"}},
            .Headers = {{"x-ms-lease-action", "renew"}, {"x-ms-lease-id", "lease-1"}},
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Blob_ReleaseLease",
            .Start =
                [](Clients& c)
        {
            ReleaseLeaseOptions o;
            o.LeaseId = "lease-1";
            o.Conditions = FullConditions();
            c.Blob.ReleaseLeaseAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "lease"}},
            .Headers =
                Concat({{"x-ms-lease-action", "release"}, {"x-ms-lease-id", "lease-1"}}, ConditionHeaders(false)),
            .AbsentHeaders = {},
            .Body = std::nullopt});
        return cases;
    }

    std::vector<RequestShapeCase> TypedBlobCases()
    {
        const std::string blob = "/images/photo.png";
        std::vector<RequestShapeCase> cases;

        cases.push_back({.Name = "Block_Upload",
            .Start =
                [](Clients& c)
        {
            UploadBlockBlobOptions o;
            o.HttpHeaders = HttpHeaders();
            o.Metadata = Metadata();
            o.AccessTier = Models::AccessTier::Cool();
            o.Conditions = FullConditions();
            o.TransactionalContentMd5 = "bWQ1";
            c.Block.UploadAsync(std::string{"hello"}, std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {},
            .Headers = Concat({{"x-ms-blob-type", "BlockBlob"},
                                  {"Content-MD5", "bWQ1"},
                                  {"x-ms-blob-content-type", "text/plain"},
                                  {"x-ms-blob-content-encoding", "gzip"},
                                  {"x-ms-blob-content-language", "en"},
                                  {"x-ms-blob-cache-control", "no-cache"},
                                  {"x-ms-blob-content-disposition", "inline"},
                                  {"x-ms-blob-content-md5", "bWQ1"},
                                  {"x-ms-meta-Project", "alpha"},
                                  {"x-ms-access-tier", "Cool"}},
                ConditionHeaders()),
            .AbsentHeaders = {},
            .Body = std::string{"hello"}});
        cases.push_back({.Name = "Block_UploadSpan",
            .Start =
                [](Clients& c)
        {
            static constexpr std::array<std::byte, 2> Bytes{std::byte{'h'}, std::byte{'i'}};
            c.Block.UploadAsync(std::span<const std::byte>{Bytes}, Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {},
            .Headers = {{"x-ms-blob-type", "BlockBlob"}},
            .AbsentHeaders = {"If-Match", "x-ms-meta-Project"},
            .Body = std::string{"hi"}});
        cases.push_back({.Name = "Block_StageBlock",
            .Start =
                [](Clients& c)
        {
            StageBlockOptions o;
            o.Conditions = FullConditions();
            o.TransactionalContentCrc64 = "Y3JjNjQ=";
            c.Block.StageBlockAsync("YmxvY2s=", std::string{"data"}, std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "block"}, {"blockid", "YmxvY2s%3D"}},
            .Headers = {{"x-ms-lease-id", "lease-1"}, {"x-ms-content-crc64", "Y3JjNjQ="}},
            .AbsentHeaders = {"If-Match", "If-None-Match", "x-ms-blob-type"},
            .Body = std::string{"data"}});
        cases.push_back({.Name = "Block_CommitBlockList",
            .Start =
                [](Clients& c)
        {
            CommitBlockListOptions o;
            o.HttpHeaders = HttpHeaders();
            o.Metadata = Metadata();
            o.AccessTier = Models::AccessTier::Archive();
            o.Conditions = FullConditions();
            c.Block.CommitBlockListAsync({"YQ==", "Yg=="}, std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "blocklist"}},
            .Headers = Concat({{"x-ms-blob-content-type", "text/plain"},
                                  {"x-ms-meta-Project", "alpha"},
                                  {"x-ms-access-tier", "Archive"}},
                ConditionHeaders()),
            .AbsentHeaders = {"x-ms-blob-type"},
            .Body = std::string{
                R"(<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>YQ==</Latest><Latest>Yg==</Latest></BlockList>)"}});
        cases.push_back({.Name = "Page_Create",
            .Start =
                [](Clients& c)
        {
            CreatePageBlobOptions o;
            o.HttpHeaders = HttpHeaders();
            o.Metadata = Metadata();
            o.AccessTier = Models::AccessTier::P10();
            o.Conditions = FullConditions();
            c.Page.CreateAsync(1024, std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {},
            .Headers = Concat({{"x-ms-blob-type", "PageBlob"},
                                  {"x-ms-blob-content-length", "1024"},
                                  {"x-ms-blob-content-type", "text/plain"},
                                  {"x-ms-meta-Project", "alpha"},
                                  {"x-ms-access-tier", "P10"}},
                ConditionHeaders()),
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Page_UploadPages",
            .Start =
                [](Clients& c)
        {
            UploadPagesOptions o;
            o.ContentMd5 = "bWQ1";
            o.Conditions = FullConditions();
            c.Page.UploadPagesAsync(512, std::string(512, 'x'), std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "page"}},
            .Headers =
                Concat({{"x-ms-page-write", "update"}, {"x-ms-range", "bytes=512-1023"}, {"Content-MD5", "bWQ1"}},
                    ConditionHeaders()),
            .AbsentHeaders = {},
            .Body = std::string(512, 'x')});
        cases.push_back({.Name = "Page_ClearPages",
            .Start =
                [](Clients& c)
        {
            ClearPagesOptions o;
            o.Conditions = FullConditions();
            c.Page.ClearPagesAsync(0, 512, std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "page"}},
            .Headers = Concat({{"x-ms-page-write", "clear"}, {"x-ms-range", "bytes=0-511"}}, ConditionHeaders()),
            .AbsentHeaders = {},
            .Body = std::string{}});
        cases.push_back({.Name = "Page_Resize",
            .Start =
                [](Clients& c)
        {
            ResizePageBlobOptions o;
            o.Conditions = FullConditions();
            c.Page.ResizeAsync(2048, std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = blob,
            .Query = {{"comp", "properties"}},
            .Headers = Concat({{"x-ms-blob-content-length", "2048"}}, ConditionHeaders()),
            .AbsentHeaders = {"x-ms-blob-type"},
            .Body = std::nullopt});

        return cases;
    }

    std::vector<RequestShapeCase> ContainerCases()
    {
        const std::string container = "/images";
        std::vector<RequestShapeCase> cases;

        cases.push_back({.Name = "Container_Create",
            .Start =
                [](Clients& c)
        {
            CreateBlobContainerOptions o;
            o.Metadata = Metadata();
            c.Container.CreateAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = container,
            .Query = {{"restype", "container"}},
            .Headers = {{"x-ms-meta-Project", "alpha"}},
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Container_CreateIfNotExists",
            .Start =
                [](Clients& c)
        {
            c.Container.CreateIfNotExistsAsync(Ignore);
        },
            .Method = HttpMethod::Put,
            .Path = container,
            .Query = {{"restype", "container"}},
            .Headers = {},
            .AbsentHeaders = {},
            .Body = std::nullopt});
        cases.push_back({.Name = "Container_ListBlobs",
            .Start =
                [](Clients& c)
        {
            ListBlobsOptions o;
            o.Prefix = "dir/a b";
            o.Delimiter = "/";
            o.Marker = "m1";
            o.MaxResults = 10;
            o.IncludeMetadata = true;
            o.IncludeSnapshots = true;
            o.IncludeVersions = true;
            o.IncludeDeleted = true;
            o.IncludeTags = true;
            o.IncludeUncommittedBlobs = true;
            o.IncludeCopy = true;
            c.Container.ListBlobsAsync(std::move(o), Ignore);
        },
            .Method = HttpMethod::Get,
            .Path = container,
            .Query = {{"restype", "container"},
                {"comp", "list"},
                {"prefix", "dir%2Fa%20b"},
                {"delimiter", "%2F"},
                {"marker", "m1"},
                {"maxresults", "10"},
                {"include", "copy%2Cdeleted%2Cmetadata%2Csnapshots%2Ctags%2Cuncommittedblobs%2Cversions"}},
            .Headers = {},
            .AbsentHeaders = {},
            .Body = std::nullopt});
        return cases;
    }

    std::vector<RequestShapeCase> AllCases()
    {
        std::vector<RequestShapeCase> all = BlobCases();
        for (auto&& group : {TypedBlobCases(), ContainerCases()})
        {
            all.insert(all.end(), group.begin(), group.end());
        }
        return all;
    }

    class RequestShapeTests : public ::testing::TestWithParam<RequestShapeCase>
    {
    };
} // namespace

TEST_P(RequestShapeTests, FirstRequestHasTheDocumentedShape)
{
    const RequestShapeCase& shape = GetParam();
    Clients clients;
    shape.Start(clients);
    clients.Http.Poll();
    ASSERT_GE(clients.Http.RequestCount(), 1U);

    const HttpRequest& request = clients.Http.RequestAt(0).Request;
    const std::string body = BodyOf(clients.Http);

    EXPECT_EQ(MethodName(request.GetMethod()), MethodName(shape.Method));

    ParsedUrl url = ParseUrl(request.GetUrl());
    EXPECT_EQ(url.Path, shape.Path);
    // The SAS is appended last, after every operation parameter.
    ASSERT_GE(url.Query.size(), 2U);
    EXPECT_EQ(url.Query.at(url.Query.size() - 2U), (std::pair<std::string, std::string>{"sv", "2025-01-05"}));
    EXPECT_EQ(url.Query.back(), (std::pair<std::string, std::string>{"sig", "fakesig"}));
    url.Query.resize(url.Query.size() - 2U);
    EXPECT_EQ(url.Query, shape.Query);

    EXPECT_FALSE(FakeHttpClient::FindHeaderValue(request, "x-ms-version").empty());
    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(request, "x-ms-date").ends_with(" GMT"));
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-client-request-id").size(), 36U);
    EXPECT_FALSE(HasHeader(request, "Authorization")) << "SAS requests are not signed";

    for (const auto& [name, value] : shape.Headers)
    {
        EXPECT_TRUE(HasHeader(request, name)) << name;
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, name), value) << name;
    }
    for (const std::string& name : shape.AbsentHeaders)
    {
        EXPECT_FALSE(HasHeader(request, name)) << name;
    }
    if (shape.Body.has_value())
    {
        EXPECT_EQ(body, *shape.Body);
    }
}

INSTANTIATE_TEST_SUITE_P(AllOperations,
    RequestShapeTests,
    ::testing::ValuesIn(AllCases()),
    [](const ::testing::TestParamInfo<RequestShapeCase>& paramInfo)
{
    return paramInfo.param.Name;
});
