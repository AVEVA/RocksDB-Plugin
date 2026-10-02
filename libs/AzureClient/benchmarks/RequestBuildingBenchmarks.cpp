#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>

namespace
{
    using AVEVA::HttpMethod;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobContainerClientOptions;
    using AVEVA::AzureClient::Private::BuildBlobUrl;
    using AVEVA::AzureClient::Private::BuildContainerRequest;
    using AVEVA::AzureClient::Private::BuildQueryString;
    using AVEVA::AzureClient::Private::MakeBlobTarget;
    using AVEVA::AzureClient::Private::MakeContainerTarget;
    using AVEVA::AzureClient::Private::ParseBlobProperties;
    using AVEVA::AzureClient::Private::ParseListBlobsResultXml;

    constexpr unsigned int HttpStatusOk = 200U;
    // Size of the synthetic List Blobs page the XML-parsing benchmarks work on.
    constexpr int ListBlobsPageEntryCount = 5000;

    [[nodiscard]] BlobClientOptions MakeBlobOptions()
    {
        return BlobClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .ContainerName = "images",
            .BlobName = "folder/photo.png",
            .SasToken = "sv=2025-01-05&sig=fakesig"};
    }

    [[nodiscard]] BlobContainerClientOptions MakeContainerOptions()
    {
        return BlobContainerClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .ContainerName = "images",
            .SasToken = "sv=2025-01-05&sig=fakesig"};
    }

    [[nodiscard]] HttpResponse MakePropertiesResponse()
    {
        return HttpResponse{HttpStatusOk,
            {
                {"ETag", "\"etag\""},
                {"Last-Modified", "Fri, 26 Jun 2015 18:59:17 GMT"},
                {"Content-Length", "1048576"},
                {"Content-Type", "application/octet-stream"},
                {"Content-MD5", "abcd"},
                {"Cache-Control", "max-age=60"},
                {"x-ms-blob-type", "BlockBlob"},
                {"x-ms-meta-project", "aveva"},
                {"x-ms-meta-owner", "storage"},
            },
            {}};
    }

    [[nodiscard]] const std::string& ListBlobsXml()
    {
        static const std::string Xml = R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://storageaccount.blob.core.windows.net/" ContainerName="images">
  <Prefix>folder/</Prefix>
  <Delimiter>/</Delimiter>
  <Blobs>
    <Blob>
      <Name>folder/alpha.txt</Name>
      <Properties>
        <BlobType>BlockBlob</BlobType>
        <Content-Length>3</Content-Length>
      </Properties>
      <Metadata>
        <project>aveva</project>
      </Metadata>
    </Blob>
    <BlobPrefix>
      <Name>folder/subdir/</Name>
    </BlobPrefix>
  </Blobs>
  <NextMarker>marker-2</NextMarker>
</EnumerationResults>)";
        return Xml;
    }

    // A full 5,000-item List Blobs page with the properties Azure returns for each blob.
    [[nodiscard]] const std::string& LargeListBlobsXml()
    {
        static const std::string Xml = []
        {
            std::string page = R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://storageaccount.blob.core.windows.net/" ContainerName="images"><Prefix>folder/</Prefix><Blobs>)";
            for (int i = 0; i < ListBlobsPageEntryCount; ++i)
            {
                page +=
                    "<Blob><Name>folder/blob-" + std::to_string(i) +
                    R"(.bin</Name><Properties>)"
                    R"(<Creation-Time>Fri, 26 Jun 2015 18:59:17 GMT</Creation-Time><Last-Modified>Fri, 26 Jun 2015 18:59:17 GMT</Last-Modified>)"
                    R"(<Etag>0x8D1234567890ABC</Etag><Content-Length>4194304</Content-Length><Content-Type>application/octet-stream</Content-Type>)"
                    R"(<Content-Encoding /><Content-Language /><Content-MD5>1B2M2Y8AsgTpgAmY7PhCfg==</Content-MD5><Cache-Control />)"
                    R"(<BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>)"
                    R"(<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState><ServerEncrypted>true</ServerEncrypted>)"
                    R"(</Properties><Metadata><project>aveva</project></Metadata></Blob>)";
            }
            page += "</Blobs><NextMarker>marker-2</NextMarker></EnumerationResults>";
            return page;
        }();
        return Xml;
    }

    void BmBuildBlobUrl(benchmark::State& state)
    {
        const auto target = MakeBlobTarget(MakeBlobOptions());
        const std::string query =
            BuildQueryString({{"comp", "metadata"}, {"timeout", "30"}, {"snapshot", "2026-09-28T11:23:21.6900000Z"}});

        for (auto _ : state)
        {
            std::string url = BuildBlobUrl(target, query);
            benchmark::DoNotOptimize(url.data());
            benchmark::DoNotOptimize(url.size());
        }

        state.SetItemsProcessed(state.iterations());
    }

    BENCHMARK(BmBuildBlobUrl);

    void BmBuildBlobRequest(benchmark::State& state)
    {
        const auto target = MakeContainerTarget(MakeContainerOptions());
        const std::string query = BuildQueryString({{"restype", "container"}, {"comp", "metadata"}, {"timeout", "30"}});

        for (auto _ : state)
        {
            auto request = BuildContainerRequest(target, HttpMethod::Head, query);
            auto headerCount = request.GetHeaders().size();
            benchmark::DoNotOptimize(request);
            benchmark::DoNotOptimize(headerCount);
        }

        state.SetItemsProcessed(state.iterations());
    }

    BENCHMARK(BmBuildBlobRequest);

    void BmParseBlobProperties(benchmark::State& state)
    {
        const HttpResponse response = MakePropertiesResponse();

        for (auto _ : state)
        {
            auto properties = ParseBlobProperties(response);
            auto metadataSize = properties.Metadata.size();
            benchmark::DoNotOptimize(properties);
            benchmark::DoNotOptimize(metadataSize);
        }

        state.SetItemsProcessed(state.iterations());
    }

    BENCHMARK(BmParseBlobProperties);

    void BmParseListBlobsResultXml(benchmark::State& state)
    {
        const std::string& xml = ListBlobsXml();

        for (auto _ : state)
        {
            auto result = ParseListBlobsResultXml(xml);
            auto blobCount = result.Blobs.size();
            auto prefixCount = result.BlobPrefixes.size();
            benchmark::DoNotOptimize(result);
            benchmark::DoNotOptimize(blobCount);
            benchmark::DoNotOptimize(prefixCount);
        }

        state.SetItemsProcessed(state.iterations());
    }

    BENCHMARK(BmParseListBlobsResultXml);

    void BmParseListBlobsResultXml5000(benchmark::State& state)
    {
        const std::string& xml = LargeListBlobsXml();

        for (auto _ : state)
        {
            auto result = ParseListBlobsResultXml(xml);
            benchmark::DoNotOptimize(result);
        }

        state.SetItemsProcessed(state.iterations() * ListBlobsPageEntryCount);
        state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(xml.size()));
    }

    BENCHMARK(BmParseListBlobsResultXml5000)->Unit(benchmark::kMillisecond);
} // namespace
