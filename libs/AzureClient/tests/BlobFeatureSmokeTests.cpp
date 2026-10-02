#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/PageBlobClient.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestHelpers.hpp"

#include <AVEVA/AzureClient/AppendBlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/ITokenCredential.hpp>

#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <gtest/gtest.h>

#include <expected>
#include <memory>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::AccessToken;
    using AVEVA::AzureClient::AppendBlobClient;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobContainerClientOptions;
    using AVEVA::AzureClient::BlobServiceClient;
    using AVEVA::AzureClient::BlobServiceClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::ITokenCredential;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::AppendBlockResult;
    using AVEVA::AzureClient::Models::CreateAppendBlobResult;
    using AVEVA::AzureClient::Models::ListBlobContainersResult;
    using AVEVA::AzureClient::Models::ListBlobsResult;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::FakeHttpClient;

    class CountingCredential final : public ITokenCredential
    {
      public:
        void GetTokenAsync(std::vector<std::string> /*scopes*/, GetTokenCompletionHandler completion) override
        {
            ++m_callCount;
            completion({}, AccessToken{.Token = "token-value"});
        }

        [[nodiscard]] int CallCount() const noexcept
        {
            return m_callCount;
        }

      private:
        int m_callCount = 0;
    };

    void VerifyListBlobsParsedXml(const std::expected<Response<ListBlobsResult>, BlobStorageError>& result,
        CallbackExpectation& callback)
    {
        ASSERT_TRUE(result.has_value());
        const auto& response = result.value();
        ASSERT_EQ(response.Value().Blobs.size(), 1U);
        EXPECT_EQ(response.Value().Blobs.at(0).Name, "alpha.txt");
        EXPECT_EQ(response.Value().BlobPrefixes.at(0), "folder/");
        EXPECT_EQ(response.Value().NextMarker, "next");
        callback.MarkInvoked();
    }

    void StartListBlobsAndVerifyParsedXml(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.ListBlobsAsync([&](std::expected<Response<ListBlobsResult>, BlobStorageError> result)
        {
            VerifyListBlobsParsedXml(result, callback);
        });
    }
} // namespace

TEST(BlobFeatureSmokeTests, ListBlobsAsyncParsesXml)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        {},
        R"(<?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
              <Prefix>pre</Prefix>
              <Delimiter>/</Delimiter>
              <Blobs>
                <Blob>
                  <Name>alpha.txt</Name>
                  <Properties>
                    <BlobType>BlockBlob</BlobType>
                    <Content-Length>3</Content-Length>
                  </Properties>
                </Blob>
                <BlobPrefix><Name>folder/</Name></BlobPrefix>
              </Blobs>
              <NextMarker>next</NextMarker>
            </EnumerationResults>)"};

    BlobContainerClient client{httpClient,
        BlobContainerClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .ContainerName = "images",
            .SasToken = "sv=1&sig=2"}};

    CallbackExpectation callback;
    StartListBlobsAndVerifyParsedXml(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlobFeatureSmokeTests, ListBlobContainersAsyncParsesXml)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        {},
        R"(<?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
              <Containers>
                <Container>
                  <Name>images</Name>
                  <Metadata><Project>aveva</Project></Metadata>
                </Container>
              </Containers>
            </EnumerationResults>)"};

    BlobServiceClient client{httpClient,
        BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .SasToken = "sv=1&sig=2"}};

    CallbackExpectation callback;
    client.ListBlobContainersAsync([&](std::expected<Response<ListBlobContainersResult>, BlobStorageError> result)
    {
        ASSERT_TRUE(result.has_value());
        const auto& response = result.value();
        ASSERT_EQ(response.Value().Containers.size(), 1U);
        EXPECT_EQ(response.Value().Containers.at(0).Name, "images");
        EXPECT_EQ(response.Value().Containers.at(0).Properties.Metadata.at("project"), "aveva");
        callback.MarkInvoked();
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlobFeatureSmokeTests, TokenCredentialAddsBearerHeader)
{
    FakeHttpClient httpClient;
    auto inner = std::make_shared<CountingCredential>();
    BlobServiceClient client{httpClient,
        BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .TokenCredential = inner}};

    client.ListBlobContainersAsync([](std::expected<Response<ListBlobContainersResult>, BlobStorageError>) {});

    // BearerToken/TokenCredential completions that finish on the same stack are deferred
    // via post() (never completing before the initiating call returns); poll the fake
    // client's executor to observe them.
    httpClient.Poll();

    const std::string auth = FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization");
    EXPECT_EQ(auth, "Bearer token-value");
    EXPECT_EQ(inner->CallCount(), 1);
}

TEST(BlobFeatureSmokeTests, SharedKeyAddsAuthorizationHeader)
{
    FakeHttpClient httpClient;
    BlobServiceClient client{httpClient,
        BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="}}};

    client.ListBlobContainersAsync([](std::expected<Response<ListBlobContainersResult>, BlobStorageError>) {});

    const std::string auth = FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization");
    EXPECT_TRUE(std::regex_match(auth, std::regex{"^SharedKey storageaccount:[A-Za-z0-9+/]+=*$"}));
}

TEST(BlobFeatureSmokeTests, BlobServiceClientRejectsConflictingCredentials)
{
    FakeHttpClient httpClient;

    EXPECT_THROW((BlobServiceClient{httpClient,
                     BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
                         .SasToken = "sv=1&sig=2",
                         .BearerToken = "token"}}),
        std::invalid_argument);
}

TEST(BlobFeatureSmokeTests, BlobServiceClientRejectsInvalidSharedKeyBase64)
{
    FakeHttpClient httpClient;

    EXPECT_THROW((BlobServiceClient{httpClient,
                     BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
                         .SharedKey = {.AccountName = "storageaccount", .AccountKey = "bad=="}}}),
        std::invalid_argument);
}

TEST(BlobFeatureSmokeTests, AppendBlobClientBuildsAppendBlockRequest)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, {{"ETag", "\"etag\""}}, {}};

    AppendBlobClient client{httpClient,
        BlobClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .ContainerName = "images",
            .BlobName = "append.txt",
            .SasToken = "sv=1&sig=2"}};

    client.CreateAsync([](std::expected<Response<CreateAppendBlobResult>, BlobStorageError>) {});
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-blob-type"), "AppendBlob");

    client.AppendBlockAsync("abc", [](std::expected<Response<AppendBlockResult>, BlobStorageError>) {});
    EXPECT_NE(httpClient.LastRequest().GetUrl().find("comp=appendblock"), std::string::npos);
    EXPECT_EQ(httpClient.Requests().back().Body, "abc");
}

TEST(BlobFeatureSmokeTests, ChildClientsAreMovableAndWorkFromFactoryResults)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, {{"ETag", "\"etag\""}}, ""};
    BlobServiceClient service{httpClient,
        BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .SasToken = "sv=1&sig=2"}};
    BlobContainerClient container{httpClient,
        BlobContainerClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .ContainerName = "images",
            .SasToken = "sv=1&sig=2"}};

    std::vector<BlobServiceClient> services;
    services.push_back(std::move(service));

    std::vector<BlobContainerClient> containers;
    containers.push_back(std::move(container));

    std::vector<BlockBlobClient> clients;
    clients.push_back(containers.at(0).GetBlockBlobClient("vector-1.txt"));
    clients.push_back(containers.at(0).GetBlockBlobClient("vector-2.txt"));

    std::vector<AVEVA::AzureClient::PageBlobClient> pageClients;
    pageClients.push_back(containers.at(0).GetPageBlobClient("page-1.vhd"));

    std::vector<AppendBlobClient> appendClients;
    appendClients.push_back(containers.at(0).GetAppendBlobClient("append-1.log"));

    auto sharedClient = std::make_shared<BlockBlobClient>(containers.at(0).GetBlockBlobClient("shared.txt"));
    sharedClient->UploadAsync("hello",
        [](std::expected<Response<AVEVA::AzureClient::Models::UploadBlockBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(services.size(), 1U);
    EXPECT_EQ(containers.size(), 1U);
    EXPECT_EQ(clients.size(), 2U);
    EXPECT_EQ(pageClients.size(), 1U);
    EXPECT_EQ(appendClients.size(), 1U);
    EXPECT_NE(httpClient.LastRequest().GetUrl().find("/images/shared.txt"), std::string::npos);
}

TEST(BlobFeatureSmokeTests, BlobServiceClientCreatesContainerClientFactory)
{
    FakeHttpClient httpClient;
    BlobServiceClient client{httpClient,
        BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
            .SasToken = "sv=1&sig=2"}};

    BlobContainerClient container = client.GetBlobContainerClient("archive");
    container.ListBlobsAsync([](std::expected<Response<ListBlobsResult>, BlobStorageError>) {});

    EXPECT_NE(httpClient.LastRequest().GetUrl().find("/archive?restype=container&comp=list"), std::string::npos);
}
