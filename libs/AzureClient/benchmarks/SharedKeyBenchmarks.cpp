#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <benchmark/benchmark.h>

#include <boost/asio/io_context.hpp>

#include <string>

namespace
{
    using AVEVA::HttpRequest;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobContainerClientOptions;
    using AVEVA::AzureClient::SharedKeyCredentialOptions;
    using AVEVA::AzureClient::Private::AuthorizeRequest;
    using AVEVA::AzureClient::Private::SharedKeySigner;

    // The well-known Azurite development account key.
    constexpr const char* AccountKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    [[nodiscard]] HttpRequest MakeRequest()
    {
        HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Put);
        request.SetUrl("https://account.blob.core.windows.net/container/blob?comp=block&blockid=AAAA");
        request.AddHeader(AVEVA::HttpHeader("x-ms-date", "Mon, 01 Jan 2020 00:00:00 GMT"));
        request.AddHeader(AVEVA::HttpHeader("x-ms-version", "2023-11-03"));
        request.AddHeader(AVEVA::HttpHeader("x-ms-client-request-id", "00000000-0000-0000-0000-000000000000"));
        request.SetBody("hello world");
        return request;
    }

    class IdleHttpClient final : public AVEVA::IHttpClient
    {
      public:
        executor_type get_executor() const override
        {
            return {m_context.get_executor()};
        }

        void SendAsyncErased(HttpRequest /*request*/,
            CompletionHandler /*completion*/,
            AVEVA::HttpRequestOptions /*options*/) override
        {
        }

      private:
        mutable boost::asio::io_context m_context;
    };

    // Signing with the per-connection signer: the key is decoded and HMAC keyed once (T12).
    void BmSharedKeySign(benchmark::State& state)
    {
        const SharedKeySigner signer{"account", AccountKey};
        const HttpRequest request = MakeRequest();
        for (auto _ : state)
        {
            std::string authorization = signer.Authorize(request);
            benchmark::DoNotOptimize(authorization.data());
        }
    }

    BENCHMARK(BmSharedKeySign);

    // Baseline: decode the key and fetch/key HMAC on every request (the pre-T12 behaviour).
    void BmSharedKeySignOneShot(benchmark::State& state)
    {
        const SharedKeyCredentialOptions options{.AccountName = "account", .AccountKey = AccountKey};
        for (auto _ : state)
        {
            HttpRequest request = MakeRequest();
            AuthorizeRequest(options, request);
            benchmark::DoNotOptimize(request);
        }
    }

    BENCHMARK(BmSharedKeySignOneShot);

    // Child clients share the container's connection state instead of re-normalising options (T14).
    void BmGetBlockBlobClient(benchmark::State& state)
    {
        IdleHttpClient httpClient;
        const BlobContainerClient container{httpClient,
            BlobContainerClientOptions{.ServiceEndpoint = "https://account.blob.core.windows.net",
                .ContainerName = "container",
                .SharedKey = {.AccountName = "account", .AccountKey = AccountKey}}};
        for (auto _ : state)
        {
            auto client = container.GetBlockBlobClient("folder/blob.bin");
            benchmark::DoNotOptimize(client);
        }
    }

    BENCHMARK(BmGetBlockBlobClient);
} // namespace
