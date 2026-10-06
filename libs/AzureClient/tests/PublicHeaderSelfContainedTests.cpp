// Compile-and-run check that consumers can use every client through the public headers alone (T01).
// This target deliberately has no include path into src/ or tests/: only <AVEVA/AzureClient/...>.
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobServiceClient.hpp"
#include "AVEVA/AzureClient/BlockBlobClient.hpp"
#include "AVEVA/AzureClient/PageBlobClient.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <string>
#include <utility>

namespace
{
    using namespace AVEVA::AzureClient;

    // Minimal transport: every request succeeds; completions are posted, as the contract requires.
    class LoopbackHttpClient final : public AVEVA::IHttpClient
    {
      public:
        executor_type get_executor() const override
        {
            return m_context.get_executor();
        }

        void SendAsyncErased(AVEVA::HttpRequest request,
            CompletionHandler completion,
            AVEVA::HttpRequestOptions /*options*/) override
        {
            const bool isList = request.GetUrl().contains("comp=list");
            AVEVA::HttpResponse response{200,
                {{"ETag", "\"etag\""}, {"Last-Modified", "Sun, 06 Nov 1994 08:49:37 GMT"}, {"x-ms-request-id", "req"}},
                isList ? R"(<?xml version="1.0" encoding="utf-8"?><EnumerationResults></EnumerationResults>)" : ""};
            ++m_requests;
            boost::asio::post(m_context,
                [completion = std::move(completion), response = std::move(response)]() mutable
            {
                completion({}, std::move(response));
            });
        }

        void Run()
        {
            m_context.restart();
            m_context.run();
        }

        [[nodiscard]] int Requests() const noexcept
        {
            return m_requests;
        }

      private:
        mutable boost::asio::io_context m_context;
        int m_requests = 0;
    };

    constexpr const char* Endpoint = "https://account.blob.core.windows.net";
    constexpr const char* TestSasToken = "sv=2025-01-05&sig=fake";

    // Free coroutine taking a pointer rather than a capturing coroutine lambda, so the coroutine
    // frame never outlives captured references (cppcoreguidelines-avoid-capturing-lambda-coroutines).
    template <class MakeOperation> boost::asio::awaitable<void> AwaitHasValue(bool* out, MakeOperation makeOperation)
    {
        const auto result = co_await makeOperation();
        *out = result.has_value();
    }

    [[nodiscard]] BlobClientOptions BlobOptions()
    {
        return BlobClientOptions{.ServiceEndpoint = Endpoint,
            .ContainerName = "container",
            .BlobName = "blob",
            .SasToken = TestSasToken};
    }

    // Exercises one operation of `client` with (a) a lambda, (b) use_future and (c) the default
    // deferred token co_awaited in a coroutine.
    template <class TClient, class TOperation>
    void ExerciseAllTokenKinds(LoopbackHttpClient& http, TClient& client, TOperation operation)
    {
        bool lambdaOk = false;
        operation(client,
            [&lambdaOk](auto result)
        {
            lambdaOk = result.has_value();
        });
        http.Run();
        EXPECT_TRUE(lambdaOk) << "lambda";

        auto future = operation(client, boost::asio::use_future);
        http.Run();
        EXPECT_TRUE(future.get().has_value()) << "use_future";

        bool awaitedOk = false;
        boost::asio::co_spawn(client.get_executor(),
            AwaitHasValue(&awaitedOk,
                [&]
        {
            return operation(client, typename TClient::DefaultCompletionToken{});
        }),
            boost::asio::detached);
        http.Run();
        EXPECT_TRUE(awaitedOk) << "deferred + co_await";
    }
} // namespace

TEST(PublicHeaderSelfContainedTests, BlockBlobClient)
{
    LoopbackHttpClient http;
    BlockBlobClient client{http, BlobOptions()};
    ExerciseAllTokenKinds(http,
        client,
        [](BlockBlobClient& c, auto&& token)
    {
        return c.GetPropertiesAsync(std::forward<decltype(token)>(token));
    });
    ExerciseAllTokenKinds(http,
        client,
        [](BlockBlobClient& c, auto&& token)
    {
        return c.UploadAsync(std::string{"data"}, std::forward<decltype(token)>(token));
    });
    EXPECT_EQ(http.Requests(), 6);
}

TEST(PublicHeaderSelfContainedTests, PageBlobClient)
{
    LoopbackHttpClient http;
    PageBlobClient client{http, BlobOptions()};
    ExerciseAllTokenKinds(http,
        client,
        [](PageBlobClient& c, auto&& token)
    {
        return c.DeleteAsync(std::forward<decltype(token)>(token));
    });
}

TEST(PublicHeaderSelfContainedTests, BlobContainerClient)
{
    LoopbackHttpClient http;
    BlobContainerClient client{http,
        BlobContainerClientOptions{.ServiceEndpoint = Endpoint,
            .ContainerName = "container",
            .SasToken = TestSasToken}};
    ExerciseAllTokenKinds(http,
        client,
        [](BlobContainerClient& c, auto&& token)
    {
        return c.ListBlobsAsync(std::forward<decltype(token)>(token));
    });

    BlockBlobClient child = client.GetBlockBlobClient("child");
    ExerciseAllTokenKinds(http,
        child,
        [](BlockBlobClient& c, auto&& token)
    {
        return c.ExistsAsync(std::forward<decltype(token)>(token));
    });
}
