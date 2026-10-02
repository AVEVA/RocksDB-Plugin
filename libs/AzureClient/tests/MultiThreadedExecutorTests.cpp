// Runs the multi-request engines (UploadFrom, DownloadTo, retry) on a multi-threaded executor, so
// completions for one operation genuinely arrive on several threads at once. Built as its own
// executable with the `concurrency` label; run it under ThreadSanitizer (LinuxDebugTSan) to check
// the engines' locking (T30).
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlockBlobClient.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include <cctype>
#include <expected>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    using namespace AVEVA::AzureClient;
    using AVEVA::HttpHeader;
    using AVEVA::HttpMethod;
    using AVEVA::HttpRequest;
    using AVEVA::HttpResponse;

    constexpr std::size_t ThreadCount = 4;

    [[nodiscard]] std::string PercentDecode(std::string_view value)
    {
        std::string decoded;
        for (std::size_t i = 0; i < value.size(); ++i)
        {
            if (value.at(i) == '%' && i + 2 < value.size())
            {
                decoded.push_back(static_cast<char>(std::stoi(std::string(value.substr(i + 1, 2)), nullptr, 16)));
                i += 2;
            }
            else
            {
                decoded.push_back(value.at(i));
            }
        }
        return decoded;
    }

    [[nodiscard]] std::string_view HeaderValue(const HttpRequest& request, std::string_view name)
    {
        for (const HttpHeader& header : request.GetHeaders())
        {
            if (std::ranges::equal(header.GetName(),
                    name,
                    [](char a, char b)
            {
                return std::tolower(a) == std::tolower(b);
            }))
            {
                return header.GetValue();
            }
        }
        return {};
    }

    [[nodiscard]] std::string BodyOf(const HttpRequest& request)
    {
        if (!request.HasBodyView())
        {
            return request.GetBody();
        }
        const auto view = request.GetBodyView();
        return {reinterpret_cast<const char*>(view.data()), view.size()};
    }

    // A thread-safe in-memory block blob service. Every completion is posted to a thread pool, so
    // consecutive completions of one operation run concurrently on different threads. Occasionally
    // answers 503 to exercise the retry engine's timer path too.
    class ThreadPoolBlobService final : public AVEVA::IHttpClient
    {
      public:
        ThreadPoolBlobService() = default;

        ~ThreadPoolBlobService() override
        {
            m_pool.join();
        }

        ThreadPoolBlobService(const ThreadPoolBlobService&) = delete;
        ThreadPoolBlobService& operator=(const ThreadPoolBlobService&) = delete;
        ThreadPoolBlobService(ThreadPoolBlobService&&) = delete;
        ThreadPoolBlobService& operator=(ThreadPoolBlobService&&) = delete;

        executor_type get_executor() const override
        {
            return m_pool.get_executor();
        }

        void SendAsyncErased(HttpRequest request,
            CompletionHandler completion,
            AVEVA::HttpRequestOptions /*options*/) override
        {
            HttpResponse response = Handle(request);
            boost::asio::post(m_pool,
                [completion = std::move(completion), response = std::move(response)]() mutable
            {
                completion({}, std::move(response));
            });
        }

        [[nodiscard]] std::size_t Requests() const noexcept
        {
            return m_requests.load();
        }

        [[nodiscard]] std::size_t Throttled() const noexcept
        {
            return m_throttled.load();
        }

      private:
        [[nodiscard]] static HttpResponse Ok(unsigned int status,
            std::vector<HttpHeader> extra = {},
            std::string body = {})
        {
            std::vector<HttpHeader> headers{{"ETag", "\"0x1\""},
                {"Last-Modified", "Fri, 26 Jun 2015 18:59:17 GMT"},
                {"x-ms-request-id", "req"}};
            headers.insert(headers.end(), extra.begin(), extra.end());
            return HttpResponse{status, std::move(headers), std::move(body)};
        }

        HttpResponse Handle(const HttpRequest& request)
        {
            if (++m_requests % 17 == 0)
            {
                ++m_throttled;
                return HttpResponse{503, {{"x-ms-error-code", "ServerBusy"}, {"Retry-After", "0"}}, ""};
            }

            const std::string& url = request.GetUrl();
            const std::size_t queryStart = url.find('?');
            const std::string path = url.substr(0, queryStart);
            std::map<std::string, std::string> query;
            for (std::string_view rest = queryStart == std::string::npos ? std::string_view{}
                                                                         : std::string_view(url).substr(queryStart + 1);
                !rest.empty();)
            {
                const std::size_t amp = rest.find('&');
                const std::string_view pair = rest.substr(0, amp);
                const std::size_t eq = pair.find('=');
                query[std::string(pair.substr(0, eq))] =
                    eq == std::string_view::npos ? std::string{} : PercentDecode(pair.substr(eq + 1));
                rest = amp == std::string_view::npos ? std::string_view{} : rest.substr(amp + 1);
            }

            const std::scoped_lock lock(m_mutex);
            if (request.GetMethod() == HttpMethod::Put)
            {
                if (query["comp"] == "block")
                {
                    m_blocks[path + "#" + query["blockid"]] = BodyOf(request);
                    return Ok(201);
                }
                if (query["comp"] == "blocklist")
                {
                    std::string content;
                    const std::string xml = BodyOf(request);
                    for (std::size_t pos = xml.find("<Latest>"); pos != std::string::npos;
                        pos = xml.find("<Latest>", pos))
                    {
                        pos += 8;
                        const std::size_t end = xml.find("</Latest>", pos);
                        content += m_blocks.at(path + "#" + xml.substr(pos, end - pos));
                    }
                    m_blobs[path] = std::move(content);
                    return Ok(201);
                }
                m_blobs[path] = BodyOf(request);
                return Ok(201);
            }

            const auto blob = m_blobs.find(path);
            if (blob == m_blobs.end())
            {
                return HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""};
            }
            const std::string& content = blob->second;
            const std::string_view range = HeaderValue(request, "Range");
            if (request.GetMethod() != HttpMethod::Get || range.empty())
            {
                return Ok(200,
                    {{"Content-Length", std::to_string(content.size())}},
                    request.GetMethod() == HttpMethod::Get ? content : "");
            }
            // "bytes=first-last"
            const std::size_t dash = range.find('-');
            const std::uint64_t first = std::stoull(std::string(range.substr(6, dash - 6)));
            const std::uint64_t last =
                std::min<std::uint64_t>(std::stoull(std::string(range.substr(dash + 1))), content.size() - 1U);
            std::string body =
                content.substr(static_cast<std::size_t>(first), static_cast<std::size_t>(last - first + 1U));
            const std::string contentLength = std::to_string(body.size());
            return Ok(206,
                {{"Content-Range",
                     "bytes " + std::to_string(first) + "-" + std::to_string(last) + "/" +
                         std::to_string(content.size())},
                    {"Content-Length", contentLength}},
                std::move(body));
        }

        mutable boost::asio::thread_pool m_pool{ThreadCount};
        std::mutex m_mutex;
        std::map<std::string, std::string> m_blobs;
        std::map<std::string, std::string> m_blocks;
        std::atomic<std::size_t> m_requests{0};
        std::atomic<std::size_t> m_throttled{0};
    };

    [[nodiscard]] BlobContainerClientOptions SharedKeyContainerOptions()
    {
        BlobContainerClientOptions options{.ServiceEndpoint = "https://account.blob.core.windows.net",
            .ContainerName = "container"};
        options.SharedKey = {.AccountName = "account",
            .AccountKey = "a2V5LWJ5dGVzLWZvci10aHJlYWQtc2FuaXRpemVyLXRlc3Q="};
        options.Retry.MaxRetries = 5;
        options.Retry.InitialDelay = std::chrono::milliseconds{0};
        options.Retry.MaxDelay = std::chrono::milliseconds{1};
        return options;
    }

    // Tags the content index with its own type so it's never adjacent-and-same-type with the size
    // parameter (see bugprone-easily-swappable-parameters).
    struct ContentIndex
    {
        std::size_t Value;

        constexpr ContentIndex(std::size_t value) noexcept : Value(value)
        {
        }
    };

    struct BlobCountValue
    {
        std::size_t Value;

        constexpr BlobCountValue(std::size_t value) noexcept : Value(value)
        {
        }
    };

    struct BlobSizeValue
    {
        std::size_t Value;

        constexpr BlobSizeValue(std::size_t value) noexcept : Value(value)
        {
        }
    };

    [[nodiscard]] std::string MakeContent(ContentIndex index, std::size_t size)
    {
        std::string content(size, '\0');
        for (std::size_t i = 0; i < size; ++i)
        {
            content.at(i) = static_cast<char>('a' + (((i * 31U) + (index.Value * 7U)) % 26U));
        }
        return content;
    }

    struct ConcurrentBlobData
    {
        std::vector<BlockBlobClient> Clients;
        std::vector<std::string> Contents;
        std::vector<std::istringstream> Sources;
    };

    [[nodiscard]] ConcurrentBlobData MakeConcurrentBlobData(const BlobContainerClient& container,
        BlobCountValue blobCount,
        BlobSizeValue blobSize)
    {
        ConcurrentBlobData data;
        data.Clients.reserve(blobCount.Value);
        data.Contents.reserve(blobCount.Value);
        for (std::size_t i = 0; i < blobCount.Value; ++i)
        {
            data.Clients.push_back(container.GetBlockBlobClient("blob-" + std::to_string(i)));
            data.Contents.push_back(MakeContent(i, blobSize.Value));
        }
        data.Sources.reserve(blobCount.Value);
        for (const std::string& content : data.Contents)
        {
            data.Sources.emplace_back(content);
        }
        return data;
    }

    void UploadAllBlobs(std::vector<BlockBlobClient>& clients,
        std::vector<std::istringstream>& sources,
        const UploadFromOptions& uploadOptions)
    {
        std::vector<std::future<std::expected<Response<Models::UploadBlockBlobResult>, BlobStorageError>>> uploads;
        uploads.reserve(clients.size());
        for (std::size_t i = 0; i < clients.size(); ++i)
        {
            uploads.push_back(clients.at(i).UploadFromAsync(sources.at(i), uploadOptions, boost::asio::use_future));
        }
        for (auto& upload : uploads)
        {
            const auto result = upload.get();
            ASSERT_TRUE(result.has_value()) << result.error().Message;
        }
    }

    void DownloadAndVerifyAllBlobs(std::vector<BlockBlobClient>& clients,
        const std::vector<std::string>& contents,
        const DownloadToOptions& downloadOptions)
    {
        std::vector<std::ostringstream> sinks(clients.size());
        std::vector<std::future<std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>>> downloads;
        downloads.reserve(clients.size());
        for (std::size_t i = 0; i < clients.size(); ++i)
        {
            downloads.push_back(clients.at(i).DownloadToAsync(sinks.at(i), downloadOptions, boost::asio::use_future));
        }
        for (std::size_t i = 0; i < clients.size(); ++i)
        {
            const auto result = downloads.at(i).get();
            ASSERT_TRUE(result.has_value()) << result.error().Message;
            EXPECT_EQ(sinks.at(i).str(), contents.at(i)) << "blob " << i;
        }
    }
} // namespace

TEST(MultiThreadedExecutorTests, ConcurrentChunkedUploadsAndDownloadsOnAThreadPool)
{
    constexpr std::size_t BlobCount = 8;
    constexpr std::size_t BlobSize = (64U * 1024U) + 123U;

    ThreadPoolBlobService service;
    const BlobContainerClient container{service, SharedKeyContainerOptions()};
    ConcurrentBlobData data = MakeConcurrentBlobData(container, BlobCountValue{BlobCount}, BlobSizeValue{BlobSize});

    UploadFromOptions uploadOptions;
    uploadOptions.BlockSize = 4096U;
    uploadOptions.Concurrency = 4U;
    UploadAllBlobs(data.Clients, data.Sources, uploadOptions);

    DownloadToOptions downloadOptions;
    downloadOptions.ChunkSize = 4096U;
    downloadOptions.Concurrency = 4U;
    DownloadAndVerifyAllBlobs(data.Clients, data.Contents, downloadOptions);
    EXPECT_GT(service.Throttled(), 0U) << "the retry path was not exercised";
}

TEST(MultiThreadedExecutorTests, ClientsAreSafeToInitiateFromManyThreads)
{
    constexpr std::size_t Threads = 8;
    constexpr std::size_t CallsPerThread = 25;

    ThreadPoolBlobService service;
    const BlobContainerClient container{service, SharedKeyContainerOptions()};
    BlockBlobClient shared = container.GetBlockBlobClient("shared");
    ASSERT_TRUE(shared.UploadAsync(std::string{"payload"}, boost::asio::use_future).get().has_value());

    std::atomic<std::size_t> succeeded{0};
    std::vector<std::thread> threads;
    threads.reserve(Threads);
    for (std::size_t t = 0; t < Threads; ++t)
    {
        threads.emplace_back([&, t]
        {
            // Mix the shared client with per-thread derived clients so both the shared target and
            // the shared signer are used concurrently.
            BlockBlobClient own = container.GetBlockBlobClient("shared");
            for (std::size_t call = 0; call < CallsPerThread; ++call)
            {
                BlockBlobClient& client = (call + t) % 2 == 0 ? shared : own;
                if (client.GetPropertiesAsync(boost::asio::use_future).get().has_value())
                {
                    ++succeeded;
                }
            }
        });
    }
    for (std::thread& thread : threads)
    {
        thread.join();
    }
    EXPECT_EQ(succeeded.load(), Threads * CallsPerThread);
}
