#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"

#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <benchmark/benchmark.h>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <new>
#include <span>
#include <sstream>
#include <string>
#include <vector>

namespace
{
    constexpr unsigned int HttpStatusCreated = 201U;
    // A prime period keeps the synthetic payload from lining up with any power-of-two block size.
    constexpr std::size_t PayloadBytePeriod = 251U;
    // Number of blocks the staged-upload benchmark splits its payload into.
    constexpr std::size_t BlocksPerPayload = 8U;

    // Scoped, global allocation-byte counter used by BmUploadFromAsyncAllocationBytes below to
    // prove that the internal UploadFromAsync block-staging path (Task 13, SetBodyView) allocates
    // roughly 1x the payload size (transport buffer only) rather than the 2-3x copies the
    // SetBody(BytesToString(...)) path would have incurred.
    [[nodiscard]] std::atomic<bool>& TrackAllocations() noexcept
    {
        static std::atomic<bool> value{false};
        return value;
    }

    [[nodiscard]] std::atomic<std::uint64_t>& AllocatedBytes() noexcept
    {
        static std::atomic<std::uint64_t> value{0};
        return value;
    }
} // namespace

void* operator new(std::size_t size)
{
    if (TrackAllocations().load(std::memory_order_relaxed))
    {
        AllocatedBytes().fetch_add(size, std::memory_order_relaxed);
    }
    void* pointer = std::malloc(size);
    if (pointer == nullptr)
    {
        throw std::bad_alloc();
    }
    return pointer;
}

void operator delete(void* pointer) noexcept
{
    std::free(pointer);
}

void operator delete(void* pointer, std::size_t /*unused*/) noexcept
{
    std::free(pointer);
}

namespace
{
    using AVEVA::HttpRequest;
    using AVEVA::HttpRequestOptions;
    using AVEVA::HttpResponse;
    using AVEVA::IHttpClient;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::UploadBlockBlobResult;

    class NullHttpClient final : public IHttpClient
    {
      public:
        executor_type get_executor() const override
        {
            return {m_context.get_executor()};
        }

        void SendAsyncErased(HttpRequest request, CompletionHandler completion, HttpRequestOptions options) override
        {
            static_cast<void>(options);
            m_totalBytes += request.GetBodySize();
            completion({}, HttpResponse{HttpStatusCreated, {{"ETag", "\"etag\""}}, {}});
        }

        [[nodiscard]] std::uint64_t TotalBytes() const noexcept
        {
            return m_totalBytes;
        }

        // Runs the completions the client posted to this executor.
        void Poll()
        {
            m_context.restart();
            m_context.poll();
        }

      private:
        mutable boost::asio::io_context m_context;
        std::uint64_t m_totalBytes = 0;
    };

    [[nodiscard]] std::vector<std::byte> MakePayload(std::size_t size)
    {
        std::vector<std::byte> payload(size);
        for (std::size_t index = 0; index < payload.size(); ++index)
        {
            payload.at(index) = static_cast<std::byte>(index % PayloadBytePeriod);
        }
        return payload;
    }

    void BmUploadAsyncThroughput(benchmark::State& state)
    {
        NullHttpClient httpClient;
        BlockBlobClient client{httpClient,
            BlobClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
                .ContainerName = "images",
                .BlobName = "payload.bin",
                .SasToken = "sv=2025-01-05&sig=fakesig"}};

        const std::vector<std::byte> payload = MakePayload(static_cast<std::size_t>(state.range(0)));

        for (auto _ : state)
        {
            bool completed = false;

            client.UploadAsync(std::span<const std::byte>{payload},
                [&completed](
                    std::expected<Response<UploadBlockBlobResult>, AVEVA::AzureClient::BlobStorageError> result)
            {
                benchmark::DoNotOptimize(result.has_value());
                if (result.has_value())
                {
                    benchmark::DoNotOptimize(result->RawResponse().GetStatus());
                }
                completed = true;
            });
            httpClient.Poll();

            benchmark::DoNotOptimize(completed);
            benchmark::ClobberMemory();
        }

        benchmark::DoNotOptimize(httpClient.TotalBytes());
        state.SetBytesProcessed(
            static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(payload.size()));
    }

    BENCHMARK(BmUploadAsyncThroughput)->Arg(1 << 20)->Arg(4 << 20)->Arg(16 << 20);

    // Proves Task 13's zero-copy internal block-upload path (UploadFromAsync -> UploadFromState::Pump
    // -> BlockBlobClient::StageBlockAsyncImplView, using HttpRequest::SetBodyView) allocates roughly 1x
    // the payload size rather than the 2-3x copies the public SetBody(BytesToString(...)) path incurs.
    void BmUploadFromAsyncAllocationBytes(benchmark::State& state)
    {
        NullHttpClient httpClient;
        BlockBlobClient client{httpClient,
            BlobClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
                .ContainerName = "images",
                .BlobName = "payload.bin",
                .SasToken = "sv=2025-01-05&sig=fakesig"}};

        const auto payloadSize = static_cast<std::size_t>(state.range(0));
        std::string payload(payloadSize, '\0');
        for (std::size_t index = 0; index < payload.size(); ++index)
        {
            payload.at(index) = static_cast<char>(index % PayloadBytePeriod);
        }

        AVEVA::AzureClient::UploadFromOptions options;
        options.BlockSize = std::max<std::size_t>(payloadSize / BlocksPerPayload, 1U);

        for (auto _ : state)
        {
            std::istringstream stream(payload);
            bool completed = false;

            AllocatedBytes().store(0, std::memory_order_relaxed);
            TrackAllocations().store(true, std::memory_order_relaxed);

            client.UploadFromAsync(stream,
                options,
                [&completed](
                    std::expected<Response<UploadBlockBlobResult>, AVEVA::AzureClient::BlobStorageError> result)
            {
                benchmark::DoNotOptimize(result.has_value());
                if (result.has_value())
                {
                    benchmark::DoNotOptimize(result->RawResponse().GetStatus());
                }
                completed = true;
            });

            httpClient.Poll();
            TrackAllocations().store(false, std::memory_order_relaxed);

            benchmark::DoNotOptimize(completed);
            benchmark::ClobberMemory();

            state.counters["BytesAllocatedPerPayloadByte"] =
                static_cast<double>(AllocatedBytes().load(std::memory_order_relaxed)) /
                static_cast<double>(payloadSize);
        }

        benchmark::DoNotOptimize(httpClient.TotalBytes());
        state.SetBytesProcessed(
            static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(payload.size()));
    }

    BENCHMARK(BmUploadFromAsyncAllocationBytes)->Arg(1 << 20)->Arg(4 << 20)->Arg(16 << 20);
} // namespace
