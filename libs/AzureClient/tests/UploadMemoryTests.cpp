#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include "TestHooks.hpp"
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <cstddef>
#include <expected>
#include <gtest/gtest.h>
#include <sstream>

using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlockBlobClient;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::UploadFromOptions;
using AVEVA::AzureClient::Models::UploadBlockBlobResult;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;

TEST(T10_PeakMemoryTest, PeakBytesBoundedByConcurrencyForIstreamUploads)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    // Build a stream large enough to require multiple blocks
    std::string data;
    for (int i = 0; i < 100; ++i)
    {
        data += std::string(1024, 'A'); // ~100 KiB
    }
    std::istringstream stream(data);

    UploadFromOptions options;
    options.BlockSize = 4096U; // 4 KiB blocks
    options.Concurrency = 4U;

    // Reset internal counters before starting.
    AVEVA::AzureClient::TestHooks::ResetPeakUploadBufferBytes();

    client.UploadFromAsync(stream,
        options,
        [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
    {
        // No-op; we'll complete requests manually in the fake client during the test harness.
    });

    // At this point the helper should have allocated buffers; peak bytes should be bounded by Concurrency * BlockSize
    const std::size_t peak = AVEVA::AzureClient::TestHooks::GetPeakUploadBufferBytes();
    EXPECT_LE(peak, options.Concurrency * options.BlockSize);
}
