#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "BlobRequestHelpers.hpp"
#include "TestFixtures.hpp"

#include <gtest/gtest.h>

#include <string>

namespace
{
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::Private::BuildBlobUrl;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
} // namespace

TEST(T07_ValidateEndpointTests, PathStyleServiceEndpoint_AppendsAccountPath)
{
    BlobClientOptions opts = MakeBlobClientOptions();
    opts.ServiceEndpoint = "http://127.0.0.1:10000/devstoreaccount1"; // Azurite-style
    opts.ContainerName = "mycontainer";
    opts.BlobName = "path/to/blob.txt";

    const std::string url = BuildBlobUrl(opts);
    // Expect the account path preserved before container/blob
    EXPECT_NE(url.find("/devstoreaccount1/mycontainer/path/to/blob.txt"), std::string::npos);
}
