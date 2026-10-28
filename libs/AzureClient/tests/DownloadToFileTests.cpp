// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <gtest/gtest.h>

#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <ostream>
#include <streambuf>
#include <string>
#include <system_error>

namespace {
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobClientOptions;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::Models::DownloadBlobToResult;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RunAwaitable;

[[nodiscard]] BlobClientOptions BuildOptions() { return MakeBlobClientOptions("logs", "t06.txt"); }

} // namespace

TEST(T06_DownloadToFileTests, SuccessRenamesAtomic) {
    FakeHttpClient httpClient;
    // Prepare a single successful response containing "downloaded"
    httpClient.DefaultResponse() =
        HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "10"}}), "downloaded"};

    PageBlobClient client{httpClient, BuildOptions()};

    const AVEVA::AzureClient::Tests::ScopedTestDirectory scratch{"t06-success"};
    const std::filesystem::path path = scratch.Path() / "download-output.txt";

    // Create an existing target file to ensure rename overwrites atomically
    {
        std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
        ofs << "original";
    }

    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(path));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().BytesWritten, 10U);

    // Verify file content was replaced with downloaded data
    std::ifstream ifs(path, std::ios::binary);
    std::string const contents((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, "downloaded");
}

TEST(T06_DownloadToFileTests, DirectoryTargetReportsIsADirectory) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "10"}}), "downloaded"};
    PageBlobClient client{httpClient, BuildOptions()};

    const AVEVA::AzureClient::Tests::ScopedTestDirectory scratch{"t06-dir-target"};
    const std::filesystem::path dir = scratch.Path() / "target";
    std::filesystem::create_directories(dir);

    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(dir));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::is_a_directory));
    EXPECT_TRUE(std::filesystem::is_directory(dir));
}

TEST(T06_DownloadToFileTests, HttpFailureRemovesPartialAndLeavesTargetUntouched) {
    FakeHttpClient httpClient;
    // Simulate a transport failure for the request
    httpClient.EnqueueResponse(HttpResponse{}, std::make_error_code(std::errc::connection_reset));

    PageBlobClient client{httpClient, BuildOptions()};

    const AVEVA::AzureClient::Tests::ScopedTestDirectory scratch{"t06-failure"};
    const std::filesystem::path path = scratch.Path() / "download-output-failure.txt";

    // Create an existing target file to ensure it remains untouched on failure
    {
        std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
        ofs << "preserved";
    }

    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(path));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::connection_reset));

    // Ensure no partial files remain (look for filename.partial-*)
    bool foundPartial = false;
    for (const auto& p : std::filesystem::directory_iterator(scratch.Path())) {
        if (p.path().filename().string().starts_with("download-output-failure.txt.partial-")) {
            foundPartial = true;
            break;
        }
    }
    EXPECT_FALSE(foundPartial);

    // Target should remain with original contents
    std::ifstream ifs(path, std::ios::binary);
    std::string const contents((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, "preserved");
}
