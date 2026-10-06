#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>

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

namespace
{
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::DownloadBlobToResult;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;

    [[nodiscard]] BlobClientOptions BuildOptions()
    {
        return MakeBlobClientOptions("logs", "t06.txt");
    }

    void VerifyDownloadToPathSuccess(const std::expected<Response<DownloadBlobToResult>, BlobStorageError>& result,
        bool& invoked)
    {
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().BytesWritten, 10U);
        invoked = true;
    }

    void StartDownloadToPathAndVerifySuccess(PageBlobClient& client, const std::filesystem::path& path, bool& invoked)
    {
        client.DownloadToAsync(path,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            VerifyDownloadToPathSuccess(result, invoked);
        });
    }

    void VerifyDownloadToPathFailure(const std::expected<Response<DownloadBlobToResult>, BlobStorageError>& result,
        bool& invoked)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::connection_reset));
        invoked = true;
    }

    void StartDownloadToPathAndVerifyFailure(PageBlobClient& client, const std::filesystem::path& path, bool& invoked)
    {
        client.DownloadToAsync(path,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            VerifyDownloadToPathFailure(result, invoked);
        });
    }
} // namespace

TEST(T06_DownloadToFileTests, SuccessRenamesAtomic)
{
    FakeHttpClient httpClient;
    // Prepare a single successful response containing "downloaded"
    httpClient.DefaultResponse() =
        HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "10"}}), "downloaded"};

    PageBlobClient client{httpClient, BuildOptions()};

    const std::filesystem::path path = std::filesystem::temp_directory_path() / "t06-download-output.txt";

    // Create an existing target file to ensure rename overwrites atomically
    {
        std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
        ofs << "original";
    }

    bool invoked = false;
    StartDownloadToPathAndVerifySuccess(client, path, invoked);

    // Complete the in-flight HTTP response. FakeHttpClient may complete inline or defer the
    // completion onto its executor depending on CompleteInline, so Poll() first to run any
    // posted completion and only then check whether the callback has been invoked.
    httpClient.Poll();
    EXPECT_TRUE(invoked);

    // Verify file content was replaced with downloaded data
    std::ifstream ifs(path, std::ios::binary);
    std::string const contents((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, "downloaded");

    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

TEST(T06_DownloadToFileTests, DirectoryTargetReportsIsADirectory)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "10"}}), "downloaded"};
    PageBlobClient client{httpClient, BuildOptions()};

    const std::filesystem::path dir = std::filesystem::temp_directory_path() / "t06-download-dir-target";
    std::filesystem::create_directories(dir);

    bool invoked = false;
    client.DownloadToAsync(dir,
        [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
    {
        invoked = true;
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::is_a_directory));
    });
    httpClient.Poll();
    EXPECT_TRUE(invoked);
    EXPECT_TRUE(std::filesystem::is_directory(dir));

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
}

TEST(T06_DownloadToFileTests, HttpFailureRemovesPartialAndLeavesTargetUntouched)
{
    FakeHttpClient httpClient;
    // Simulate a transport failure for the request
    httpClient.EnqueueResponse(HttpResponse{}, std::make_error_code(std::errc::connection_reset));

    PageBlobClient client{httpClient, BuildOptions()};

    const std::filesystem::path path = std::filesystem::temp_directory_path() / "t06-download-output-failure.txt";

    // Create an existing target file to ensure it remains untouched on failure
    {
        std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
        ofs << "preserved";
    }

    bool invoked = false;
    StartDownloadToPathAndVerifyFailure(client, path, invoked);

    // Complete the scripted failure
    // FakeHttpClient completes synchronously for EnqueueResponse unless deferred; use CompleteNext if deferred
    // Here SendAsyncErased will invoke completion immediately.
    // No pending item expected; but ensure callback ran by polling executor
    httpClient.Poll();
    EXPECT_TRUE(invoked);

    // Ensure no partial files remain (look for filename.partial-*)
    bool foundPartial = false;
    for (const auto& p : std::filesystem::directory_iterator(std::filesystem::temp_directory_path()))
    {
        if (p.path().filename().string().starts_with("t06-download-output-failure.txt.partial-"))
        {
            foundPartial = true;
            break;
        }
    }
    EXPECT_FALSE(foundPartial);

    // Target should remain with original contents
    std::ifstream ifs(path, std::ios::binary);
    std::string const contents((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, "preserved");

    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

TEST(T06_DownloadToFileTests, StreamWriteFailureReportsIoError)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "4"}}), "data"};

    PageBlobClient client{httpClient, BuildOptions()};

    // Create a failing streambuf that refuses writes
    struct FailBuf : std::streambuf
    {
      protected:
        std::streamsize xsputn(const char* /*s*/, std::streamsize /*n*/) override
        {
            return 0;
        }

        int_type overflow(int_type /*ch*/) override
        {
            return traits_type::eof();
        }
    } failbuf;

    std::ostream failingStream(&failbuf);

    bool invoked = false;
    client.DownloadToAsync(failingStream,
        [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        // Expect an io_error transported
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::io_error));
        invoked = true;
    });

    // Complete the HTTP response so download writes are attempted. FakeHttpClient may complete
    // inline or defer onto its executor, so Poll() first to run any posted completion.
    httpClient.Poll();
    EXPECT_TRUE(invoked);
}
