// Uploads a blob, downloads it again and prints it.
//
//   AZURE_BLOB_ENDPOINT   e.g. https://<account>.blob.core.windows.net
//   AZURE_BLOB_CONTAINER  an existing container
//   AZURE_BLOB_SAS        a SAS token with read/write permission on the container
//   AZURE_CA_FILE         (optional) PEM CA bundle; needed on Windows, where OpenSSL has no default trust store
//
// Exits 0 without doing anything when the variables are not set.
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlockBlobClient.hpp"
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <cstddef>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

namespace
{
    [[nodiscard]] std::string GetEnv(const char* name)
    {
#ifdef _MSC_VER
        // getenv_s avoids the caller-owned malloc'd buffer that _dupenv_s returns.
        std::size_t required = 0;
        if (getenv_s(&required, nullptr, 0, name) != 0 || required == 0)
        {
            return {};
        }

        std::string result(required, '\0');
        if (getenv_s(&required, result.data(), result.size(), name) != 0)
        {
            return {};
        }

        result.resize(required - 1);
        return result;
#else
        const char* value = std::getenv(name);
        return value != nullptr ? std::string(value) : std::string{};
#endif
    }

    void PrintError(const AVEVA::AzureClient::BlobStorageError& error)
    {
        std::cerr << "Error: " << error.Message << " (code=" << error.Code.message() << ", http=" << error.StatusCode
                  << ", service code=" << error.ErrorCode << ", request id=" << error.RequestId << ")\n";
        // Compare against portable conditions rather than strings; transport failures have StatusCode 0.
        if (error.Code == std::errc::timed_out)
        {
            std::cerr << "The request timed out; it is safe to retry idempotent operations.\n";
        }
    }

    // `container` (and the io_context and HTTP client behind it) must outlive this coroutine.
    boost::asio::awaitable<int> UploadAndDownload(AVEVA::AzureClient::BlobContainerClient& container)
    {
        AVEVA::AzureClient::BlockBlobClient blob = container.GetBlockBlobClient("quick-start.txt");

        // Omitting the completion token yields a deferred operation that can be co_awaited.
        auto uploaded = co_await blob.UploadAsync(std::string{"hello from aveva-azure-client"});
        if (!uploaded)
        {
            std::cerr << "Upload failed: ";
            PrintError(uploaded.error());
            co_return 1;
        }

        auto downloaded = co_await blob.DownloadAsync();
        if (!downloaded)
        {
            std::cerr << "Download failed: ";
            PrintError(downloaded.error());
            co_return 1;
        }

        std::cout << downloaded->Value().Content << '\n';
        co_return 0;
    }
} // namespace

int main()
{
    const std::string endpoint = GetEnv("AZURE_BLOB_ENDPOINT");
    const std::string containerName = GetEnv("AZURE_BLOB_CONTAINER");
    // The SAS token is a secret: never log it or commit it to source control.
    const std::string sasToken = GetEnv("AZURE_BLOB_SAS");
    if (endpoint.empty() || containerName.empty() || sasToken.empty())
    {
        std::cout << "Set AZURE_BLOB_ENDPOINT, AZURE_BLOB_CONTAINER and AZURE_BLOB_SAS to run the quick start.\n";
        return 0;
    }

    try
    {
        boost::asio::io_context context;
        AVEVA::HttpClientOptions httpOptions;
        if (const std::string caFile = GetEnv("AZURE_CA_FILE"); !caFile.empty())
        {
            httpOptions.SetCaFile(caFile);
        }
        const auto httpClient = AVEVA::IHttpClient::Create(context, httpOptions);

        AVEVA::AzureClient::BlobContainerClient container{*httpClient,
            AVEVA::AzureClient::BlobContainerClientOptions{.ServiceEndpoint = endpoint,
                .ContainerName = containerName,
                .SasToken = sasToken}};

        auto result = boost::asio::co_spawn(context, UploadAndDownload(container), boost::asio::use_future);
        context.run();
        return result.get();
    }
    catch (const std::exception& error)
    {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}
