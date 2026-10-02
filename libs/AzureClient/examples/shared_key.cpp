// Shows Shared Key authorization: checks whether a container exists.
//
//   AZURE_STORAGE_ACCOUNT_NAME  storage account name
//   AZURE_STORAGE_ACCOUNT_KEY   base64 account key (a secret: never log or commit it)
//   AZURE_BLOB_CONTAINER        container name
//   AZURE_CA_FILE               (optional) PEM CA bundle
//
// Exits 0 without doing anything when the variables are not set.
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <AVEVA/HttpClient/HttpClientOptions.hpp>
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
        const char* value = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
        return value != nullptr ? std::string(value) : std::string{};
#endif
    }
} // namespace

int main()
{
    const std::string account = GetEnv("AZURE_STORAGE_ACCOUNT_NAME");
    const std::string key = GetEnv("AZURE_STORAGE_ACCOUNT_KEY");
    const std::string containerName = GetEnv("AZURE_BLOB_CONTAINER");
    if (account.empty() || key.empty() || containerName.empty())
    {
        std::cout << "Set AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY and AZURE_BLOB_CONTAINER.\n";
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
            AVEVA::AzureClient::BlobContainerClientOptions{.ServiceEndpoint =
                                                               "https://" + account + ".blob.core.windows.net",
                .ContainerName = containerName,
                .SharedKey = {.AccountName = account, .AccountKey = key}}};

        auto future = container.ExistsAsync(boost::asio::use_future);
        context.run();
        const auto result = future.get();
        std::cout << (result ? "Request succeeded\n" : "Request failed\n");
        return result ? 0 : 1;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}
