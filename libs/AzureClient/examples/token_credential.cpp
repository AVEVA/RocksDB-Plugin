// Shows token-credential authorization with a Microsoft Entra ID service principal.
//
//   AZURE_STORAGE_ACCOUNT_NAME      storage account name
//   AZURE_TENANT_ID                 Entra tenant
//   AZURE_SERVICE_PRINCIPAL_ID      application (client) id
//   AZURE_SERVICE_PRINCIPAL_SECRET  client secret (a secret: never log or commit it)
//   AZURE_BLOB_CONTAINER            container name
//   AZURE_CA_FILE                   (optional) PEM CA bundle
//
// Exits 0 without doing anything when the variables are not set. The credential is wrapped in a
// CachingTokenCredential so tokens are reused until shortly before they expire.
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/Credentials.hpp"
#include "AVEVA/AzureClient/ITokenCredential.hpp"
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <cstddef>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
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
    const std::string tenant = GetEnv("AZURE_TENANT_ID");
    const std::string clientId = GetEnv("AZURE_SERVICE_PRINCIPAL_ID");
    const std::string secret = GetEnv("AZURE_SERVICE_PRINCIPAL_SECRET");
    const std::string containerName = GetEnv("AZURE_BLOB_CONTAINER");
    if (account.empty() || tenant.empty() || clientId.empty() || secret.empty() || containerName.empty())
    {
        std::cout << "Set the AZURE_* variables listed at the top of this file to run the example.\n";
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

        // The credential borrows *httpClient, so it must not outlive it.
        auto credential = AVEVA::AzureClient::CachingTokenCredential::Create(
            std::make_shared<AVEVA::AzureClient::ClientSecretCredential>(*httpClient,
                AVEVA::AzureClient::ClientSecretCredentialOptions{.TenantId = tenant,
                    .ClientId = clientId,
                    .ClientSecret = secret}));

        AVEVA::AzureClient::BlobContainerClient container{*httpClient,
            AVEVA::AzureClient::BlobContainerClientOptions{.ServiceEndpoint =
                                                               "https://" + account + ".blob.core.windows.net",
                .ContainerName = containerName,
                .TokenCredential = credential}};

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
