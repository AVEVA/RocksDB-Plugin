// Shows per-request options (timeout) and cancellation through WithRequestOptions.
//
//   AZURE_BLOB_ENDPOINT   e.g. https://<account>.blob.core.windows.net
//   AZURE_BLOB_CONTAINER  container name
//   AZURE_BLOB_SAS        SAS token (a secret: never log or commit it)
//   AZURE_CA_FILE         (optional) PEM CA bundle
//
// The request is cancelled right after it is started, so it normally completes with an error.
// Exits 0 without doing anything when the variables are not set.
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/WithRequestOptions.hpp"
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
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
    const std::string endpoint = GetEnv("AZURE_BLOB_ENDPOINT");
    const std::string containerName = GetEnv("AZURE_BLOB_CONTAINER");
    const std::string sasToken = GetEnv("AZURE_BLOB_SAS");
    if (endpoint.empty() || containerName.empty() || sasToken.empty())
    {
        std::cout << "Set AZURE_BLOB_ENDPOINT, AZURE_BLOB_CONTAINER and AZURE_BLOB_SAS.\n";
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

        // The signal must outlive the request.
        boost::asio::cancellation_signal signal;
        AVEVA::HttpRequestOptions requestOptions;
        requestOptions.SetTimeout(std::chrono::seconds{10});
        requestOptions.SetCancellationSlot(signal.slot());

        auto future =
            container.ExistsAsync(AVEVA::AzureClient::WithRequestOptions(requestOptions, boost::asio::use_future));
        signal.emit(boost::asio::cancellation_type::terminal);
        context.run();

        const auto result = future.get();
        if (!result)
        {
            std::cout << "Request ended with error: " << result.error().Code.message() << '\n';
            return 0;
        }
        std::cout << "Request completed before cancellation took effect\n";
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}
