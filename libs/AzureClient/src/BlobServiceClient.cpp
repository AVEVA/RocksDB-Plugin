#include <AVEVA/AzureClient/BlobServiceClient.hpp>

#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Models/BlobServiceModels.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient
{
    BlobServiceClient::BlobServiceClient(IHttpClient& httpClient, BlobServiceClientOptions options)
        : m_httpClient(&httpClient), m_connection(Private::MakeConnectionState(std::move(options)))
    {
    }

    const HttpRequestOptions& BlobServiceClient::GetDefaultRequestOptions() const noexcept
    {
        return m_connection->DefaultRequestOptions;
    }

    BlobContainerClient BlobServiceClient::GetBlobContainerClient(std::string containerName) const
    {
        return BlobContainerClient{*m_httpClient, m_connection, std::move(containerName)};
    }

    void BlobServiceClient::GetUserDelegationKeyAsyncImpl(GetUserDelegationKeyOptions options,
        GetUserDelegationKeyCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        const auto startsOn = options.StartsOn.value_or(std::chrono::system_clock::now());
        if (options.ExpiresOn == std::chrono::system_clock::time_point{} || options.ExpiresOn <= startsOn)
        {
            Private::PostCompletion(*m_httpClient,
                std::move(completion),
                Private::MakeError<Models::UserDelegationKey>(std::make_error_code(std::errc::invalid_argument),
                    "User delegation key ExpiresOn must be set and later than StartsOn."));
            return;
        }

        HttpRequest request =
            Private::BuildServiceRequest(*m_connection, HttpMethod::Post, "restype=service&comp=userdelegationkey");
        request.SetBody(R"(<?xml version="1.0" encoding="utf-8"?><KeyInfo><Start>)" +
                        Private::FormatIso8601Utc(startsOn) + "</Start><Expiry>" +
                        Private::FormatIso8601Utc(options.ExpiresOn) + "</Expiry></KeyInfo>");
        Private::SendAndParse<Models::UserDelegationKey>(*m_httpClient,
            *m_connection,
            std::move(request),
            [](const HttpResponse& response)
        {
            return Private::ParseUserDelegationKeyXml(response.GetBody());
        },
            std::move(completion),
            requestOptions);
    }

} // namespace AVEVA::AzureClient
