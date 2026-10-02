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

    void BlobServiceClient::ListBlobContainersAsyncImpl(ListBlobContainersOptions options,
        ListBlobContainersCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        ListBlobContainersPageAsync(*m_httpClient,
            m_connection,
            std::move(options),
            std::move(completion),
            requestOptions);
    }

    void BlobServiceClient::ListBlobContainersPageAsync(IHttpClient& httpClient,
        const std::shared_ptr<const Private::ConnectionState>& connection,
        ListBlobContainersOptions options,
        ListBlobContainersCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        std::vector<std::pair<std::string, std::string>> queryParameters{{"comp", "list"}};
        if (!options.Prefix.empty())
        {
            queryParameters.emplace_back("prefix", options.Prefix);
        }
        if (!options.Marker.empty())
        {
            queryParameters.emplace_back("marker", options.Marker);
        }
        if (options.MaxResults.has_value())
        {
            queryParameters.emplace_back("maxresults", std::to_string(*options.MaxResults));
        }
        if (options.IncludeMetadata)
        {
            queryParameters.emplace_back("include", "metadata");
        }

        HttpRequest request =
            Private::BuildServiceRequest(*connection, HttpMethod::Get, Private::BuildQueryString(queryParameters));
        Private::SendAuthorizedRequestAsync(httpClient,
            *connection,
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::ListBlobContainersResult>(error,
                std::move(response),
                [](const HttpResponse& value)
            {
                return Private::ParseListBlobContainersResultXml(value.GetBody());
            },
                std::move(completion));
        },
            requestOptions);
    }

    BlobContainerClient BlobServiceClient::GetBlobContainerClient(std::string containerName) const
    {
        return BlobContainerClient{*m_httpClient, m_connection, std::move(containerName)};
    }

    void BlobServiceClient::ListBlobContainersAllAsyncImpl(ListBlobContainersOptions options,
        ListBlobContainersCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        std::string marker = options.Marker;
        const std::optional<std::size_t> maxItems = options.MaxItems;
        auto collector = std::make_shared<Private::PageCollector<Models::ListBlobContainersResult>>(
            [httpClient = m_httpClient,
                connection = m_connection,
                options = std::move(options),
                requestOptions = requestOptions](std::string pageMarker,
                ListBlobContainersCompletionHandler pageCompletion) mutable
        {
            ListBlobContainersOptions pageOptions = options;
            pageOptions.Marker = std::move(pageMarker);
            ListBlobContainersPageAsync(*httpClient,
                connection,
                std::move(pageOptions),
                std::move(pageCompletion),
                requestOptions);
        },
            [](Models::ListBlobContainersResult& accumulated, Models::ListBlobContainersResult page)
        {
            std::ranges::move(page.Containers, std::back_inserter(accumulated.Containers));
        },
            std::move(completion),
            [](const Models::ListBlobContainersResult& value)
        {
            return value.Containers.size();
        },
            maxItems);
        collector->Start(std::move(marker));
    }

    void BlobServiceClient::GetPropertiesAsyncImpl(GetPropertiesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request =
            Private::BuildServiceRequest(*m_connection, HttpMethod::Get, "restype=service&comp=properties");
        Private::SendAndParse<Models::BlobServiceProperties>(*m_httpClient,
            *m_connection,
            std::move(request),
            [](const HttpResponse& response)
        {
            return Private::ParseBlobServicePropertiesXml(response.GetBody());
        },
            std::move(completion),
            requestOptions);
    }

    void BlobServiceClient::GetAccountInfoAsyncImpl(GetAccountInfoCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request =
            Private::BuildServiceRequest(*m_connection, HttpMethod::Get, "restype=account&comp=properties");
        Private::SendAndParse<Models::AccountInfo>(*m_httpClient,
            *m_connection,
            std::move(request),
            Private::ParseAccountInfo,
            std::move(completion),
            requestOptions);
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

    void BlobServiceClient::FindBlobsByTagsAsyncImpl(const std::string& where,
        const FindBlobsByTagsOptions& options,
        FindBlobsByTagsCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (where.empty())
        {
            Private::PostCompletion(*m_httpClient,
                std::move(completion),
                Private::MakeError<Models::FindBlobsByTagsResult>(std::make_error_code(std::errc::invalid_argument),
                    "The tag filter expression must not be empty."));
            return;
        }
        HttpRequest request = Private::BuildServiceRequest(*m_connection,
            HttpMethod::Get,
            Private::BuildFindBlobsByTagsQuery(where, options));
        Private::SendAndParse<Models::FindBlobsByTagsResult>(*m_httpClient,
            *m_connection,
            std::move(request),
            [](const HttpResponse& response)
        {
            return Private::ParseFindBlobsByTagsResultXml(response.GetBody());
        },
            std::move(completion),
            requestOptions);
    }
} // namespace AVEVA::AzureClient
