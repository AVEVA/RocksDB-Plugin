// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlockBlobClient.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/PageBlobClient.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient
{
    namespace
    {
        [[nodiscard]] HttpRequest BuildCreateContainerRequest(const Private::ContainerTarget& target,
            const CreateBlobContainerOptions& options)
        {
            HttpRequest request = Private::BuildContainerRequest(target, HttpMethod::Put);
            Private::ApplyMetadata(request, options.Metadata);
            Private::ApplyBlobRequestConditions(request, options.Conditions);
            return request;
        }
    } // namespace

    BlobContainerClient::BlobContainerClient(IHttpClient& httpClient, const BlobContainerClientOptions& options)
        : m_httpClient(&httpClient),
          m_target(std::make_shared<const Private::ContainerTarget>(Private::MakeContainerTarget(options)))
    {
    }

    BlobContainerClient::BlobContainerClient(IHttpClient& httpClient,
        std::shared_ptr<const Private::ConnectionState> connection,
        std::string containerName)
        : m_httpClient(&httpClient), m_target(std::make_shared<const Private::ContainerTarget>(
                                         Private::MakeContainerTarget(std::move(connection), std::move(containerName))))
    {
    }

    const HttpRequestOptions& BlobContainerClient::GetDefaultRequestOptions() const noexcept
    {
        return m_target->Connection->DefaultRequestOptions;
    }

    void BlobContainerClient::CreateAsyncImpl(const CreateBlobContainerOptions& options,
        CreateCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SendAndParse<Models::CreateBlobContainerResult>(*m_httpClient,
            *m_target,
            BuildCreateContainerRequest(*m_target, options),
            Private::ParseETagAndLastModified<Models::CreateBlobContainerResult>,
            std::move(completion),
            requestOptions);
    }

    void BlobContainerClient::ListBlobsAsyncImpl(ListBlobsOptions options,
        ListBlobsCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        ListBlobsPageAsync(*m_httpClient, m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobContainerClient::ListBlobsPageAsync(IHttpClient& httpClient,
        const std::shared_ptr<const Private::ContainerTarget>& target,
        ListBlobsOptions options,
        ListBlobsCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        std::vector<std::pair<std::string, std::string>> queryParameters{{"comp", "list"}};
        if (!options.Prefix.empty())
        {
            queryParameters.emplace_back("prefix", options.Prefix);
        }
        if (!options.Delimiter.empty())
        {
            queryParameters.emplace_back("delimiter", options.Delimiter);
        }
        if (!options.Marker.empty())
        {
            queryParameters.emplace_back("marker", options.Marker);
        }
        if (options.MaxResults.has_value())
        {
            queryParameters.emplace_back("maxresults", std::to_string(*options.MaxResults));
        }
        std::string include;
        for (const auto& [enabled, name] : {
                 std::pair{options.IncludeCopy, "copy"},
                 std::pair{options.IncludeDeleted, "deleted"},
                 std::pair{options.IncludeMetadata, "metadata"},
                 std::pair{options.IncludeSnapshots, "snapshots"},
                 std::pair{options.IncludeTags, "tags"},
                 std::pair{options.IncludeUncommittedBlobs, "uncommittedblobs"},
                 std::pair{options.IncludeVersions, "versions"},
             })
        {
            if (enabled)
            {
                include += include.empty() ? "" : ",";
                include += name;
            }
        }
        if (!include.empty())
        {
            queryParameters.emplace_back("include", std::move(include));
        }

        HttpRequest request =
            Private::BuildContainerRequest(*target, HttpMethod::Get, Private::BuildQueryString(queryParameters));
        Private::SendAuthorizedRequestAsync(httpClient,
            *target,
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::ListBlobsResult>(error,
                std::move(response),
                [](const HttpResponse& value)
            {
                return Private::ParseListBlobsResultXml(value.GetBody());
            },
                std::move(completion));
        },
            requestOptions);
    }

    void BlobContainerClient::CreateIfNotExistsAsyncImpl(const CreateBlobContainerOptions& options,
        CreateCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SendAndParse<Models::CreateBlobContainerResult>(*m_httpClient,
            *m_target,
            BuildCreateContainerRequest(*m_target, options),
            Private::ParseETagAndLastModified<Models::CreateBlobContainerResult>,
            Private::SuppressErrors<Models::CreateBlobContainerResult>(std::move(completion),
                {BlobStorageErrorCode::ContainerAlreadyExists, BlobStorageErrorCode::ConditionNotMet}),
            requestOptions);
    }

    std::unique_ptr<BlobClient> BlobContainerClient::GetBlobClient(std::string blobName) const
    {
        return std::unique_ptr<BlobClient>(new BlobClient{*m_httpClient,
            std::make_shared<const Private::BlobTarget>(
                Private::MakeBlobTarget(m_target->Connection, m_target->ContainerName, std::move(blobName)))});
    }

    std::unique_ptr<BlockBlobClient> BlobContainerClient::GetBlockBlobClient(std::string blobName) const
    {
        return std::unique_ptr<BlockBlobClient>(new BlockBlobClient{*m_httpClient,
            std::make_shared<const Private::BlobTarget>(
                Private::MakeBlobTarget(m_target->Connection, m_target->ContainerName, std::move(blobName)))});
    }

    std::unique_ptr<PageBlobClient> BlobContainerClient::GetPageBlobClient(std::string blobName) const
    {
        return std::unique_ptr<PageBlobClient>(new PageBlobClient{*m_httpClient,
            std::make_shared<const Private::BlobTarget>(
                Private::MakeBlobTarget(m_target->Connection, m_target->ContainerName, std::move(blobName)))});
    }

} // namespace AVEVA::AzureClient
