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
        using Private::FindHeaderValue;
        using Private::IStartsWith;
        using Private::ParseBoolHeader;
        using Private::ParseLeaseDurationType;
        using Private::ParseLeaseState;
        using Private::ParseLeaseStatus;
        using Private::ParsePublicAccessType;
        using Private::XMsMetaHeaderPrefix;

        constexpr std::string_view XMsBlobPublicAccessHeaderName = "x-ms-blob-public-access";
        constexpr std::string_view XMsLeaseStatusHeaderName = "x-ms-lease-status";
        constexpr std::string_view XMsLeaseStateHeaderName = "x-ms-lease-state";
        constexpr std::string_view XMsLeaseDurationHeaderName = "x-ms-lease-duration";
        constexpr std::string_view XMsHasImmutabilityPolicyHeaderName = "x-ms-has-immutability-policy";
        constexpr std::string_view XMsHasLegalHoldHeaderName = "x-ms-has-legal-hold";
        constexpr std::string_view XMsDefaultEncryptionScopeHeaderName = "x-ms-default-encryption-scope";
        constexpr std::string_view XMsDenyEncryptionScopeOverrideHeaderName = "x-ms-deny-encryption-scope-override";

        [[nodiscard]] Models::DeleteBlobContainerResult ParseDeleteBlobContainerResult(const HttpResponse& /*unused*/)
        {
            return {};
        }

        [[nodiscard]] Models::BlobContainerProperties ParseBlobContainerProperties(const HttpResponse& response)
        {
            Models::BlobContainerProperties properties;
            Private::ApplyETagAndLastModified(properties, response);
            properties.AccessType = ParsePublicAccessType(FindHeaderValue(response, XMsBlobPublicAccessHeaderName));
            properties.HasImmutabilityPolicy =
                ParseBoolHeader(FindHeaderValue(response, XMsHasImmutabilityPolicyHeaderName));
            properties.HasLegalHold = ParseBoolHeader(FindHeaderValue(response, XMsHasLegalHoldHeaderName));
            properties.Status = ParseLeaseStatus(FindHeaderValue(response, XMsLeaseStatusHeaderName));
            properties.State = ParseLeaseState(FindHeaderValue(response, XMsLeaseStateHeaderName));
            properties.DurationType = ParseLeaseDurationType(FindHeaderValue(response, XMsLeaseDurationHeaderName));
            properties.DefaultEncryptionScope =
                std::string{FindHeaderValue(response, XMsDefaultEncryptionScopeHeaderName)};
            properties.PreventEncryptionScopeOverride =
                ParseBoolHeader(FindHeaderValue(response, XMsDenyEncryptionScopeOverrideHeaderName));

            for (const auto& header : response.GetHeaders())
            {
                const std::string_view name = header.GetName();
                if (IStartsWith(name, XMsMetaHeaderPrefix))
                {
                    properties.Metadata[std::string{name.substr(XMsMetaHeaderPrefix.size())}] = header.GetValue();
                }
            }

            return properties;
        }

        [[nodiscard]] HttpRequest BuildCreateContainerRequest(const Private::ContainerTarget& target,
            const CreateBlobContainerOptions& options)
        {
            HttpRequest request = Private::BuildContainerRequest(target, HttpMethod::Put);
            Private::ApplyMetadata(request, options.Metadata);
            Private::ApplyBlobRequestConditions(request, options.Conditions);
            return request;
        }

        [[nodiscard]] HttpRequest BuildDeleteContainerRequest(const Private::ContainerTarget& target,
            const DeleteBlobContainerOptions& options)
        {
            HttpRequest request = Private::BuildContainerRequest(target, HttpMethod::Delete);
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

    void BlobContainerClient::DeleteAsyncImpl(const DeleteBlobContainerOptions& options,
        DeleteCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SendAndParse<Models::DeleteBlobContainerResult>(*m_httpClient,
            *m_target,
            BuildDeleteContainerRequest(*m_target, options),
            ParseDeleteBlobContainerResult,
            std::move(completion),
            requestOptions);
    }

    void BlobContainerClient::GetPropertiesAsyncImpl(const GetBlobContainerPropertiesOptions& options,
        GetPropertiesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildContainerRequest(*m_target, HttpMethod::Head);
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAuthorizedRequestAsync(*m_httpClient,
            *m_target,
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::BlobContainerProperties>(error,
                std::move(response),
                ParseBlobContainerProperties,
                std::move(completion));
        },
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

    void BlobContainerClient::ListBlobsAllAsyncImpl(ListBlobsOptions options,
        ListBlobsCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        std::string marker = options.Marker;
        const std::optional<std::size_t> maxItems = options.MaxItems;
        auto collector = std::make_shared<Private::PageCollector<Models::ListBlobsResult>>(
            [httpClient = m_httpClient,
                target = m_target,
                options = std::move(options),
                requestOptions = requestOptions](std::string pageMarker,
                ListBlobsCompletionHandler pageCompletion) mutable
        {
            ListBlobsOptions pageOptions = options;
            pageOptions.Marker = std::move(pageMarker);
            ListBlobsPageAsync(*httpClient, target, std::move(pageOptions), std::move(pageCompletion), requestOptions);
        },
            [](Models::ListBlobsResult& accumulated, Models::ListBlobsResult page)
        {
            std::ranges::move(page.Blobs, std::back_inserter(accumulated.Blobs));
            std::ranges::move(page.BlobPrefixes, std::back_inserter(accumulated.BlobPrefixes));
        },
            std::move(completion),
            [](const Models::ListBlobsResult& value)
        {
            return value.Blobs.size() + value.BlobPrefixes.size();
        },
            maxItems);
        collector->Start(std::move(marker));
    }

    void BlobContainerClient::ExistsAsyncImpl(ExistsCompletionHandler completion, HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildContainerRequest(*m_target, HttpMethod::Head);
        Private::SendAuthorizedRequestAsync(*m_httpClient,
            *m_target,
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::RequestFailure failure = Private::DetermineBlobStorageFailure(error, response);
            bool exists = false;
            if (!failure.Error)
            {
                exists = true;
            }
            else if (failure.Error == BlobStorageErrorCode::ContainerNotFound ||
                     failure.Error == BlobStorageErrorCode::ResourceNotFound)
            {
                failure.Error = {};
            }
            completion(Private::ToExpected(failure.Error,
                Response<bool>{exists, std::move(response), std::move(failure.Details)}));
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

    void BlobContainerClient::DeleteIfExistsAsyncImpl(const DeleteBlobContainerOptions& options,
        DeleteCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SendAndParse<Models::DeleteBlobContainerResult>(*m_httpClient,
            *m_target,
            BuildDeleteContainerRequest(*m_target, options),
            ParseDeleteBlobContainerResult,
            Private::SuppressErrors<Models::DeleteBlobContainerResult>(std::move(completion),
                {BlobStorageErrorCode::ContainerNotFound, BlobStorageErrorCode::ResourceNotFound}),
            requestOptions);
    }

    BlobClient BlobContainerClient::GetBlobClient(std::string blobName) const
    {
        return BlobClient{*m_httpClient,
            std::make_shared<const Private::BlobTarget>(
                Private::MakeBlobTarget(m_target->Connection, m_target->ContainerName, std::move(blobName)))};
    }

    BlockBlobClient BlobContainerClient::GetBlockBlobClient(std::string blobName) const
    {
        return BlockBlobClient{*m_httpClient,
            std::make_shared<const Private::BlobTarget>(
                Private::MakeBlobTarget(m_target->Connection, m_target->ContainerName, std::move(blobName)))};
    }

    PageBlobClient BlobContainerClient::GetPageBlobClient(std::string blobName) const
    {
        return PageBlobClient{*m_httpClient,
            std::make_shared<const Private::BlobTarget>(
                Private::MakeBlobTarget(m_target->Connection, m_target->ContainerName, std::move(blobName)))};
    }

    void BlobContainerClient::AcquireLeaseAsyncImpl(AcquireLeaseOptions options,
        AcquireLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::AcquireLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobContainerClient::RenewLeaseAsyncImpl(RenewLeaseOptions options,
        RenewLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::RenewLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobContainerClient::ChangeLeaseAsyncImpl(ChangeLeaseOptions options,
        ChangeLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::ChangeLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobContainerClient::ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options,
        ReleaseLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::ReleaseLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobContainerClient::BreakLeaseAsyncImpl(BreakLeaseOptions options,
        BreakLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::BreakLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobContainerClient::FindBlobsByTagsAsyncImpl(const std::string& where,
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
        HttpRequest request = Private::BuildContainerRequest(*m_target,
            HttpMethod::Get,
            Private::BuildFindBlobsByTagsQuery(where, options));
        Private::SendAndParse<Models::FindBlobsByTagsResult>(*m_httpClient,
            *m_target,
            std::move(request),
            [](const HttpResponse& response)
        {
            return Private::ParseFindBlobsByTagsResultXml(response.GetBody());
        },
            std::move(completion),
            requestOptions);
    }
} // namespace AVEVA::AzureClient
