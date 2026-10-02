#include <AVEVA/AzureClient/AppendBlobClient.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace AVEVA::AzureClient
{
    namespace
    {
        constexpr std::string_view AppendBlobTypeValue = "AppendBlob";

        void AddOptionalUnsignedHeader(HttpRequest& request,
            std::string_view name,
            const std::optional<std::uint64_t>& value)
        {
            if (value.has_value())
            {
                Private::AddHeader(request, name, std::to_string(*value));
            }
        }

        [[nodiscard]] HttpRequest BuildCreateRequest(const Private::BlobTarget& target,
            const CreateAppendBlobOptions& options)
        {
            HttpRequest request = Private::BuildBlobRequest(target, HttpMethod::Put);
            Private::AddHeader(request, Private::XMsBlobTypeHeaderName, AppendBlobTypeValue);
            Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
            Private::ApplyMetadata(request, options.Metadata);
            Private::ApplyBlobRequestConditions(request, options.Conditions);
            return request;
        }
    } // namespace

    AppendBlobClient::AppendBlobClient(IHttpClient& httpClient, const BlobClientOptions& options)
        : BlobClient(httpClient, options)
    {
    }

    AppendBlobClient AppendBlobClient::WithSnapshot(std::string snapshot) const
    {
        Private::BlobTarget target = Target();
        target.Snapshot = std::move(snapshot);
        target.VersionId.clear();
        return AppendBlobClient{HttpClient(), std::make_shared<const Private::BlobTarget>(std::move(target))};
    }

    AppendBlobClient AppendBlobClient::WithVersionId(std::string versionId) const
    {
        Private::BlobTarget target = Target();
        target.VersionId = std::move(versionId);
        target.Snapshot.clear();
        return AppendBlobClient{HttpClient(), std::make_shared<const Private::BlobTarget>(std::move(target))};
    }

    AppendBlobClient::AppendBlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target)
        : BlobClient(httpClient, std::move(target))
    {
    }

    void AppendBlobClient::CreateAsyncImpl(const CreateAppendBlobOptions& options,
        CreateCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SendAndParse<Models::CreateAppendBlobResult>(HttpClient(),
            Target(),
            BuildCreateRequest(Target(), options),
            Private::ParseETagAndLastModified<Models::CreateAppendBlobResult>,
            std::move(completion),
            requestOptions);
    }

    void AppendBlobClient::AppendBlockBytesAsyncImpl(std::span<const std::byte> content,
        const AppendBlockOptions& options,
        AppendBlockCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put, "comp=appendblock");
        request.SetBody(Private::BytesToString(content));
        SendAppendBlockRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    void AppendBlobClient::AppendBlockStringAsyncImpl(std::string content,
        const AppendBlockOptions& options,
        AppendBlockCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put, "comp=appendblock");
        request.SetBody(std::move(content));
        SendAppendBlockRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    void AppendBlobClient::SendAppendBlockRequest(HttpRequest request,
        const AppendBlockOptions& options,
        AppendBlockCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        AddOptionalUnsignedHeader(request, Private::XMsBlobConditionAppendPosHeaderName, options.IfAppendPositionEqual);
        AddOptionalUnsignedHeader(request,
            Private::XMsBlobConditionMaxSizeHeaderName,
            options.IfMaxSizeLessThanOrEqual);
        Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
        Private::SendAndParse<Models::AppendBlockResult>(HttpClient(),
            Target(),
            std::move(request),
            Private::ParseAppendBlockResult,
            std::move(completion),
            requestOptions);
    }

    void AppendBlobClient::CreateIfNotExistsAsyncImpl(CreateAppendBlobOptions options,
        CreateCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (options.Conditions.IfNoneMatch.empty())
        {
            options.Conditions.IfNoneMatch = "*";
        }
        Private::SendAndParse<Models::CreateAppendBlobResult>(HttpClient(),
            Target(),
            BuildCreateRequest(Target(), options),
            Private::ParseETagAndLastModified<Models::CreateAppendBlobResult>,
            Private::SuppressErrors<Models::CreateAppendBlobResult>(std::move(completion),
                {BlobStorageErrorCode::BlobAlreadyExists, BlobStorageErrorCode::ConditionNotMet}),
            requestOptions);
    }

    void AppendBlobClient::SealAsyncImpl(const SealAppendBlobOptions& options,
        SealCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put, "comp=seal");
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        AddOptionalUnsignedHeader(request, Private::XMsBlobConditionAppendPosHeaderName, options.IfAppendPositionEqual);
        Private::SendAndParse<Models::SealAppendBlobResult>(HttpClient(),
            Target(),
            std::move(request),
            Private::ParseSealAppendBlobResult,
            std::move(completion),
            requestOptions);
    }
} // namespace AVEVA::AzureClient
