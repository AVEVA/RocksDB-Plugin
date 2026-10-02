#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <filesystem>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient
{
    namespace
    {
        const Private::BlobTarget MakeValidatedTarget(const BlobClientOptions& options)
        {
            if (!options.Snapshot.empty() && !options.VersionId.empty())
            {
                throw std::invalid_argument("Snapshot and VersionId are mutually exclusive.");
            }
            return Private::MakeBlobTarget(options);
        }
    } // namespace

    BlobClient::BlobClient(IHttpClient& httpClient, const BlobClientOptions& options)
        : m_httpClient(&httpClient), m_target(std::make_shared<const Private::BlobTarget>(MakeValidatedTarget(options)))
    {
    }

    BlobClient::BlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target)
        : m_httpClient(&httpClient), m_target(std::move(target))
    {
    }

    const HttpRequestOptions& BlobClient::GetDefaultRequestOptions() const noexcept
    {
        return m_target->Connection->DefaultRequestOptions;
    }

    void BlobClient::DownloadAsyncImpl(DownloadBlobOptions options,
        DownloadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::DownloadBlobAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobClient::DownloadToStreamAsyncImpl(std::ostream& stream,
        DownloadToOptions options,
        DownloadToCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::DownloadBlobToAsync(*m_httpClient,
            *m_target,
            std::move(options),
            stream,
            std::move(completion),
            requestOptions);
    }

    void BlobClient::DownloadToFileAsyncImpl(const std::filesystem::path& path,
        DownloadToOptions options,
        DownloadToCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::DownloadBlobToFileAsync(*m_httpClient,
            *m_target,
            std::move(options),
            path,
            std::move(completion),
            requestOptions);
    }

    void BlobClient::DownloadRangeToStreamAsyncImpl(std::ostream& stream,
        DownloadBlobOptions options,
        DownloadToCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        DownloadToStreamAsyncImpl(stream,
            Private::ToDownloadToOptions(std::move(options)),
            std::move(completion),
            requestOptions);
    }

    void BlobClient::DownloadRangeToFileAsyncImpl(const std::filesystem::path& path,
        DownloadBlobOptions options,
        DownloadToCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        DownloadToFileAsyncImpl(path,
            Private::ToDownloadToOptions(std::move(options)),
            std::move(completion),
            requestOptions);
    }

    void BlobClient::DeleteAsyncImpl(const DeleteBlobOptions& options,
        DeleteCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::DeleteBlobAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    void BlobClient::DeleteIfExistsAsyncImpl(const DeleteBlobOptions& options,
        DeleteCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::DeleteBlobAsync(*m_httpClient,
            *m_target,
            options,
            Private::SuppressErrors<Models::DeleteBlobResult>(std::move(completion),
                {BlobStorageErrorCode::BlobNotFound, BlobStorageErrorCode::ResourceNotFound}),
            requestOptions);
    }

    void BlobClient::GetPropertiesAsyncImpl(const GetBlobPropertiesOptions& options,
        GetPropertiesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::GetBlobPropertiesAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    void BlobClient::ExistsAsyncImpl(ExistsCompletionHandler completion, HttpRequestOptions requestOptions)
    {
        Private::ExistsBlobAsync(*m_httpClient, *m_target, std::move(completion), requestOptions);
    }

    void BlobClient::SetMetadataAsyncImpl(const SetBlobMetadataOptions& options,
        SetMetadataCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SetBlobMetadataAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    void BlobClient::SetHttpHeadersAsyncImpl(const SetBlobHttpHeadersOptions& options,
        SetHttpHeadersCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SetBlobHttpHeadersAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    void BlobClient::SetAccessTierAsyncImpl(const SetBlobAccessTierOptions& options,
        SetAccessTierCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SetBlobAccessTierAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    void BlobClient::StartCopyFromUriAsyncImpl(const std::string& sourceUri,
        const StartCopyFromUriOptions& options,
        StartCopyFromUriCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::StartBlobCopyFromUriAsync(*m_httpClient,
            *m_target,
            sourceUri,
            options,
            std::move(completion),
            requestOptions);
    }

    void BlobClient::CopyFromUriAsyncImpl(const std::string& sourceUri,
        const CopyFromUriOptions& options,
        CopyFromUriCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::CopyBlobFromUriAsync(*m_httpClient,
            *m_target,
            sourceUri,
            options,
            std::move(completion),
            requestOptions);
    }

    void BlobClient::AbortCopyFromUriAsyncImpl(const std::string& copyId,
        const AbortCopyFromUriOptions& options,
        AbortCopyFromUriCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::AbortCopyBlobFromUriAsync(*m_httpClient,
            *m_target,
            copyId,
            options,
            std::move(completion),
            requestOptions);
    }

    void BlobClient::SnapshotAsyncImpl(const SnapshotBlobOptions& options,
        SnapshotCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SnapshotBlobAsync(*m_httpClient, *m_target, options, std::move(completion), requestOptions);
    }

    void BlobClient::AcquireLeaseAsyncImpl(AcquireLeaseOptions options,
        AcquireLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::AcquireLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobClient::RenewLeaseAsyncImpl(RenewLeaseOptions options,
        RenewLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::RenewLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobClient::ChangeLeaseAsyncImpl(ChangeLeaseOptions options,
        ChangeLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::ChangeLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobClient::ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options,
        ReleaseLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::ReleaseLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    void BlobClient::BreakLeaseAsyncImpl(BreakLeaseOptions options,
        BreakLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::BreakLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
    }

    BlobClient BlobClient::WithSnapshot(std::string snapshot) const
    {
        Private::BlobTarget target = *m_target;
        target.Snapshot = std::move(snapshot);
        target.VersionId.clear();
        return BlobClient{*m_httpClient, std::make_shared<const Private::BlobTarget>(std::move(target))};
    }

    BlobClient BlobClient::WithVersionId(std::string versionId) const
    {
        Private::BlobTarget target = *m_target;
        target.VersionId = std::move(versionId);
        target.Snapshot.clear();
        return BlobClient{*m_httpClient, std::make_shared<const Private::BlobTarget>(std::move(target))};
    }

    void BlobClient::GetTagsAsyncImpl(const GetBlobTagsOptions& options,
        GetTagsCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(*m_target, HttpMethod::Get, "comp=tags");
        Private::AddHeaderIfNotEmpty(request, Private::XMsLeaseIdHeaderName, options.LeaseId);
        Private::AddHeaderIfNotEmpty(request, Private::XMsIfTagsHeaderName, options.TagConditions);
        Private::SendAndParse<Models::GetBlobTagsResult>(*m_httpClient,
            *m_target,
            std::move(request),
            [](const HttpResponse& response)
        {
            return Private::ParseGetBlobTagsResultXml(response.GetBody());
        },
            std::move(completion),
            requestOptions);
    }

    void BlobClient::SetTagsAsyncImpl(const Models::BlobTags& tags,
        const SetBlobTagsOptions& options,
        SetTagsCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (std::string problem = Private::ValidateBlobTags(tags); !problem.empty())
        {
            Private::PostCompletion(*m_httpClient,
                std::move(completion),
                Private::MakeError<Models::SetBlobTagsResult>(std::make_error_code(std::errc::invalid_argument),
                    std::move(problem)));
            return;
        }
        HttpRequest request = Private::BuildBlobRequest(*m_target, HttpMethod::Put, "comp=tags");
        Private::AddHeader(request, Private::ContentTypeHeaderName, "application/xml; charset=UTF-8");
        Private::AddHeaderIfNotEmpty(request, Private::XMsLeaseIdHeaderName, options.LeaseId);
        Private::AddHeaderIfNotEmpty(request, Private::XMsIfTagsHeaderName, options.TagConditions);
        Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, {});
        request.SetBody(Private::BuildBlobTagsXml(tags));
        Private::SendAndParse<Models::SetBlobTagsResult>(*m_httpClient,
            *m_target,
            std::move(request),
            [](const HttpResponse& response)
        {
            return Models::SetBlobTagsResult{
                .RequestId = std::string{Private::FindHeaderValue(response, Private::XMsRequestIdHeaderName)}};
        },
            std::move(completion),
            requestOptions);
    }

    void BlobClient::UndeleteAsyncImpl(UndeleteCompletionHandler completion, HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(*m_target, HttpMethod::Put, "comp=undelete");
        Private::SendAndParse<Models::UndeleteBlobResult>(*m_httpClient,
            *m_target,
            std::move(request),
            [](const HttpResponse& response)
        {
            return Models::UndeleteBlobResult{
                .RequestId = std::string{Private::FindHeaderValue(response, Private::XMsRequestIdHeaderName)}};
        },
            std::move(completion),
            requestOptions);
    }
} // namespace AVEVA::AzureClient
