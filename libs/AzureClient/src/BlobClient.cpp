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

    void BlobClient::ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options,
        ReleaseLeaseCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::ReleaseLeaseAsync(*m_httpClient, *m_target, std::move(options), std::move(completion), requestOptions);
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

} // namespace AVEVA::AzureClient
