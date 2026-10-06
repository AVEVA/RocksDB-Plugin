// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "IntegrationTestHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <boost/asio/use_future.hpp>
#include <boost/log/trivial.hpp>

#include <algorithm>
#include <cstring>
using namespace boost::log::trivial;

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Testing {
TestIoContext::TestIoContext(std::size_t threadCount) : m_workGuard(boost::asio::make_work_guard(m_context)) {
    m_threads.reserve(threadCount);
    for (std::size_t i = 0; i < threadCount; ++i) {
        m_threads.emplace_back([this]() { m_context.run(); });
    }
}

TestIoContext::~TestIoContext() {
    m_workGuard.reset();
    m_context.stop();
    for (auto& thread : m_threads) {
        thread.join();
    }
}

std::optional<Models::ServicePrincipalStorageInfo> LoadAzureCredentialsFromEnvironment() {
    const char* spId = std::getenv("AZURE_SERVICE_PRINCIPAL_ID");
    const char* spSecret = std::getenv("AZURE_SERVICE_PRINCIPAL_SECRET");
    const char* tenant = std::getenv("AZURE_TENANT_ID");
    const char* storageAccountName = std::getenv("AZURE_STORAGE_ACCOUNT_NAME");
    const char* container = std::getenv("AZURE_TEST_CONTAINER");

    if (!spId || !spSecret || !storageAccountName) {
        return std::nullopt;
    }

    std::string tenantId = tenant ? tenant : "";
    std::string storageAccountUrl = "https://" + std::string(storageAccountName) + ".blob.core.windows.net/";
    std::string containerName = container ? container : "aveva-rocksdb-plugin-integration-tests";

    return Models::ServicePrincipalStorageInfo(containerName, // Using container name as dbName
                                               storageAccountUrl, spId, spSecret, tenantId);
}

std::string GenerateRandomBlobName(const std::string& prefix) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(100000, 999999);
    return prefix + "-" + std::to_string(dis(gen)) + ".blob";
}

bool IsAuthenticationError(const std::exception& e) {
    std::string errorMsg = e.what();
    return errorMsg.find("invalid_client") != std::string::npos || errorMsg.find("AADSTS") != std::string::npos ||
           errorMsg.find("Unauthorized") != std::string::npos || errorMsg.find("expired") != std::string::npos;
}

void AzureIntegrationTestBase::SetUp() {
    m_credentials = LoadAzureCredentialsFromEnvironment();
    if (!m_credentials) {
        GTEST_SKIP() << "Azure credentials not found in environment variables. "
                     << "Set AZURE_SERVICE_PRINCIPAL_ID, AZURE_SERVICE_PRINCIPAL_SECRET, and "
                        "AZURE_STORAGE_ACCOUNT_NAME to run integration tests.";
    }

    m_blobName = GenerateRandomBlobName(GetBlobNamePrefix());
    m_containerPrefix = StorageAccount::UniquePrefix(m_credentials->GetStorageAccountUrl(), m_credentials->GetDbName());
    m_logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

    CreateContainerClient();
}

void AzureIntegrationTestBase::TearDown() { CleanupBlob(); }

void AzureIntegrationTestBase::CreateContainerClient() {
    try {
        m_runtime = std::make_shared<ClientRuntime>(m_ioContext.Get());
        const auto serviceClient = BlobHelpers::CreateServiceClient(m_runtime, *m_credentials);

        m_containerClient = std::make_shared<AzureClient::BlobContainerClient>(
            serviceClient.GetBlobContainerClient(m_credentials->GetDbName()));

        // Create container if it doesn't exist - this will test authentication
        TryCreateContainer();
    } catch (const RequestFailedException& e) {
        HandleAuthenticationError(e);
        FAIL() << "Failed to connect to Azure: " << e.what();
    } catch (const std::exception& e) {
        if (IsAuthenticationError(e)) {
            FAIL() << "Azure authentication failed: " << e.what()
                   << ". Please check your service principal credentials are valid and not expired.";
        }
        FAIL() << "Failed to connect to Azure: " << e.what();
    }
}

void AzureIntegrationTestBase::TryCreateContainer() {
    try {
        Unwrap(m_containerClient->CreateIfNotExistsAsync(boost::asio::use_future).get());
    } catch (const RequestFailedException& e) {
        HandleAuthenticationError(e);
        if (HasFatalFailure()) {
            return;
        }
        // Container might already exist or other non-auth error, continue
    } catch (const std::exception& e) {
        if (IsAuthenticationError(e)) {
            FAIL() << "Azure authentication failed: " << e.what()
                   << ". Please check your service principal credentials are valid and not expired.";
        }
        // Otherwise, continue - might be a transient error
    }
}

void AzureIntegrationTestBase::HandleAuthenticationError(const RequestFailedException& e) {
    static const constexpr unsigned int unauthorized = 401;
    static const constexpr unsigned int forbidden = 403;
    if (e.StatusCode == unauthorized || e.StatusCode == forbidden ||
        e.Code == AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::AuthenticationFailed)) {
        FAIL() << "Azure authentication failed: " << e.what()
               << ". Please check your service principal credentials are valid and not expired.";
    }
}

void AzureIntegrationTestBase::CleanupBlob() {
    if (m_containerClient && !m_blobName.empty()) {
        try {
            auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
            Unwrap(pageBlobClient.DeleteAsync(boost::asio::use_future).get());
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_logger, error) << e.what();
        } catch (...) {
            // Ignore cleanup errors
        }
    }
}

std::shared_ptr<Core::BlobClient> AzureIntegrationTestBase::CreateEmptyBlob() {
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    Unwrap(pageBlobClient.CreateAsync(Configuration::PageBlob::DefaultSize, boost::asio::use_future).get());
    BlobHelpers::SetFileSize(pageBlobClient, 0);
    return std::make_shared<PageBlob>(m_runtime, std::move(pageBlobClient));
}

std::shared_ptr<Core::BlobClient> AzureIntegrationTestBase::CreateBlobWithData(const std::vector<char>& data) {
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);

    // Calculate capacity rounded to page size
    int64_t capacity = std::max<int64_t>(static_cast<int64_t>(data.size()), Configuration::PageBlob::DefaultSize);
    if (capacity % Configuration::PageBlob::PageSize != 0) {
        capacity = ((capacity / Configuration::PageBlob::PageSize) + 1) * Configuration::PageBlob::PageSize;
    }

    Unwrap(pageBlobClient.CreateAsync(static_cast<uint64_t>(capacity), boost::asio::use_future).get());

    if (!data.empty()) {
        std::vector<char> paddedData = data;
        if (paddedData.size() % Configuration::PageBlob::PageSize != 0) {
            size_t padSize =
                Configuration::PageBlob::PageSize - (paddedData.size() % Configuration::PageBlob::PageSize);
            paddedData.insert(paddedData.end(), padSize, 0);
        }

        Unwrap(pageBlobClient
                   .UploadPagesAsync(
                       0, std::as_bytes(std::span<const char>(paddedData)), boost::asio::use_future,
                       RequestOptionsForTransfer(pageBlobClient.GetDefaultRequestOptions(), paddedData.size()))
                   .get());
        BlobHelpers::SetFileSize(pageBlobClient, static_cast<int64_t>(data.size()));
    }

    return std::make_shared<PageBlob>(m_runtime, std::move(pageBlobClient));
}

std::vector<char> AzureIntegrationTestBase::DownloadBlobData(size_t maxSize) {
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    const auto actualSize = BlobHelpers::GetFileSize(pageBlobClient);

    std::vector<char> data(std::min(static_cast<size_t>(actualSize), maxSize));
    if (!data.empty()) {
        AzureClient::DownloadBlobOptions options;
        options.Range = AzureClient::Models::BlobByteRange{0, data.size()};

        const auto result =
            Unwrap(pageBlobClient
                       .DownloadAsync(std::move(options), boost::asio::use_future,
                                      RequestOptionsForTransfer(pageBlobClient.GetDefaultRequestOptions(), data.size()))
                       .get());
        std::memcpy(data.data(), result.Content.data(), std::min(data.size(), result.Content.size()));
    }
    return data;
}

} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Testing
