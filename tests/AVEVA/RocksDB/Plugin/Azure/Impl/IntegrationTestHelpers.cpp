// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "IntegrationTestHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/TestLogger.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <boost/asio/use_future.hpp>
#include <boost/log/trivial.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <cstring>
#include <string_view>
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
    // Credentials alone must never make a plain test run mutate cloud storage.
    const char* optIn = std::getenv("AVEVA_RUN_AZURE_INTEGRATION_TESTS");
    if (optIn == nullptr || std::string_view(optIn) != "1") {
        return std::nullopt;
    }

    const char* spId = std::getenv("AZURE_SERVICE_PRINCIPAL_ID");
    const char* spSecret = std::getenv("AZURE_SERVICE_PRINCIPAL_SECRET");
    const char* tenant = std::getenv("AZURE_TENANT_ID");
    const char* storageAccountName = std::getenv("AZURE_STORAGE_ACCOUNT_NAME");
    const char* container = std::getenv("AZURE_TEST_CONTAINER");

    if (!spId || !spSecret || !storageAccountName || !container || std::string_view(container).empty()) {
        return std::nullopt;
    }

    std::string tenantId = tenant ? tenant : "";
    std::string storageAccountUrl = "https://" + std::string(storageAccountName) + ".blob.core.windows.net/";
    std::string containerName = container;

    return Models::ServicePrincipalStorageInfo(containerName, // Using container name as dbName
                                               storageAccountUrl, spId, spSecret, tenantId);
}

std::string GenerateRandomBlobName(const std::string& prefix) {
    return prefix + "-" + boost::uuids::to_string(boost::uuids::random_generator()()) + ".blob";
}

bool IsAuthenticationError(const std::exception& e) {
    std::string errorMsg = e.what();
    return errorMsg.find("invalid_client") != std::string::npos || errorMsg.find("AADSTS") != std::string::npos ||
           errorMsg.find("Unauthorized") != std::string::npos || errorMsg.find("expired") != std::string::npos;
}

void AzureIntegrationTestBase::SetUp() {
    m_credentials = LoadAzureCredentialsFromEnvironment();
    if (!m_credentials) {
        GTEST_SKIP() << "Live Azure tests are opt-in. Set AVEVA_RUN_AZURE_INTEGRATION_TESTS=1, "
                     << "AZURE_SERVICE_PRINCIPAL_ID, AZURE_SERVICE_PRINCIPAL_SECRET, AZURE_STORAGE_ACCOUNT_NAME and "
                        "AZURE_TEST_CONTAINER to run them.";
    }

    m_blobName = GenerateRandomBlobName(GetBlobNamePrefix());
    m_root = "it-" + boost::uuids::to_string(boost::uuids::random_generator()());
    m_containerPrefix =
        StorageAccount::UniquePrefix(m_credentials->GetStorageAccountUrl(), m_credentials->GetDbName()) + "/" + m_root;
    m_logger = AVEVA::RocksDB::Plugin::Tests::MakeTestLogger();

    CreateContainerClient();
}

void AzureIntegrationTestBase::TearDown() { CleanupBlob(); }

void AzureIntegrationTestBase::CreateContainerClient() {
    try {
        m_runtime = std::make_shared<ClientRuntime>(m_ioContext.Get());
        const auto serviceClient = ClientFactory::CreateServiceClient(m_runtime, *m_credentials);

        m_containerClient = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::BlobContainerClient>(
            serviceClient.GetBlobContainerClient(m_credentials->GetDbName()));

        // Create container if it doesn't exist - this will test authentication
        TryCreateContainer();
    } catch (const RequestFailedException& e) {
        HandleAuthenticationError(e);
        if (HasFatalFailure()) {
            return;
        }
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
    if (!m_containerClient || m_root.empty()) {
        return;
    }
    try {
        AzureClient::ListBlobsOptions options;
        options.Prefix = m_root + "/";
        std::vector<std::string> names;
        while (true) {
            auto page = Unwrap(m_containerClient->ListBlobsAsync(options, boost::asio::use_future).get());
            for (const auto& blob : page.Blobs) {
                names.push_back(blob.Name);
            }
            if (page.NextMarker.empty()) {
                break;
            }
            options.Marker = std::move(page.NextMarker);
        }
        if (!names.empty()) {
            Unwrap(
                m_containerClient->DeleteBlobsAsync(names, AzureClient::DeleteBlobsOptions{}, boost::asio::use_future)
                    .get());
        }
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, error) << "Cleanup of '" << m_root << "' failed: " << e.what();
    } catch (...) {
        // Ignore cleanup errors
    }
}

std::shared_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::PageBlobClient> AzureIntegrationTestBase::CreateEmptyBlob() {
    const auto pageBlobClientPtr = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::PageBlobClient>(
        m_containerClient->GetPageBlobClient(BlobPath()));
    Unwrap(pageBlobClientPtr->CreateAsync(Configuration::PageBlob::DefaultSize, boost::asio::use_future).get());
    BlobOperations::SetSize(*m_runtime, *pageBlobClientPtr, 0);
    return pageBlobClientPtr;
}

std::shared_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::PageBlobClient>
AzureIntegrationTestBase::CreateBlobWithData(const std::vector<char>& data) {
    const auto pageBlobClientPtr = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::PageBlobClient>(
        m_containerClient->GetPageBlobClient(BlobPath()));
    auto& pageBlobClient = *pageBlobClientPtr;

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
        BlobOperations::SetSize(*m_runtime, pageBlobClient, static_cast<int64_t>(data.size()));
    }

    return pageBlobClientPtr;
}

std::vector<char> AzureIntegrationTestBase::DownloadBlobData(size_t maxSize) {
    const auto pageBlobClientPtr = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::PageBlobClient>(
        m_containerClient->GetPageBlobClient(BlobPath()));
    auto& pageBlobClient = *pageBlobClientPtr;
    const auto actualSize = BlobOperations::GetInfo(pageBlobClient).Size;

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
