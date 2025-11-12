// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>
#include <azure/core/http/http.hpp>
#include <boost/log/sources/logger.hpp>

#include <cstdlib>
#include <random>
#include <string>
#include <optional>
#include <memory>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Testing
{
    /// <summary>
    /// Azure credentials for integration tests, loaded from environment variables.
    /// </summary>
    struct AzureTestCredentials
    {
        std::string servicePrincipalId;
        std::string servicePrincipalSecret;
        std::string tenantId;
        std::string storageAccountUrl;
        std::string containerName;

        /// <summary>
        /// Loads Azure credentials from environment variables.
        /// Required: AZURE_SERVICE_PRINCIPAL_ID, AZURE_SERVICE_PRINCIPAL_SECRET, AZURE_STORAGE_ACCOUNT_NAME
        /// Optional: AZURE_TENANT_ID, AZURE_TEST_CONTAINER
        /// </summary>
        static std::optional<AzureTestCredentials> FromEnvironment();
    };

    /// <summary>
    /// Generates a random blob name with the given prefix for test isolation.
    /// </summary>
    std::string GenerateRandomBlobName(const std::string& prefix = "test");

    /// <summary>
    /// Checks if an exception indicates an Azure authentication failure.
    /// </summary>
    bool IsAuthenticationError(const std::exception& e);

    /// <summary>
    /// Base class for Azure integration tests with common setup and teardown.
    /// </summary>
    class AzureIntegrationTestBase : public ::testing::Test
    {
    protected:
        std::optional<AzureTestCredentials> m_credentials;
        std::unique_ptr<::Azure::Storage::Blobs::BlobContainerClient> m_containerClient;
        std::string m_blobName;
        std::shared_ptr<boost::log::sources::logger_mt> m_logger;

        /// <summary>
        /// Override this to provide a custom blob name prefix.
        /// </summary>
        virtual std::string GetBlobNamePrefix() const = 0;

        void SetUp() override;
        void TearDown() override;

        /// <summary>
        /// Creates the Azure container client and ensures the container exists.
        /// </summary>
        void CreateContainerClient();

        /// <summary>
        /// Attempts to create the container, handling authentication errors gracefully.
        /// </summary>
        void TryCreateContainer();

        /// <summary>
        /// Checks if a RequestFailedException is an authentication error and skips the test if so.
        /// </summary>
        void HandleAuthenticationError(const ::Azure::Core::RequestFailedException& e);

        /// <summary>
        /// Deletes the test blob created during the test.
        /// </summary>
        void CleanupBlob();

        /// <summary>
        /// Creates an empty page blob with default size and file size set to 0.
        /// </summary>
        std::shared_ptr<Core::BlobClient> CreateEmptyBlob();

        /// <summary>
        /// Creates a page blob with the provided data.
        /// The blob capacity is rounded up to the nearest page size.
        /// </summary>
        std::shared_ptr<Core::BlobClient> CreateBlobWithData(const std::vector<char>& data);

        /// <summary>
        /// Downloads blob data up to maxSize bytes.
        /// </summary>
        std::vector<char> DownloadBlobData(size_t maxSize);
    };
}
