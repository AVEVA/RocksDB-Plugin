// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>
#include <azure/core/http/http.hpp>
#include <boost/log/trivial.hpp>

#include <cstdlib>
#include <random>
#include <string>
#include <optional>
#include <memory>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Testing
{
    /// <summary>
    /// Loads Azure credentials from environment variables and creates a ServicePrincipalStorageInfo.
    /// Required: AZURE_SERVICE_PRINCIPAL_ID, AZURE_SERVICE_PRINCIPAL_SECRET, AZURE_STORAGE_ACCOUNT_NAME
    /// Optional: AZURE_TENANT_ID, AZURE_TEST_CONTAINER
    /// </summary>
    std::optional<Models::ServicePrincipalStorageInfo> LoadAzureCredentialsFromEnvironment();

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
        std::optional<Models::ServicePrincipalStorageInfo> m_credentials;
        std::unique_ptr<::Azure::Storage::Blobs::BlobContainerClient> m_containerClient;
        std::string m_blobName;
        std::string m_containerPrefix;
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

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
