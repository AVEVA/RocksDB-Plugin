// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/log/trivial.hpp>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Testing {
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
/// Plays the host application's role for tests: owns an io_context and runs it on its own threads until destroyed.
/// </summary>
class TestIoContext {
    boost::asio::io_context m_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_workGuard;
    std::vector<std::thread> m_threads;

  public:
    static const constexpr std::size_t DefaultThreadCount = 4;

    explicit TestIoContext(std::size_t threadCount = DefaultThreadCount);
    ~TestIoContext();
    TestIoContext(const TestIoContext&) = delete;
    TestIoContext& operator=(const TestIoContext&) = delete;
    TestIoContext(TestIoContext&&) = delete;
    TestIoContext& operator=(TestIoContext&&) = delete;

    [[nodiscard]] boost::asio::io_context& Get() noexcept { return m_context; }
};

/// <summary>
/// Base class for Azure integration tests with common setup and teardown.
/// </summary>
class AzureIntegrationTestBase : public ::testing::Test {
  protected:
    // Declared first so that it is destroyed last, after every Azure client using it (including those owned by
    // derived fixtures, whose members are destroyed before the base's).
    TestIoContext m_ioContext;
    std::optional<Models::ServicePrincipalStorageInfo> m_credentials;
    std::shared_ptr<ClientRuntime> m_runtime;
    std::shared_ptr<AzureClient::BlobContainerClient> m_containerClient;
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
    void HandleAuthenticationError(const RequestFailedException& e);

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
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Testing
