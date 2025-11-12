#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>
#include <azure/core/http/http.hpp>

#include <cstdlib>
#include <random>
#include <string>

using namespace AVEVA::RocksDB::Plugin::Azure::Impl;
using namespace Azure::Storage::Blobs;

namespace
{
    struct AzureTestCredentials
    {
        std::string servicePrincipalId;
        std::string servicePrincipalSecret;
        std::string tenantId;
        std::string storageAccountUrl;
        std::string containerName;

        static std::optional<AzureTestCredentials> FromEnvironment()
        {
            const char* spId = std::getenv("AZURE_SERVICE_PRINCIPAL_ID");
            const char* spSecret = std::getenv("AZURE_SERVICE_PRINCIPAL_SECRET");
            const char* tenant = std::getenv("AZURE_TENANT_ID");
            const char* storageAccountName = std::getenv("AZURE_STORAGE_ACCOUNT_NAME");
            const char* container = std::getenv("AZURE_TEST_CONTAINER");

            if (!spId || !spSecret || !storageAccountName)
            {
                return std::nullopt;
            }

            AzureTestCredentials creds;
            creds.servicePrincipalId = spId;
            creds.servicePrincipalSecret = spSecret;
            creds.tenantId = tenant ? tenant : ""; // Optional
            creds.storageAccountUrl = "https://" + std::string(storageAccountName) + ".blob.core.windows.net/";
            creds.containerName = container ? container : "aveva-rocksdb-plugin-integration-tests";

            return creds;
        }
    };

    std::string GenerateRandomBlobName()
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(100000, 999999);
        return "test-readable-" + std::to_string(dis(gen)) + ".blob";
    }
}

class ReadableFileIntegrationTests : public ::testing::Test
{
protected:
    std::optional<AzureTestCredentials> m_credentials;
    std::unique_ptr<BlobContainerClient> m_containerClient;
    std::string m_blobName;

    void SetUp() override
    {
        m_credentials = AzureTestCredentials::FromEnvironment();
        if (!m_credentials)
        {
            GTEST_SKIP() << "Azure credentials not found in environment variables. "
                << "Set AZURE_SERVICE_PRINCIPAL_ID and AZURE_SERVICE_PRINCIPAL_SECRET to run integration tests.";
        }

        m_blobName = GenerateRandomBlobName();

        // Create container client
        try
        {
            BlobServiceClient serviceClient(
                m_credentials->storageAccountUrl,
                std::make_shared<Azure::Identity::ClientSecretCredential>(
                    m_credentials->tenantId,
                    m_credentials->servicePrincipalId,
                    m_credentials->servicePrincipalSecret
                )
            );

            m_containerClient = std::make_unique<BlobContainerClient>(
                serviceClient.GetBlobContainerClient(m_credentials->containerName)
            );

            // Create container if it doesn't exist - this will test authentication
            try
            {
                m_containerClient->CreateIfNotExists();
            }
            catch (const Azure::Core::RequestFailedException& e)
            {
                // Check if it's an authentication error
                if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Unauthorized ||
                    e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
                {
                    GTEST_SKIP() << "Azure authentication failed: " << e.what()
                        << ". Please check your service principal credentials are valid and not expired.";
                }
                // Container might already exist or other non-auth error, continue
            }
            catch (const std::exception& e)
            {
                // For any other exception during container creation, check if it's auth-related
                std::string errorMsg = e.what();
                if (errorMsg.find("invalid_client") != std::string::npos ||
                    errorMsg.find("AADSTS") != std::string::npos ||
                    errorMsg.find("Unauthorized") != std::string::npos ||
                    errorMsg.find("expired") != std::string::npos)
                {
                    GTEST_SKIP() << "Azure authentication failed: " << e.what()
                        << ". Please check your service principal credentials are valid and not expired.";
                }
                // Otherwise, continue - might be a transient error
            }
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Unauthorized ||
                e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
            {
                GTEST_SKIP() << "Azure authentication failed: " << e.what()
                    << ". Please check your service principal credentials are valid and not expired.";
            }
            GTEST_SKIP() << "Failed to connect to Azure: " << e.what();
        }
        catch (const std::exception& e)
        {
            // Check if error message indicates authentication issues
            std::string errorMsg = e.what();
            if (errorMsg.find("invalid_client") != std::string::npos ||
                errorMsg.find("AADSTS") != std::string::npos ||
                errorMsg.find("Unauthorized") != std::string::npos ||
                errorMsg.find("expired") != std::string::npos)
            {
                GTEST_SKIP() << "Azure authentication failed: " << e.what()
                    << ". Please check your service principal credentials are valid and not expired.";
            }
            GTEST_SKIP() << "Failed to connect to Azure: " << e.what();
        }
    }

    void TearDown() override
    {
        if (m_containerClient && !m_blobName.empty())
        {
            try
            {
                auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
                pageBlobClient.Delete();
            }
            catch (...)
            {
                // Ignore cleanup errors
            }
        }
    }

    std::shared_ptr<AVEVA::RocksDB::Plugin::Core::BlobClient> CreateTestBlob(const std::vector<char>& data)
    {
        auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);

        // Calculate capacity rounded to page size
        size_t capacity = data.size();
        if (capacity % Configuration::PageBlob::PageSize != 0)
        {
            capacity = ((capacity / Configuration::PageBlob::PageSize) + 1) * Configuration::PageBlob::PageSize;
        }

        // Create the blob with appropriate capacity
        pageBlobClient.Create(capacity);

        // Upload data if any
        if (!data.empty())
        {
            std::vector<char> paddedData = data;
            // Pad to page size
            if (paddedData.size() % Configuration::PageBlob::PageSize != 0)
            {
                size_t padSize = Configuration::PageBlob::PageSize - (paddedData.size() % Configuration::PageBlob::PageSize);
                paddedData.insert(paddedData.end(), padSize, 0);
            }

            Azure::Core::IO::MemoryBodyStream stream(
                reinterpret_cast<const uint8_t*>(paddedData.data()),
                paddedData.size()
            );
            pageBlobClient.UploadPages(0, stream);

            // Set metadata for actual size
            BlobHelpers::SetFileSize(pageBlobClient, data.size());
        }

        return std::make_shared<PageBlob>(pageBlobClient);
    }
};

TEST_F(ReadableFileIntegrationTests, SequentialRead_SmallFile_ReadsCorrectly)
{
    // Arrange
    std::vector<char> testData(1024);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateTestBlob(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr);

    // Act
    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.SequentialRead(testData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
    EXPECT_EQ(testData.size(), file.GetOffset());
}

TEST_F(ReadableFileIntegrationTests, SequentialRead_MultipleChunks_ReadsInOrder)
{
    // Arrange
    std::vector<char> testData(2048);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateTestBlob(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr);

    // Act - Read in chunks
    std::vector<char> chunk1(512);
    std::vector<char> chunk2(512);
    std::vector<char> chunk3(1024);

    int64_t read1 = file.SequentialRead(512, chunk1.data());
    int64_t read2 = file.SequentialRead(512, chunk2.data());
    int64_t read3 = file.SequentialRead(1024, chunk3.data());

    // Assert
    EXPECT_EQ(512, read1);
    EXPECT_EQ(512, read2);
    EXPECT_EQ(1024, read3);
    EXPECT_EQ(2048, file.GetOffset());

    // Verify data
    std::vector<char> combined;
    combined.insert(combined.end(), chunk1.begin(), chunk1.end());
    combined.insert(combined.end(), chunk2.begin(), chunk2.end());
    combined.insert(combined.end(), chunk3.begin(), chunk3.end());
    EXPECT_EQ(testData, combined);
}

TEST_F(ReadableFileIntegrationTests, RandomRead_DifferentOffsets_ReadsCorrectly)
{
    // Arrange
    std::vector<char> testData(Configuration::PageBlob::PageSize * 2);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateTestBlob(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr);

    // Act - Random reads at different offsets
    std::vector<char> buffer1(100);
    std::vector<char> buffer2(200);
    std::vector<char> buffer3(512);

    int64_t read1 = file.RandomRead(0, 100, buffer1.data());
    int64_t read2 = file.RandomRead(500, 200, buffer2.data());
    int64_t read3 = file.RandomRead(Configuration::PageBlob::PageSize, 512, buffer3.data());

    // Assert
    EXPECT_EQ(100, read1);
    EXPECT_EQ(200, read2);
    EXPECT_EQ(512, read3);

    // Verify data
    EXPECT_TRUE(std::equal(buffer1.begin(), buffer1.end(), testData.begin()));
    EXPECT_TRUE(std::equal(buffer2.begin(), buffer2.end(), testData.begin() + 500));
    EXPECT_TRUE(std::equal(buffer3.begin(), buffer3.end(), testData.begin() + Configuration::PageBlob::PageSize));

    // Sequential offset should not be affected by random reads
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(ReadableFileIntegrationTests, Skip_AdvancesOffset_WithoutReading)
{
    // Arrange
    std::vector<char> testData(1024);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateTestBlob(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr);

    // Act
    file.Skip(100);
    std::vector<char> buffer(200);
    int64_t bytesRead = file.SequentialRead(200, buffer.data());

    // Assert
    EXPECT_EQ(100, file.GetOffset() - bytesRead);
    EXPECT_EQ(200, bytesRead);
    EXPECT_TRUE(std::equal(buffer.begin(), buffer.end(), testData.begin() + 100));
}

TEST_F(ReadableFileIntegrationTests, GetSize_ReturnsCorrectSize)
{
    // Arrange
    std::vector<char> testData(12345);
    auto blobClient = CreateTestBlob(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr);

    // Act
    int64_t size = file.GetSize();

    // Assert
    EXPECT_EQ(testData.size(), size);
}

TEST_F(ReadableFileIntegrationTests, SequentialRead_BeyondFileSize_ReturnsAvailableData)
{
    // Arrange
    std::vector<char> testData(500);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = 'X';
    }

    auto blobClient = CreateTestBlob(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr);

    // Act - Try to read more than available
    std::vector<char> buffer(1000, 0);
    int64_t bytesRead = file.SequentialRead(1000, buffer.data());

    // Assert
    EXPECT_EQ(500, bytesRead); // Should only read what's available
    EXPECT_TRUE(std::equal(buffer.begin(), buffer.begin() + 500, testData.begin()));
}

TEST_F(ReadableFileIntegrationTests, ReadEmptyFile_ReturnsZero)
{
    // Arrange - Create empty blob
    std::vector<char> emptyData;
    auto blobClient = CreateTestBlob(emptyData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr);

    // Act
    std::vector<char> buffer(100);
    int64_t bytesRead = file.SequentialRead(100, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(0, file.GetSize());
}
