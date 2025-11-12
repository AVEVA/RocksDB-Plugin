#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>
#include <azure/core/http/http.hpp>
#include <boost/log/sources/logger.hpp>

#include <cstdlib>
#include <random>
#include <string>
#include <span>

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
  const char* storageUrl = std::getenv("AZURE_STORAGE_ACCOUNT_URL");
        const char* container = std::getenv("AZURE_TEST_CONTAINER");

      if (!spId || !spSecret)
            {
return std::nullopt;
            }

            AzureTestCredentials creds;
            creds.servicePrincipalId = spId;
            creds.servicePrincipalSecret = spSecret;
        creds.tenantId = tenant ? tenant : "";
         creds.storageAccountUrl = storageUrl ? storageUrl : "https://teststorageaccount.blob.core.windows.net";
            creds.containerName = container ? container : "rocksdb-integration-tests";

         return creds;
   }
    };

    std::string GenerateRandomBlobName()
    {
        std::random_device rd;
     std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(100000, 999999);
        return "test-readwrite-" + std::to_string(dis(gen)) + ".blob";
  }
}

class ReadWriteFileIntegrationTests : public ::testing::Test
{
protected:
    std::optional<AzureTestCredentials> m_credentials;
    std::unique_ptr<BlobContainerClient> m_containerClient;
    std::string m_blobName;
    std::shared_ptr<boost::log::sources::logger_mt> m_logger;

    void SetUp() override
    {
   m_credentials = AzureTestCredentials::FromEnvironment();
        if (!m_credentials)
   {
          GTEST_SKIP() << "Azure credentials not found in environment variables. "
      << "Set AZURE_SERVICE_PRINCIPAL_ID and AZURE_SERVICE_PRINCIPAL_SECRET to run integration tests.";
   }

    m_blobName = GenerateRandomBlobName();
        m_logger = std::make_shared<boost::log::sources::logger_mt>();

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

std::shared_ptr<AVEVA::RocksDB::Plugin::Core::BlobClient> CreateEmptyBlob()
    {
      auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    pageBlobClient.Create(Configuration::PageBlob::DefaultSize);
    BlobHelpers::SetFileSize(pageBlobClient, 0);
        return std::make_shared<PageBlob>(pageBlobClient);
    }

    std::shared_ptr<AVEVA::RocksDB::Plugin::Core::BlobClient> CreateBlobWithData(const std::vector<char>& data)
    {
      auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    
 // Calculate capacity rounded to page size
      size_t capacity = std::max<size_t>(data.size(), Configuration::PageBlob::DefaultSize);
        if (capacity % Configuration::PageBlob::PageSize != 0)
        {
  capacity = ((capacity / Configuration::PageBlob::PageSize) + 1) * Configuration::PageBlob::PageSize;
        }

  pageBlobClient.Create(capacity);

        if (!data.empty())
        {
            std::vector<char> paddedData = data;
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
      BlobHelpers::SetFileSize(pageBlobClient, data.size());
  }

        return std::make_shared<PageBlob>(pageBlobClient);
    }

    std::vector<char> DownloadBlobData(size_t maxSize)
    {
        auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
   uint64_t actualSize = BlobHelpers::GetFileSize(pageBlobClient);
 
        std::vector<char> data(std::min(actualSize, static_cast<uint64_t>(maxSize)));
  if (!data.empty())
  {
            auto result = pageBlobClient.DownloadTo(
         reinterpret_cast<uint8_t*>(data.data()),
         data.size()
        );
  }
        return data;
    }
};

TEST_F(ReadWriteFileIntegrationTests, Write_ThenRead_DataMatchesCorrectly)
{
    // Arrange
  auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> testData(500);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    // Act - Write data
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    // Read data back
  std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(0, testData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, Write_AtDifferentOffsets_ReadsCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> data1(100, 'A');
    std::vector<char> data2(150, 'B');
    std::vector<char> data3(200, 'C');

    // Act - Write at different offsets
    file.Write(0, data1.data(), data1.size());
file.Write(500, data2.data(), data2.size());
    file.Write(1000, data3.data(), data3.size());
    file.Sync();

    // Read back
    std::vector<char> readBuffer1(100);
    std::vector<char> readBuffer2(150);
    std::vector<char> readBuffer3(200);

    int64_t read1 = file.Read(0, 100, readBuffer1.data());
    int64_t read2 = file.Read(500, 150, readBuffer2.data());
    int64_t read3 = file.Read(1000, 200, readBuffer3.data());

    // Assert
    EXPECT_EQ(100, read1);
    EXPECT_EQ(150, read2);
    EXPECT_EQ(200, read3);
    EXPECT_EQ(data1, readBuffer1);
    EXPECT_EQ(data2, readBuffer2);
    EXPECT_EQ(data3, readBuffer3);
}

TEST_F(ReadWriteFileIntegrationTests, Write_LargeData_HandlesCorrectly)
{
// Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
  // Create large data spanning multiple pages
    std::vector<char> testData(Configuration::PageBlob::PageSize * 3);
    for (size_t i = 0; i < testData.size(); ++i)
  {
        testData[i] = static_cast<char>(i % 256);
    }

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(0, testData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, OverwriteExistingData_UpdatesCorrectly)
{
    // Arrange
    std::vector<char> initialData(1000, 'X');
    auto blobClient = CreateBlobWithData(initialData);
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> newData(200, 'Y');

    // Act - Overwrite portion of data
    file.Write(100, newData.data(), newData.size());
    file.Sync();

    // Assert
    std::vector<char> expected = initialData;
    std::copy(newData.begin(), newData.end(), expected.begin() + 100);

    std::vector<char> readBuffer(expected.size());
    int64_t bytesRead = file.Read(0, expected.size(), readBuffer.data());
    
    EXPECT_EQ(expected.size(), bytesRead);
    EXPECT_EQ(expected, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, Flush_PersistsChangesToBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> testData(750, 'F');

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Flush();

    // Assert - Data should be persisted
    auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData, downloadedData);
}

TEST_F(ReadWriteFileIntegrationTests, Sync_UpdatesFileSizeInBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> testData(888, 'S');

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    // Assert
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    uint64_t actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(testData.size(), actualSize);
}

TEST_F(ReadWriteFileIntegrationTests, Read_FromExistingFile_ReadsCorrectly)
{
  // Arrange - Create file with data
    std::vector<char> initialData(Configuration::PageBlob::PageSize);
    for (size_t i = 0; i < initialData.size(); ++i)
    {
        initialData[i] = static_cast<char>(i % 256);
    }
    auto blobClient = CreateBlobWithData(initialData);
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act - Read the data
    std::vector<char> readBuffer(initialData.size());
    int64_t bytesRead = file.Read(0, initialData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(initialData.size(), bytesRead);
    EXPECT_EQ(initialData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, NonPageAlignedWrites_HandleCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    // Write at non-page-aligned offset
    int64_t offset = 137;
    std::vector<char> testData(555, 'N');

    // Act
    file.Write(offset, testData.data(), testData.size());
    file.Sync();

    // Assert
 std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(offset, testData.size(), readBuffer.data());
    
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, GetFileSize_ReturnsCorrectSize)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> testData(1234, 'G');

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    // Assert - Verify size through blob client
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    uint64_t actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(testData.size(), actualSize);
}

TEST_F(ReadWriteFileIntegrationTests, Close_CanBeCalledMultipleTimes)
{
  // Arrange
  auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> testData(100, 'C');
    file.Write(0, testData.data(), testData.size());

    // Act & Assert - Should not throw
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
}

TEST_F(ReadWriteFileIntegrationTests, WriteAndReadAcrossPageBoundaries_HandlesCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    // Write data that spans across page boundaries
    int64_t offset = Configuration::PageBlob::PageSize - 100;
    std::vector<char> testData(200, 'B');

    // Act
file.Write(offset, testData.data(), testData.size());
    file.Sync();

    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(offset, testData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, WriteExtendsBeyondInitialCapacity_ExpandsBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    // Write data beyond default size
    int64_t offset = Configuration::PageBlob::DefaultSize + 1000;
    std::vector<char> testData(1000, 'E');

    // Act
    file.Write(offset, testData.data(), testData.size());
    file.Sync();

    // Assert
    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(offset, testData.size(), readBuffer.data());
    
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, MultipleWritesAndReads_MaintainDataIntegrity)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
  ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    // Act - Perform multiple interleaved writes and reads
    std::vector<char> data1(300, '1');
    std::vector<char> data2(400, '2');
    std::vector<char> data3(500, '3');

    file.Write(0, data1.data(), data1.size());
    file.Write(1000, data2.data(), data2.size());
    
  std::vector<char> read1(300);
 int64_t bytes1 = file.Read(0, 300, read1.data());
    EXPECT_EQ(data1, read1);

    file.Write(2000, data3.data(), data3.size());
    file.Sync();

    std::vector<char> read2(400);
    std::vector<char> read3(500);
    int64_t bytes2 = file.Read(1000, 400, read2.data());
 int64_t bytes3 = file.Read(2000, 500, read3.data());

    // Assert
    EXPECT_EQ(300, bytes1);
    EXPECT_EQ(400, bytes2);
    EXPECT_EQ(500, bytes3);
    EXPECT_EQ(data2, read2);
    EXPECT_EQ(data3, read3);
}
