#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
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
        return "test-writeable-" + std::to_string(dis(gen)) + ".blob";
 }
}

class WriteableFileIntegrationTests : public ::testing::Test
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
   Azure::Storage::Blobs::DownloadBlobToOptions options;
      options.Range = Azure::Core::Http::HttpRange();
            options.Range.Value().Offset = 0;
            options.Range.Value().Length = static_cast<int64_t>(data.size());
            
     auto result = pageBlobClient.DownloadTo(
   reinterpret_cast<uint8_t*>(data.data()),
             data.size(),
          options
            );
        }
return data;
    }
};

TEST_F(WriteableFileIntegrationTests, Append_SmallData_WritesSuccessfully)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    std::vector<char> testData(100, 'A');

    // Act
    file.Append(std::span<const char>(testData));
    file.Close();

    // Assert
    auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, Append_MultipleWrites_AccumulatesData)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> data1(100, 'A');
    std::vector<char> data2(150, 'B');
 std::vector<char> data3(200, 'C');

    // Act
    file.Append(std::span<const char>(data1));
    file.Append(std::span<const char>(data2));
    file.Append(std::span<const char>(data3));
    file.Close();

    // Assert
    std::vector<char> expected;
    expected.insert(expected.end(), data1.begin(), data1.end());
    expected.insert(expected.end(), data2.begin(), data2.end());
    expected.insert(expected.end(), data3.begin(), data3.end());

    auto downloadedData = DownloadBlobData(expected.size());
    EXPECT_EQ(expected, downloadedData);
    EXPECT_EQ(expected.size(), file.GetFileSize());
}

TEST_F(WriteableFileIntegrationTests, Append_LargeData_WritesCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
 
    // Create data larger than buffer size
    std::vector<char> testData(Configuration::PageBlob::DefaultBufferSize * 2);
    for (size_t i = 0; i < testData.size(); ++i)
    {
   testData[i] = static_cast<char>(i % 256);
    }

    // Act
    file.Append(std::span<const char>(testData));
    file.Close();

    // Assert
    auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData.size(), downloadedData.size());
    EXPECT_EQ(testData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, Flush_PersistsData_ToBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
  std::vector<char> testData(500, 'X');

    // Act
    file.Append(std::span<const char>(testData));
    file.Flush(); // Flush without closing

    // Assert - Data should be persisted, but size metadata may not be updated yet
    // Download the actual uploaded data (rounded to page size)
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    size_t downloadSize = testData.size();
 if (downloadSize % Configuration::PageBlob::PageSize != 0)
  {
        downloadSize = ((downloadSize / Configuration::PageBlob::PageSize) + 1) * Configuration::PageBlob::PageSize;
    }
    
    std::vector<char> downloadedData(downloadSize);
    Azure::Storage::Blobs::DownloadBlobToOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = 0;
    options.Range.Value().Length = static_cast<int64_t>(downloadSize);
    
    pageBlobClient.DownloadTo(
        reinterpret_cast<uint8_t*>(downloadedData.data()),
 downloadedData.size(),
        options
 );
    
    // Verify the test data is at the beginning
    EXPECT_TRUE(std::equal(testData.begin(), testData.end(), downloadedData.begin()));
}

TEST_F(WriteableFileIntegrationTests, Sync_PersistsDataAndSize_ToBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> testData(750, 'Y');

    // Act
    file.Append(std::span<const char>(testData));
  file.Sync();

    // Assert
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    uint64_t actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(testData.size(), actualSize);

    auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, Truncate_ReducesFileSize)
{
    // Arrange
    std::vector<char> initialData(1000, 'Z');
    auto blobClient = CreateBlobWithData(initialData);
  WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act
    file.Truncate(500);
    file.Close();

    // Assert
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    uint64_t actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(500, actualSize);
    EXPECT_EQ(500, file.GetFileSize());
}

TEST_F(WriteableFileIntegrationTests, AppendToExistingFile_PreservesExistingData)
{
    // Arrange - Create file with initial data
    std::vector<char> initialData(Configuration::PageBlob::PageSize - 100, 'I');
    auto blobClient = CreateBlobWithData(initialData);
    
    // Open file for writing
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> newData(200, 'N');

    // Act
    file.Append(std::span<const char>(newData));
    file.Close();

    // Assert
    std::vector<char> expected;
    expected.insert(expected.end(), initialData.begin(), initialData.end());
    expected.insert(expected.end(), newData.begin(), newData.end());

    auto downloadedData = DownloadBlobData(expected.size());
    EXPECT_EQ(expected, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, GetFileSize_ReturnsCorrectSize)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> data1(300, 'A');
std::vector<char> data2(200, 'B');

    // Act & Assert
    EXPECT_EQ(0, file.GetFileSize());
    
    file.Append(std::span<const char>(data1));
    EXPECT_EQ(300, file.GetFileSize());
  
    file.Append(std::span<const char>(data2));
    EXPECT_EQ(500, file.GetFileSize());
}

TEST_F(WriteableFileIntegrationTests, Close_CanBeCalledMultipleTimes)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    std::vector<char> testData(100, 'M');
    file.Append(std::span<const char>(testData));

    // Act & Assert - Should not throw
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
}

TEST_F(WriteableFileIntegrationTests, PageAlignedWrites_HandleCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
 // Write exactly one page
 std::vector<char> pageData(Configuration::PageBlob::PageSize, 'P');

    // Act
 file.Append(std::span<const char>(pageData));
    file.Close();

    // Assert
 auto downloadedData = DownloadBlobData(pageData.size());
    EXPECT_EQ(pageData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, NonPageAlignedWrites_HandleCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file(m_blobName, blobClient, nullptr, m_logger);
    
    // Write non-page-aligned data
    std::vector<char> testData(Configuration::PageBlob::PageSize + 100, 'Q');

    // Act
    file.Append(std::span<const char>(testData));
    file.Close();

    // Assert
 auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData, downloadedData);
}
