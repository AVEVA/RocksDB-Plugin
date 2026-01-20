// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "IntegrationTestHelpers.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>

#include <string>
#include <vector>
#include <memory>

using AVEVA::RocksDB::Plugin::Azure::Impl::BlobFilesystemImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::Testing::AzureIntegrationTestBase;
using AVEVA::RocksDB::Plugin::Azure::Impl::Testing::GenerateRandomBlobName;

class BlobFilesystemIntegrationTests : public AzureIntegrationTestBase
{
protected:
    std::unique_ptr<BlobFilesystemImpl> m_filesystem;

    std::string GetBlobNamePrefix() const override
    {
        return "test-filesystem";
    }

    void SetUp() override
    {
        AzureIntegrationTestBase::SetUp();

        if (m_credentials)
        {
            m_filesystem = std::make_unique<BlobFilesystemImpl>(
                *m_credentials,
                std::nullopt,  // No backup credentials for tests
                Configuration::PageBlob::DefaultSize,
                Configuration::PageBlob::DefaultBufferSize,
                m_logger
            );
        }
    }
};

TEST_F(BlobFilesystemIntegrationTests, CreateWriteableFile_CreatesNewFile)
{
    // Arrange
    const auto path = m_containerPrefix + "/" + m_blobName;

    // Act
    auto file = m_filesystem->CreateWriteableFile(path);

    // Assert
    EXPECT_TRUE(m_filesystem->FileExists(path));
    EXPECT_EQ(0, m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, CreateReadableFile_FromExistingBlob_Succeeds)
{
    // Arrange
    std::vector<char> testData(1024, 'X');
    CreateBlobWithData(testData);
    const auto path = m_containerPrefix + "/" + m_blobName;

    // Act
    auto file = m_filesystem->CreateReadableFile(path);

    // Assert
    EXPECT_EQ(testData.size(), file.GetSize());
}

TEST_F(BlobFilesystemIntegrationTests, CreateReadWriteFile_CreatesNewFile)
{
    // Arrange
    const auto path = m_containerPrefix + "/" + m_blobName;

    // Act
    auto file = m_filesystem->CreateReadWriteFile(path);

    // Assert
    EXPECT_TRUE(m_filesystem->FileExists(path));
}

TEST_F(BlobFilesystemIntegrationTests, FileExists_NonExistentFile_ReturnsFalse)
{
    // Arrange
    std::string nonExistentFile = m_containerPrefix + "/nonexistent-" + m_blobName;

    // Act & Assert
    EXPECT_FALSE(m_filesystem->FileExists(nonExistentFile));
}

TEST_F(BlobFilesystemIntegrationTests, GetFileSize_AfterWriting_ReturnsCorrectSize)
{
    // Arrange
    const size_t dataSize = 2048;
    std::vector<char> testData(dataSize, 'A');
    CreateBlobWithData(testData);
    const auto path = m_containerPrefix + "/" + m_blobName;

    // Act
    int64_t size = m_filesystem->GetFileSize(path);

    // Assert
    EXPECT_EQ(dataSize, size);
}

TEST_F(BlobFilesystemIntegrationTests, GetFileModificationTime_ReturnsValidTimestamp)
{
    // Arrange
    const auto path = m_containerPrefix + "/" + m_blobName;
    auto file = m_filesystem->CreateWriteableFile(path);

    // Act
    const auto modTime = m_filesystem->GetFileModificationTime(path);

    // Assert
    EXPECT_GT(modTime, 0);
}

TEST_F(BlobFilesystemIntegrationTests, DeleteFile_ExistingFile_ReturnsTrue)
{
    // Arrange
    const auto path = m_containerPrefix + "/" + m_blobName;
    auto file = m_filesystem->CreateWriteableFile(path);
    EXPECT_TRUE(m_filesystem->FileExists(path));

    // Act
    bool deleted = m_filesystem->DeleteFile(path);

    // Assert
    EXPECT_TRUE(deleted);
    EXPECT_FALSE(m_filesystem->FileExists(path));
}

TEST_F(BlobFilesystemIntegrationTests, DeleteFile_NonExistentFile_ReturnsFalse)
{
    // Arrange
    std::string nonExistentFile = m_containerPrefix + "/nonexistent-" + m_blobName;

    // Act
    bool deleted = m_filesystem->DeleteFile(nonExistentFile);

    // Assert
    EXPECT_FALSE(deleted);
}

TEST_F(BlobFilesystemIntegrationTests, GetChildren_EmptyDirectory_ReturnsEmpty)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/empty-dir-" + GenerateRandomBlobName();

    // Act
    auto children = m_filesystem->GetChildren(dirPrefix);

    // Assert
    EXPECT_TRUE(children.empty());
}

TEST_F(BlobFilesystemIntegrationTests, GetChildren_WithFiles_ReturnsFileNames)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/test-dir-" + GenerateRandomBlobName();
    std::string file1 = dirPrefix + "/file1.sst";
    std::string file2 = dirPrefix + "/file2.sst";
    std::string file3 = dirPrefix + "/file3.log";

    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        auto f2 = m_filesystem->CreateWriteableFile(file2);
        auto f3 = m_filesystem->CreateWriteableFile(file3);

        // Act
        auto children = m_filesystem->GetChildren(dirPrefix);

        // Assert
        EXPECT_EQ(3, children.size());
        EXPECT_TRUE(std::find(children.begin(), children.end(), "file1.sst") != children.end());
        EXPECT_TRUE(std::find(children.begin(), children.end(), "file2.sst") != children.end());
        EXPECT_TRUE(std::find(children.begin(), children.end(), "file3.log") != children.end());
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(file1));
    EXPECT_TRUE(m_filesystem->DeleteFile(file2));
    EXPECT_TRUE(m_filesystem->DeleteFile(file3));
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_ReturnsCorrectSizes)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/attr-dir-" + GenerateRandomBlobName();
    std::string file1 = dirPrefix + "/file1.sst";
    std::string file2 = dirPrefix + "/file2.sst";

    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        f1.Append(std::vector<char>(512, 'A'));
        f1.Sync();  // Sync to update size metadata

        auto f2 = m_filesystem->CreateWriteableFile(file2);
        f2.Append(std::vector<char>(1024, 'B'));
        f2.Sync();  // Sync to update size metadata

        // Act
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

        // Assert
        EXPECT_EQ(2, attributes.size());

        auto it1 = std::find_if(attributes.begin(), attributes.end(),
            [](const auto& attr) { return attr.GetName() == "file1.sst"; });
        auto it2 = std::find_if(attributes.begin(), attributes.end(),
            [](const auto& attr) { return attr.GetName() == "file2.sst"; });

        EXPECT_NE(attributes.end(), it1);
        EXPECT_NE(attributes.end(), it2);
        EXPECT_EQ(512, it1->GetSize());
        EXPECT_EQ(1024, it2->GetSize());
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(file1));
    EXPECT_TRUE(m_filesystem->DeleteFile(file2));
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_EmptyDirectory_ReturnsEmpty)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/empty-attr-dir-" + GenerateRandomBlobName();

    // Act
    auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

    // Assert
    EXPECT_TRUE(attributes.empty());
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_WithSubdirectories_ReturnsAllFiles)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/nested-attr-dir-" + GenerateRandomBlobName();
    std::string file1 = dirPrefix + "/file1.sst";
    std::string file2 = dirPrefix + "/subdir/file2.sst";
    std::string file3 = dirPrefix + "/subdir/nested/file3.log";

    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        f1.Append(std::vector<char>(256, 'A'));
        f1.Sync();

        auto f2 = m_filesystem->CreateWriteableFile(file2);
        f2.Append(std::vector<char>(512, 'B'));
        f2.Sync();

        auto f3 = m_filesystem->CreateWriteableFile(file3);
        f3.Append(std::vector<char>(768, 'C'));
        f3.Sync();

        // Act
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

        // Assert - Should return all files in all subdirectories
        EXPECT_EQ(3, attributes.size());

        auto it1 = std::find_if(attributes.begin(), attributes.end(),
            [](const auto& attr) { return attr.GetName() == "file1.sst"; });
        auto it2 = std::find_if(attributes.begin(), attributes.end(),
            [](const auto& attr) { return attr.GetName() == "subdir/file2.sst"; });
        auto it3 = std::find_if(attributes.begin(), attributes.end(),
            [](const auto& attr) { return attr.GetName() == "subdir/nested/file3.log"; });

        EXPECT_NE(attributes.end(), it1);
        EXPECT_NE(attributes.end(), it2);
        EXPECT_NE(attributes.end(), it3);
        EXPECT_EQ(256, it1->GetSize());
        EXPECT_EQ(512, it2->GetSize());
        EXPECT_EQ(768, it3->GetSize());
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(file1));
    EXPECT_TRUE(m_filesystem->DeleteFile(file2));
    EXPECT_TRUE(m_filesystem->DeleteFile(file3));
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_ZeroSizeFiles_ReturnsCorrectly)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/zero-size-dir-" + GenerateRandomBlobName();
    std::string file1 = dirPrefix + "/empty1.sst";
    std::string file2 = dirPrefix + "/empty2.log";

    {
        // Create files without writing any data
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        auto f2 = m_filesystem->CreateWriteableFile(file2);

        // Act
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

        // Assert
        EXPECT_EQ(2, attributes.size());

        for (const auto& attr : attributes)
        {
            EXPECT_EQ(0, attr.GetSize());
        }
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(file1));
    EXPECT_TRUE(m_filesystem->DeleteFile(file2));
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_MixedFileSizes_ReturnsCorrectly)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/mixed-sizes-dir-" + GenerateRandomBlobName();
    std::vector<std::pair<std::string, size_t>> fileSpecs = {
        {"tiny.sst", 1},
        {"small.sst", 128},
        {"medium.sst", 4096},
        {"large.sst", 65536}
    };

    {
        for (const auto& [filename, size] : fileSpecs)
        {
            std::string fullPath = dirPrefix + "/" + filename;
            auto file = m_filesystem->CreateWriteableFile(fullPath);
            file.Append(std::vector<char>(size, 'X'));
            file.Sync();
        }

        // Act
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

        // Assert
        EXPECT_EQ(fileSpecs.size(), attributes.size());

        for (const auto& [filename, expectedSize] : fileSpecs)
        {
            auto it = std::find_if(attributes.begin(), attributes.end(),
                [&filename](const auto& attr) { return attr.GetName() == filename; });

            ASSERT_NE(attributes.end(), it) << "File " << filename << " not found in attributes";
            EXPECT_EQ(expectedSize, it->GetSize()) << "Wrong size for file " << filename;
        }
    }

    // Cleanup
    [[maybe_unused]] size_t remaining = m_filesystem->DeleteDir(dirPrefix);
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_WithoutTrailingSlash_ReturnsCorrectly)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/no-slash-dir-" + GenerateRandomBlobName();
    std::string file1 = dirPrefix + "/file1.sst";
    std::string file2 = dirPrefix + "/file2.sst";

    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        f1.Append(std::vector<char>(100, 'A'));
        f1.Sync();

        auto f2 = m_filesystem->CreateWriteableFile(file2);
        f2.Append(std::vector<char>(200, 'B'));
        f2.Sync();

        // Act - Call without trailing slash
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix);

        // Assert - Should still work
        EXPECT_EQ(2, attributes.size());
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(file1));
    EXPECT_TRUE(m_filesystem->DeleteFile(file2));
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_LargeNumberOfFiles_HandlesCorrectly)
{
    // Arrange - Create more files than typical page size to test pagination
    std::string dirPrefix = m_containerPrefix + "/many-files-dir-" + GenerateRandomBlobName();
    const int fileCount = 25;  // Should be enough to test pagination behavior
    std::vector<std::string> filePaths;

    {
        for (int i = 0; i < fileCount; ++i)
        {
            std::string filePath = dirPrefix + "/file" + std::to_string(i) + ".sst";
            filePaths.push_back(filePath);

            auto file = m_filesystem->CreateWriteableFile(filePath);
            // Write different sizes to each file
            file.Append(std::vector<char>(static_cast<size_t>(100 * (i + 1)), static_cast<char>('A' + (i % 26))));
            file.Sync();
        }

        // Act
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

        // Assert
        EXPECT_EQ(fileCount, attributes.size());

        // Verify each file is present with correct size
        for (int i = 0; i < fileCount; ++i)
        {
            std::string filename = "file" + std::to_string(i) + ".sst";
            size_t expectedSize = static_cast<size_t>(100 * (i + 1));

            auto it = std::find_if(attributes.begin(), attributes.end(),
                [&filename](const auto& attr) { return attr.GetName() == filename; });

            ASSERT_NE(attributes.end(), it) << "File " << filename << " not found";
            EXPECT_EQ(expectedSize, it->GetSize()) << "Wrong size for " << filename;
        }
    }

    // Cleanup
    [[maybe_unused]] size_t remaining = m_filesystem->DeleteDir(dirPrefix);
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_VeryLargeNumberOfFiles_TestsPagination)
{
    // Arrange - Create many files to force multiple pages (more than default PageSizeHint)
    std::string dirPrefix = m_containerPrefix + "/pagination-test-dir-" + GenerateRandomBlobName();
    const int fileCount = 15;  // Reasonable number for integration test, adjust PageSizeHint to test pagination

    {
        for (int i = 0; i < fileCount; ++i)
        {
            std::string filePath = dirPrefix + "/file" + std::to_string(i) + ".sst";
            auto file = m_filesystem->CreateWriteableFile(filePath);
            // Write unique size for each file
            file.Append(std::vector<char>(static_cast<size_t>(50 * (i + 1)), 'X'));
            file.Sync();
        }

        // Act
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

        // Assert - All files should be returned even across multiple pages
        EXPECT_EQ(fileCount, attributes.size()) << "Pagination should return all files";

        // Verify all files have correct sizes
        for (int i = 0; i < fileCount; ++i)
        {
            std::string filename = "file" + std::to_string(i) + ".sst";
            size_t expectedSize = static_cast<size_t>(50 * (i + 1));

            auto it = std::find_if(attributes.begin(), attributes.end(),
                [&filename](const auto& attr) { return attr.GetName() == filename; });

            ASSERT_NE(attributes.end(), it) << "File " << filename << " not found - pagination may have failed";
            EXPECT_EQ(expectedSize, it->GetSize()) << "Wrong size for " << filename;
        }
    }

    // Cleanup
    [[maybe_unused]] size_t remaining = m_filesystem->DeleteDir(dirPrefix);
}

TEST_F(BlobFilesystemIntegrationTests, GetChildrenFileAttributes_DifferentFileTypes_ReturnsAll)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/file-types-dir-" + GenerateRandomBlobName();
    std::vector<std::pair<std::string, size_t>> fileSpecs = {
        {"data.sst", 1024},
     {"log.log", 512},
        {"manifest.manifest", 256},
        {"current.current", 128}
    };

    {
        for (const auto& [filename, size] : fileSpecs)
        {
            std::string fullPath = dirPrefix + "/" + filename;
            auto file = m_filesystem->CreateWriteableFile(fullPath);
            file.Append(std::vector<char>(size, 'X'));
            file.Sync();
        }

        // Act
        auto attributes = m_filesystem->GetChildrenFileAttributes(dirPrefix + "/");

        // Assert
        EXPECT_EQ(fileSpecs.size(), attributes.size());

        for (const auto& [filename, expectedSize] : fileSpecs)
        {
            auto it = std::find_if(attributes.begin(), attributes.end(),
                [&filename](const auto& attr) { return attr.GetName() == filename; });

            ASSERT_NE(attributes.end(), it) << "File " << filename << " not found";
            EXPECT_EQ(expectedSize, it->GetSize());
        }
    }

    // Cleanup
    [[maybe_unused]] size_t remaining = m_filesystem->DeleteDir(dirPrefix);
}

TEST_F(BlobFilesystemIntegrationTests, DeleteDir_RemovesAllFilesInDirectory)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/delete-dir-" + GenerateRandomBlobName();
    std::string file1 = dirPrefix + "/file1.sst";
    std::string file2 = dirPrefix + "/file2.log";
    std::string file3 = dirPrefix + "/subdir/file3.sst";
    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        auto f2 = m_filesystem->CreateWriteableFile(file2);
        auto f3 = m_filesystem->CreateWriteableFile(file3);

        EXPECT_TRUE(m_filesystem->FileExists(file1));
        EXPECT_TRUE(m_filesystem->FileExists(file2));
        EXPECT_TRUE(m_filesystem->FileExists(file3));
    }

    // Act
    size_t remainingFiles = m_filesystem->DeleteDir(dirPrefix);

    // Assert
    EXPECT_EQ(0, remainingFiles);
    EXPECT_FALSE(m_filesystem->FileExists(file1));
    EXPECT_FALSE(m_filesystem->FileExists(file2));
    EXPECT_FALSE(m_filesystem->FileExists(file3));
}

// ============================================================================
// File Rename Tests
// ============================================================================

TEST_F(BlobFilesystemIntegrationTests, RenameFile_MovesDataCorrectly)
{
    // Arrange
    std::vector<char> testData(1024);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    std::string originalName = m_containerPrefix + "/original-" + m_blobName;
    std::string newName = m_containerPrefix + "/renamed-" + m_blobName;

    {
        auto file = m_filesystem->CreateWriteableFile(originalName);
        file.Append(testData);
        file.Sync();  // Sync to update size metadata
    }

    EXPECT_TRUE(m_filesystem->FileExists(originalName));
    EXPECT_FALSE(m_filesystem->FileExists(newName));

    // Act
    m_filesystem->RenameFile(originalName, newName);

    // Assert
    EXPECT_FALSE(m_filesystem->FileExists(originalName));
    EXPECT_TRUE(m_filesystem->FileExists(newName));
    EXPECT_EQ(testData.size(), m_filesystem->GetFileSize(newName));

    // Verify data integrity
    {
        auto readFile = m_filesystem->CreateReadableFile(newName);
        std::vector<char> readBuffer(testData.size());
        const auto bytesRead = readFile.RandomRead(0, static_cast<int64_t>(testData.size()), readBuffer.data());
        EXPECT_EQ(testData, readBuffer);
        EXPECT_EQ(bytesRead, testData.size());
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(newName));
}

TEST_F(BlobFilesystemIntegrationTests, Truncate_ReducesFileSize)
{
    // Arrange
    std::vector<char> testData(2048, 'X');
    CreateBlobWithData(testData);
    const auto path = m_containerPrefix + "/" + m_blobName;

    EXPECT_EQ(2048, m_filesystem->GetFileSize(path));

    // Act
    m_filesystem->Truncate(path, 1024);

    // Assert
    EXPECT_EQ(1024, m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, Truncate_ToLargerSize_DoesNothing)
{
    // Arrange
    std::vector<char> testData(1024, 'X');
    CreateBlobWithData(testData);
    const auto path = m_containerPrefix + "/" + m_blobName;

    EXPECT_EQ(1024, m_filesystem->GetFileSize(path));

    // Act
    m_filesystem->Truncate(path, 2048);

    // Assert - size should remain unchanged
    EXPECT_EQ(1024, m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, LockFile_SuccessfullyAcquiresLock)
{
    // Arrange
    std::string lockFileName = m_containerPrefix + "/lock-" + m_blobName;

    // Act
    auto lock = m_filesystem->LockFile(lockFileName);

    // Assert
    EXPECT_NE(nullptr, lock);
    EXPECT_EQ(1, m_filesystem->GetLeaseClientCount());

    // Cleanup
    m_filesystem->UnlockFile(*lock);
    EXPECT_EQ(0, m_filesystem->GetLeaseClientCount());
    EXPECT_TRUE(m_filesystem->DeleteFile(lockFileName));
}

TEST_F(BlobFilesystemIntegrationTests, UnlockFile_ReleasesLock)
{
    // Arrange
    std::string lockFileName = m_containerPrefix + "/lock-" + m_blobName;
    auto lock = m_filesystem->LockFile(lockFileName);
    EXPECT_EQ(1, m_filesystem->GetLeaseClientCount());

    // Act
    bool unlocked = m_filesystem->UnlockFile(*lock);

    // Assert
    EXPECT_TRUE(unlocked);
    EXPECT_EQ(0, m_filesystem->GetLeaseClientCount());

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(lockFileName));
}

TEST_F(BlobFilesystemIntegrationTests, UnlockFile_InvalidLock_ReturnsFalse)
{
    // Arrange
    std::string lockFileName1 = m_containerPrefix + "/lock1-" + m_blobName;
    std::string lockFileName2 = m_containerPrefix + "/lock2-" + m_blobName;

    auto lock1 = m_filesystem->LockFile(lockFileName1);
    auto lock2 = m_filesystem->LockFile(lockFileName2);

    // Unlock lock1
    m_filesystem->UnlockFile(*lock1);

    // Act - try to unlock lock1 again
    bool unlocked = m_filesystem->UnlockFile(*lock1);

    // Assert
    EXPECT_FALSE(unlocked);

    // Cleanup
    m_filesystem->UnlockFile(*lock2);
    EXPECT_TRUE(m_filesystem->DeleteFile(lockFileName1));
    EXPECT_TRUE(m_filesystem->DeleteFile(lockFileName2));
}

TEST_F(BlobFilesystemIntegrationTests, ReopenWriteableFile_PreservesExistingData)
{
    // Arrange
    const auto path = m_containerPrefix + "/" + m_blobName;
    std::vector<char> initialData(512, 'A');
    auto file = m_filesystem->CreateWriteableFile(path);
    file.Append(initialData);
    file.Sync();  // Sync to update size metadata

    // Act - Reopen the file
    auto reopenedFile = m_filesystem->ReopenWriteableFile(path);

    // Assert - File should still exist with same data
    EXPECT_EQ(initialData.size(), m_filesystem->GetFileSize(path));

    // Verify we can append more data
    std::vector<char> newData(512, 'B');
    reopenedFile.Append(newData);
    reopenedFile.Sync();  // Sync to update size metadata

    EXPECT_EQ(initialData.size() + newData.size(), m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, ReuseWritableFile_CreatesNewFile)
{
    // Arrange
    const auto path = m_containerPrefix + "/" + m_blobName;
    std::vector<char> oldData(1024, 'X');
    auto file = m_filesystem->CreateWriteableFile(path);
    file.Append(oldData);
    file.Sync();  // Sync to update size metadata

    EXPECT_EQ(oldData.size(), m_filesystem->GetFileSize(path));

    // Act - Reuse the file (should delete and recreate)
    auto reusedFile = m_filesystem->ReuseWritableFile(path);

    // Assert - File should be reset
    EXPECT_TRUE(m_filesystem->FileExists(path));
    EXPECT_EQ(0, m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, CreateLogger_CreatesLogFile)
{
    // Arrange
    std::string logFileName = m_containerPrefix + "/" + m_blobName;

    // Act
    auto logger = m_filesystem->CreateLogger(logFileName, 1);

    // Assert
    EXPECT_TRUE(m_filesystem->FileExists(logFileName));
}

TEST_F(BlobFilesystemIntegrationTests, CreateDirectory_ReturnsDirectoryImpl)
{
    // Arrange
    std::string dirPath = m_containerPrefix + "/test-directory-" + GenerateRandomBlobName();

    // Act
    auto directory = m_filesystem->CreateDirectory(dirPath);

    // Assert - Directory operations should work
    // Note: Azure Blob Storage doesn't have real directories,
    // but we should be able to create a DirectoryImpl object
    EXPECT_NO_THROW(
        {
            // Fsync is a no-op but should not throw
            directory.Fsync();
        });
}

TEST_F(BlobFilesystemIntegrationTests, CreateWriteableFile_SameFileTwice_TruncatesExisting)
{
    // Arrange
    const auto path = m_containerPrefix + "/" + m_blobName;
    std::vector<char> firstData(1024, 'A');
    auto file1 = m_filesystem->CreateWriteableFile(path);
    file1.Append(firstData);
    file1.Sync();  // Sync to update size metadata

    EXPECT_EQ(firstData.size(), m_filesystem->GetFileSize(path));

    // Act - Create the same file again
    auto file2 = m_filesystem->CreateWriteableFile(path);

    // Assert - File should be truncated
    EXPECT_EQ(0, m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, GetChildren_WithSizeHint_ReturnsResults)
{
    // Arrange
    std::string dirPrefix = m_containerPrefix + "/hint-dir-" + GenerateRandomBlobName();

    for (int i = 0; i < 5; ++i)
    {
        std::string fileName = dirPrefix + "/file" + std::to_string(i) + ".sst";
        {
            auto f = m_filesystem->CreateWriteableFile(fileName);
        }
    }

    // Act
    auto children = m_filesystem->GetChildren(dirPrefix, 3);

    // Assert - Should return all files even with small size hint
    // Now that pagination is properly implemented, all 5 files should be returned
    EXPECT_EQ(5, children.size());

    // Cleanup
    [[maybe_unused]] size_t remaining = m_filesystem->DeleteDir(dirPrefix);
}

TEST_F(BlobFilesystemIntegrationTests, RenameFile_LargeFile_HandlesCorrectly)
{
    // Arrange - Create a file larger than buffer size to test chunked rename.
  // The file should be larger than 5MB so that request limits are verified to be below
    // what is acceptable by azure.
    const size_t largeSize = static_cast<size_t>(6) * 1024 * 1024; // 6Mb
    std::vector<char> largeData(largeSize);
    for (size_t i = 0; i < largeData.size(); ++i)
    {
        largeData[i] = static_cast<char>(i % 256);
    }

    std::string originalName = m_containerPrefix + "/large-original-" + m_blobName;
    std::string newName = m_containerPrefix + "/large-renamed-" + m_blobName;

    auto file = m_filesystem->CreateWriteableFile(originalName);
    file.Append(largeData);
    file.Sync();  // Sync to update size metadata

    // Act
    m_filesystem->RenameFile(originalName, newName);

    // Assert
    EXPECT_FALSE(m_filesystem->FileExists(originalName));
    EXPECT_TRUE(m_filesystem->FileExists(newName));
    EXPECT_EQ(largeSize, m_filesystem->GetFileSize(newName));

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(newName));
}

TEST_F(BlobFilesystemIntegrationTests, FileExists_DirectoryPath_ReturnsTrueIfHasChildren)
{
    // Arrange - Create a directory structure with files
    std::string dirPrefix = m_containerPrefix + "/dir-check-" + GenerateRandomBlobName();
    std::string file1 = dirPrefix + "/file1.sst";

    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
    }

    // Act & Assert - Directory path should return true since it has children
    EXPECT_TRUE(m_filesystem->FileExists(dirPrefix));

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(file1));

    // After cleanup, directory should not exist
    EXPECT_FALSE(m_filesystem->FileExists(dirPrefix));
}

TEST_F(BlobFilesystemIntegrationTests, FileExists_EmptyDirectoryPath_ReturnsFalse)
{
    // Arrange - Create a path that looks like a directory but has no children
    std::string emptyDirPath = m_containerPrefix + "/empty-check-" + GenerateRandomBlobName();

    // Act & Assert - Empty directory path should return false
    EXPECT_FALSE(m_filesystem->FileExists(emptyDirPath));
}

TEST_F(BlobFilesystemIntegrationTests, RenameFile_DestinationExists_OverwritesFile)
{
    // Arrange - Create both source and destination files
    std::vector<char> sourceData(512, 'S');
    std::vector<char> destData(256, 'D');

    std::string sourcePath = m_containerPrefix + "/rename-src-" + m_blobName;
    std::string destPath = m_containerPrefix + "/rename-dst-" + m_blobName;

    {
        auto srcFile = m_filesystem->CreateWriteableFile(sourcePath);
        srcFile.Append(sourceData);
        srcFile.Sync();

        auto dstFile = m_filesystem->CreateWriteableFile(destPath);
        dstFile.Append(destData);
        dstFile.Sync();
    }

    EXPECT_EQ(512, m_filesystem->GetFileSize(sourcePath));
    EXPECT_EQ(256, m_filesystem->GetFileSize(destPath));

    // Act - Rename source to destination (should overwrite)
    m_filesystem->RenameFile(sourcePath, destPath);

    // Assert - Source should be gone, destination should have source data
    EXPECT_FALSE(m_filesystem->FileExists(sourcePath));
    EXPECT_TRUE(m_filesystem->FileExists(destPath));
    EXPECT_EQ(512, m_filesystem->GetFileSize(destPath));

    // Verify data integrity
    {
        auto readFile = m_filesystem->CreateReadableFile(destPath);
        std::vector<char> readBuffer(sourceData.size());
        const auto bytesRead = readFile.RandomRead(0, static_cast<int64_t>(sourceData.size()), readBuffer.data());
        EXPECT_EQ(sourceData, readBuffer);
        EXPECT_EQ(bytesRead, sourceData.size());
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(destPath));
}

TEST_F(BlobFilesystemIntegrationTests, RenameFile_NonExistentSource_ThrowsOrFails)
{
    // Arrange
    std::string nonExistentSource = m_containerPrefix + "/nonexistent-src-" + m_blobName;
    std::string destPath = m_containerPrefix + "/rename-dest-" + m_blobName;

    // Act & Assert - Should throw or fail when source doesn't exist
    EXPECT_THROW(
        {
 m_filesystem->RenameFile(nonExistentSource, destPath);
        },
        std::exception
    );

    // Destination should not have been created
    EXPECT_FALSE(m_filesystem->FileExists(destPath));
}

TEST_F(BlobFilesystemIntegrationTests, LockFile_AlreadyLocked_ThrowsException)
{
    // Arrange - Lock a file first
    std::string lockFileName = m_containerPrefix + "/double-lock-" + m_blobName;
    auto firstLock = m_filesystem->LockFile(lockFileName);
    EXPECT_NE(nullptr, firstLock);
    EXPECT_EQ(1, m_filesystem->GetLeaseClientCount());

    // Act & Assert - Attempting to lock again should throw
    EXPECT_THROW(
        {
            auto secondLock = m_filesystem->LockFile(lockFileName);
        },
        std::exception
    );

    // Cleanup
    m_filesystem->UnlockFile(*firstLock);
    EXPECT_TRUE(m_filesystem->DeleteFile(lockFileName));
}

TEST_F(BlobFilesystemIntegrationTests, LockFile_AfterUnlock_SucceedsOnSecondAttempt)
{
    // Arrange - Lock and then unlock a file
    std::string lockFileName = m_containerPrefix + "/relock-" + m_blobName;
    auto firstLock = m_filesystem->LockFile(lockFileName);
    EXPECT_NE(nullptr, firstLock);

    m_filesystem->UnlockFile(*firstLock);
    EXPECT_EQ(0, m_filesystem->GetLeaseClientCount());

    // Act - Lock the same file again after unlocking
    auto secondLock = m_filesystem->LockFile(lockFileName);

    // Assert - Should succeed
    EXPECT_NE(nullptr, secondLock);
    EXPECT_EQ(1, m_filesystem->GetLeaseClientCount());

    // Cleanup
    m_filesystem->UnlockFile(*secondLock);
    EXPECT_TRUE(m_filesystem->DeleteFile(lockFileName));
}

TEST_F(BlobFilesystemIntegrationTests, Truncate_NonExistentFile_ThrowsOrFails)
{
    // Arrange
    std::string nonExistentFile = m_containerPrefix + "/nonexistent-truncate-" + m_blobName;

    // Act & Assert - Truncating non-existent file should throw
    EXPECT_THROW(
        {
            m_filesystem->Truncate(nonExistentFile, 100);
        },
        std::exception
    );
}

TEST_F(BlobFilesystemIntegrationTests, Truncate_ToZero_ReducesToZero)
{
    // Arrange
    std::vector<char> testData(1024, 'Z');
    CreateBlobWithData(testData);
    const auto path = m_containerPrefix + "/" + m_blobName;

    EXPECT_EQ(1024, m_filesystem->GetFileSize(path));

    // Act
    m_filesystem->Truncate(path, 0);

    // Assert
    EXPECT_EQ(0, m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, GetFileSize_NonExistentFile_ThrowsOrFails)
{
    // Arrange
    std::string nonExistentFile = m_containerPrefix + "/nonexistent-size-" + m_blobName;

    // Act & Assert - Getting size of non-existent file should throw
    EXPECT_THROW(
        {
            [[maybe_unused]] auto size = m_filesystem->GetFileSize(nonExistentFile);
        },
        std::exception
    );
}

TEST_F(BlobFilesystemIntegrationTests, GetFileModificationTime_NonExistentFile_ThrowsOrFails)
{
    // Arrange
    std::string nonExistentFile = m_containerPrefix + "/nonexistent-modtime-" + m_blobName;

    // Act & Assert - Getting modification time of non-existent file should throw
    EXPECT_THROW(
        {
            [[maybe_unused]] auto modTime = m_filesystem->GetFileModificationTime(nonExistentFile);
        },
        std::exception
    );
}

TEST_F(BlobFilesystemIntegrationTests, CreateReadableFile_NonExistentFile_ThrowsOrFails)
{
    // Arrange
    std::string nonExistentFile = m_containerPrefix + "/nonexistent-read-" + m_blobName;

    // Act & Assert - Creating readable file from non-existent blob should throw
    EXPECT_THROW(
        {
            auto file = m_filesystem->CreateReadableFile(nonExistentFile);
        },
        std::exception
    );
}

TEST_F(BlobFilesystemIntegrationTests, ReopenWriteableFile_NonExistentFile_ThrowsOrFails)
{
    // Arrange
    std::string nonExistentFile = m_containerPrefix + "/nonexistent-reopen-" + m_blobName;

    // Act & Assert - Reopening non-existent file should throw
    EXPECT_THROW(
        {
            auto file = m_filesystem->ReopenWriteableFile(nonExistentFile);
        },
        std::exception
    );
}

TEST_F(BlobFilesystemIntegrationTests, DeleteDir_EmptyDirectory_ReturnsZero)
{
    // Arrange - Create an empty directory path (no files)
    std::string emptyDirPrefix = m_containerPrefix + "/empty-delete-" + GenerateRandomBlobName();

    // Act
    size_t remainingFiles = m_filesystem->DeleteDir(emptyDirPrefix);

    // Assert - No files to delete, should return 0
    EXPECT_EQ(0, remainingFiles);
}

TEST_F(BlobFilesystemIntegrationTests, DeleteDir_RootPath_DeletesAllInContainer)
{
    // Arrange - Create files in root and subdirectories
    std::string uniquePrefix = m_containerPrefix + "/root-delete-" + GenerateRandomBlobName();
    std::string file1 = uniquePrefix + "/file1.sst";
    std::string file2 = uniquePrefix + "/subdir/file2.sst";

    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        auto f2 = m_filesystem->CreateWriteableFile(file2);
    }

    EXPECT_TRUE(m_filesystem->FileExists(file1));
    EXPECT_TRUE(m_filesystem->FileExists(file2));

    // Act - Delete with trailing slash (mimics root deletion)
    const auto remainingFiles = m_filesystem->DeleteDir(uniquePrefix);

    // Assert
    EXPECT_EQ(0, remainingFiles);
    EXPECT_FALSE(m_filesystem->FileExists(file1));
    EXPECT_FALSE(m_filesystem->FileExists(file2));
}

TEST_F(BlobFilesystemIntegrationTests, CreateWriteableFile_VeryLongPath_HandlesCorrectly)
{
    // Arrange - Create a very long path with nested directories
    std::string longPath = m_containerPrefix;
    for (int i = 0; i < 10; ++i)
    {
        longPath += "/verylongdirectoryname" + std::to_string(i);
    }
    longPath += "/file.sst";

    // Act - Should handle long paths
    {
        auto file = m_filesystem->CreateWriteableFile(longPath);
        file.Append(std::vector<char>(100, 'L'));
    }

    // Assert
    EXPECT_TRUE(m_filesystem->FileExists(longPath));
    EXPECT_EQ(100, m_filesystem->GetFileSize(longPath));

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(longPath));
}

TEST_F(BlobFilesystemIntegrationTests, CreateWriteableFile_SpecialCharactersInName_HandlesCorrectly)
{
    // Arrange - Create file with special characters (that are valid in blob names)
    std::string specialPath = m_containerPrefix + "/special-chars-file_name.with-dashes.sst";

    // Act
    {
        auto file = m_filesystem->CreateWriteableFile(specialPath);
        file.Append(std::vector<char>(50, 'X'));
    }

    // Assert
    EXPECT_TRUE(m_filesystem->FileExists(specialPath));
    EXPECT_EQ(50, m_filesystem->GetFileSize(specialPath));

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(specialPath));
}

TEST_F(BlobFilesystemIntegrationTests, GetChildren_VeryLongDirectoryPath_HandlesCorrectly)
{
    // Arrange - Create files in a very nested directory
    std::string longDirPath = m_containerPrefix;
    for (int i = 0; i < 5; ++i)
    {
        longDirPath += "/nested" + std::to_string(i);
    }

    std::string file1 = longDirPath + "/file1.sst";
    std::string file2 = longDirPath + "/file2.sst";

    {
        auto f1 = m_filesystem->CreateWriteableFile(file1);
        auto f2 = m_filesystem->CreateWriteableFile(file2);
    }

    // Act
    auto children = m_filesystem->GetChildren(longDirPath);

    // Assert
    EXPECT_EQ(2, children.size());
    EXPECT_TRUE(std::find(children.begin(), children.end(), "file1.sst") != children.end());
    EXPECT_TRUE(std::find(children.begin(), children.end(), "file2.sst") != children.end());

    // Cleanup
    [[maybe_unused]] const auto remaining = m_filesystem->DeleteDir(longDirPath);
}

TEST_F(BlobFilesystemIntegrationTests, RenameFile_SameSourceAndDestination_HandlesGracefully)
{
    // Arrange - Create a file
    std::vector<char> testData(256, 'R');
    std::string filePath = m_containerPrefix + "/same-rename-" + m_blobName;

    {
        auto file = m_filesystem->CreateWriteableFile(filePath);
        file.Append(testData);
        file.Sync();
    }

    EXPECT_EQ(256, m_filesystem->GetFileSize(filePath));

    // Act - Rename to same path (should be a no-op)
    m_filesystem->RenameFile(filePath, filePath);

    // Assert - File should still exist with same data (operation was skipped)
    EXPECT_TRUE(m_filesystem->FileExists(filePath));
    EXPECT_EQ(256, m_filesystem->GetFileSize(filePath));

    // Verify data integrity
    {
        auto readFile = m_filesystem->CreateReadableFile(filePath);
        std::vector<char> readBuffer(testData.size());
        const auto bytesRead = readFile.RandomRead(0, static_cast<int64_t>(testData.size()), readBuffer.data());
        EXPECT_EQ(testData, readBuffer);
        EXPECT_EQ(bytesRead, testData.size());
    }

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(filePath));
}

TEST_F(BlobFilesystemIntegrationTests, CreateWriteableFile_AfterDelete_RecreatesSuccessfully)
{
    // Arrange - Create, delete, then recreate the same file
    const auto path = m_containerPrefix + "/" + m_blobName;

    // First creation
    {
        auto file1 = m_filesystem->CreateWriteableFile(path);
        file1.Append(std::vector<char>(512, 'A'));
        file1.Sync();
    }
    EXPECT_EQ(512, m_filesystem->GetFileSize(path));

    // Delete
    EXPECT_TRUE(m_filesystem->DeleteFile(path));
    EXPECT_FALSE(m_filesystem->FileExists(path));

    // Act - Recreate
    {
        auto file2 = m_filesystem->CreateWriteableFile(path);
        file2.Append(std::vector<char>(1024, 'B'));
        file2.Sync();
    }

    // Assert - New file should exist with new size
    EXPECT_TRUE(m_filesystem->FileExists(path));
    EXPECT_EQ(1024, m_filesystem->GetFileSize(path));
}

TEST_F(BlobFilesystemIntegrationTests, SequentialRead_ETagMismatch_RefreshesAndRetriess)
{
    // Arrange
    std::string blobName = m_containerPrefix + "/original-" + m_blobName;

    std::vector<char> initialData(512, 'a');
    auto file = m_filesystem->CreateWriteableFile(blobName);
    file.Append(initialData);
    file.Sync();  // Sync to update size metadata

    auto readFile = m_filesystem->CreateReadableFile(blobName);
    std::vector<char> readBuffer(512);
    auto bytesRead = readFile.SequentialRead(static_cast<int64_t>(512), readBuffer.data());
    EXPECT_EQ(512, bytesRead);
    EXPECT_EQ(512, readFile.GetSize());
    EXPECT_TRUE(std::all_of(readBuffer.begin(), readBuffer.end(), [](char c) { return c == 'a'; }));

    // Act
    std::vector<char> updatedData(512, 'b');
    file.Append(updatedData);
    file.Sync();  // Sync to update size metadata

    // Assert    
    std::vector<char> readAppendedBuffer(1024);
    bytesRead = readFile.SequentialRead(static_cast<int64_t>(1024), readAppendedBuffer.data());
    EXPECT_EQ(512, bytesRead);
    EXPECT_EQ(1024, readFile.GetSize());
    EXPECT_EQ(512, std::count(readAppendedBuffer.begin(), readAppendedBuffer.begin() + 512, 'b'));    

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(blobName));
}

TEST_F(BlobFilesystemIntegrationTests, RandomRead_ETagMismatch_RefreshesAndRetriess)
{
    // Arrange
    std::string blobName = m_containerPrefix + "/original-" + m_blobName;

    std::vector<char> initialData(512, 'a');
    auto file = m_filesystem->CreateWriteableFile(blobName);
    file.Append(initialData);
    file.Sync();  // Sync to update size metadata

    auto readFile = m_filesystem->CreateReadableFile(blobName);
    std::vector<char> readBuffer(512);
    auto bytesRead = readFile.RandomRead(0, static_cast<int64_t>(512), readBuffer.data());
    EXPECT_EQ(512, bytesRead);
    EXPECT_EQ(512, readFile.GetSize());
    EXPECT_TRUE(std::all_of(readBuffer.begin(), readBuffer.end(), [](char c) { return c == 'a'; }));

    // Act
    std::vector<char> updatedData(512, 'b');
    file.Append(updatedData);
    file.Sync();  // Sync to update size metadata

    // Assert    
    std::vector<char> readAppendedBuffer(1024);
    bytesRead = readFile.RandomRead(0, static_cast<int64_t>(1024), readAppendedBuffer.data());
    EXPECT_EQ(1024, bytesRead);
    EXPECT_EQ(1024, readFile.GetSize());
    EXPECT_EQ(512, std::count(readAppendedBuffer.begin(), readAppendedBuffer.begin() + 512, 'a'));
    EXPECT_EQ(512, std::count(readAppendedBuffer.begin() + 512, readAppendedBuffer.end(), 'b'));

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(blobName));
}

TEST_F(BlobFilesystemIntegrationTests, RandomRead_AfterBlobGrows_UpdatesSize)
{
    // Arrange
    std::string blobName = m_containerPrefix + "/original-" + m_blobName;
    std::vector<char> initialData(256, 'Z');
    auto writeFile = m_filesystem->CreateWriteableFile(blobName);
    writeFile.Append(initialData);
    writeFile.Sync();

    auto readFile = m_filesystem->CreateReadableFile(blobName);
    EXPECT_EQ(256, readFile.GetSize());

    // Act
    std::vector<char> appendData(512, 'Y');
    auto reopenFile = m_filesystem->ReopenWriteableFile(blobName);
    reopenFile.Append(appendData);
    reopenFile.Sync();

    // Assert
    std::vector<char> buffer(100);
    int64_t bytesRead = readFile.RandomRead(300, 100, buffer.data());
    EXPECT_EQ(100, bytesRead);
    EXPECT_TRUE(std::all_of(buffer.begin(), buffer.end(), [](char c) { return c == 'Y'; }));
    EXPECT_EQ(768, readFile.GetSize());

    // Cleanup
    EXPECT_TRUE(m_filesystem->DeleteFile(blobName));
}
