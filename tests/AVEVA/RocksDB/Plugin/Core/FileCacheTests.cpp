// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FilesystemMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/ContainerClientMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FileMock.hpp"

#include <gtest/gtest.h>

#include <unordered_set>
using boost::log::trivial::severity_level;
using boost::log::sources::severity_logger_mt;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::Matcher;
using AVEVA::RocksDB::Plugin::Core::FileCache;
using AVEVA::RocksDB::Plugin::Core::Mocks::FilesystemMock;
using AVEVA::RocksDB::Plugin::Core::Mocks::ContainerClientMock;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using AVEVA::RocksDB::Plugin::Core::Mocks::FileMock;
class FileCacheTests : public ::testing::Test
{
protected:
    static const constexpr std::string_view m_folderName = "tmp";
    std::shared_ptr<FilesystemMock> m_filesystem;
    std::shared_ptr<ContainerClientMock> m_containerClient;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger;
    std::vector<std::filesystem::path> m_removedFiles;
    FileCache m_cache;

public:
    FileCacheTests()
        : m_filesystem(std::make_shared<FilesystemMock>()),
        m_containerClient(std::make_shared<ContainerClientMock>()),
        m_logger(std::make_shared<severity_logger_mt<severity_level>>()),
        m_cache(m_folderName, static_cast<size_t>(1073741824), m_containerClient, m_filesystem, m_logger)
    {
        ON_CALL(*m_filesystem, DeleteFile(_))
            .WillByDefault([this](const std::filesystem::path& path)
                {
                    m_removedFiles.push_back(path);
                    return true;
                });
    }

    void EnsureReadFromCache(const std::string_view filePath)
    {
        while (true)
        {
            const auto bytesRead = m_cache.ReadFile(filePath, 0, 0, nullptr);
            if (bytesRead)
            {
                break;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    void EnsureReadFromCache(const std::string_view filePath, const size_t expectedCacheSize)
    {
        char buffer[1];
        while (true)
        {
            const auto bytesRead = m_cache.ReadFile(filePath, 0, 1, buffer);
            if (bytesRead)
            {
                ASSERT_EQ(expectedCacheSize, m_cache.CacheSize());
                break;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }
};

TEST_F(FileCacheTests, ReadEvenSpaced256MbFiles)
{
    // Arrange
    const auto fileSize = static_cast<size_t>(268435456);
    EXPECT_CALL(*m_containerClient, GetBlobClient(_))
        .WillRepeatedly(Invoke([fileSize](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize));
                return blob;
            }));

    EXPECT_CALL(*m_filesystem, Open(_))
        .WillRepeatedly(Invoke([](const std::filesystem::path&)
            {
                auto file = std::make_unique<FileMock>();
                EXPECT_CALL(*file, Read(_, _, _))
                    .WillRepeatedly(Invoke([](char*, uint64_t offset, uint64_t length)
                        {
                            return length - offset;
                        }));

                return file;
            }));

    // Act
    EnsureReadFromCache("1.sst", fileSize);
    EnsureReadFromCache("2.sst", fileSize * 2);
    EnsureReadFromCache("3.sst", fileSize * 3);
    EnsureReadFromCache("4.sst", fileSize * 4);
    EnsureReadFromCache("5.sst", fileSize * 4);

    // Assert
    std::unordered_set<std::string> expectedFiles =
    {
        "1.sst", "2.sst", "3.sst",
        "4.sst", "5.sst"
    };

    EXPECT_EQ(1, m_removedFiles.size());
    for (const auto& removed : m_removedFiles)
    {
        expectedFiles.erase(removed.filename().string());
    }

    for (const auto& expected : expectedFiles)
    {
        EXPECT_TRUE(m_cache.HasFile(expected));
    }
}

TEST_F(FileCacheTests, ReadRandomLargeFiles_EvictionWorks)
{
    // Arrange
    const auto fileSize1 = static_cast<size_t>(268435456);
    const auto fileSize2 = static_cast<size_t>(76612355);
    const auto fileSize3 = static_cast<size_t>(16612355);
    const auto fileSize4 = static_cast<size_t>(176612355);
    const auto fileSize5 = static_cast<size_t>(330812579);
    const auto fileSize6 = static_cast<size_t>(509715200);

    EXPECT_CALL(*m_containerClient, GetBlobClient("1.sst"))
        .WillRepeatedly(Invoke([fileSize1](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize1));
                return blob;
            }));

    EXPECT_CALL(*m_containerClient, GetBlobClient("2.sst"))
        .WillRepeatedly(Invoke([fileSize2](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize2));
                return blob;
            }));

    EXPECT_CALL(*m_containerClient, GetBlobClient("3.sst"))
        .WillRepeatedly(Invoke([fileSize3](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize3));
                return blob;
            }));

    EXPECT_CALL(*m_containerClient, GetBlobClient("4.sst"))
        .WillRepeatedly(Invoke([fileSize4](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize4));
                return blob;
            }));

    EXPECT_CALL(*m_containerClient, GetBlobClient("5.sst"))
        .WillRepeatedly(Invoke([fileSize5](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize5));
                return blob;
            }));

    EXPECT_CALL(*m_containerClient, GetBlobClient("6.sst"))
        .WillRepeatedly(Invoke([fileSize6](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize6));
                return blob;
            }));

    EXPECT_CALL(*m_filesystem, Open(_))
        .WillRepeatedly(Invoke([](const std::filesystem::path&)
            {
                auto file = std::make_unique<FileMock>();
                EXPECT_CALL(*file, Read(_, _, _))
                    .WillRepeatedly(Invoke([](char*, uint64_t offset, uint64_t length)
                        {
                            return length - offset;
                        }));

                return file;
            }));

    // Act
    EnsureReadFromCache("1.sst", fileSize1);
    EnsureReadFromCache("2.sst", fileSize1 + fileSize2);
    EnsureReadFromCache("3.sst", fileSize1 + fileSize2 + fileSize3);
    EnsureReadFromCache("4.sst", fileSize1 + fileSize2 + fileSize3 + fileSize4);
    EnsureReadFromCache("5.sst", fileSize1 + fileSize2 + fileSize3 + fileSize4 + fileSize5);
    EnsureReadFromCache("6.sst", fileSize3 + fileSize4 + fileSize5 + fileSize6);

    // Assert
    std::unordered_set<std::string> expectedFiles =
    {
        "1.sst", "2.sst", "3.sst",
        "4.sst", "5.sst", "6.sst"
    };

    for (const auto& removed : m_removedFiles)
    {
        expectedFiles.erase(removed.filename().string());
    }

    for (const auto& expected : expectedFiles)
    {
        EXPECT_TRUE(m_cache.HasFile(expected));
    }
}

TEST_F(FileCacheTests, ReadFileFromCache)
{
    // Arrange
    std::string fileData = "Hello, World!";
    EXPECT_CALL(*m_containerClient, GetBlobClient("1.sst"))
        .WillRepeatedly(Invoke([&fileData](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileData.size()));
                EXPECT_CALL(*blob, DownloadTo(Matcher<const std::string&>(_), _, _))
                    .Times(1);

                return blob;
            }));
    EXPECT_CALL(*m_filesystem, Open(std::filesystem::path(m_folderName) / "1.sst"))
        .WillRepeatedly(Invoke([&fileData](const std::filesystem::path&)
            {
                auto file = std::make_unique<FileMock>();
                EXPECT_CALL(*file, Read(_, _, _))
                    .WillRepeatedly([&fileData](char* buffer, uint64_t offset, uint64_t length) -> uint64_t
                        {
                            std::copy(fileData.data() + offset, fileData.data() + offset + length, buffer);
                            return length - offset;
                        });
                return file;
            }));
    EnsureReadFromCache("1.sst");

    // Act
    std::vector<char> buffer;
    buffer.resize(fileData.size());
    const auto bytesRead = m_cache.ReadFile("1.sst", 0, static_cast<int64_t>(fileData.size()), buffer.data());

    // Assert
    ASSERT_TRUE(bytesRead);
    ASSERT_EQ(*bytesRead, fileData.size());
    ASSERT_EQ(std::string_view(buffer.begin(), buffer.end()), fileData);
}

TEST_F(FileCacheTests, CacheSizeExceeded)
{
    // Arrange
    const auto fileSize = static_cast<size_t>(20000);

    EXPECT_CALL(*m_containerClient, GetBlobClient(_))
        .WillRepeatedly(Invoke([fileSize](const std::string&)
            {
                auto blob = std::make_unique<BlobClientMock>();
                EXPECT_CALL(*blob, GetSize())
                    .WillRepeatedly(Return(fileSize));
                return blob;
            }));

    EXPECT_CALL(*m_filesystem, Open(_))
        .WillRepeatedly(Invoke([](const std::filesystem::path&)
            {
                auto file = std::make_unique<FileMock>();
                EXPECT_CALL(*file, Read(_, _, _))
                    .WillRepeatedly(Invoke([](char*, uint64_t offset, uint64_t length)
                        {
                            return length - offset;
                        }));

                return file;
            }));

    EnsureReadFromCache("1.sst");
    EnsureReadFromCache("2.sst");
    EnsureReadFromCache("3.sst");
    EnsureReadFromCache("4.sst");

    // Act
    m_cache.SetCacheSize(fileSize);

    // Assert
    ASSERT_EQ(fileSize, m_cache.CacheSize());
    ASSERT_EQ(3, m_removedFiles.size());
}
