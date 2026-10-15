// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/ContainerClientMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FileMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FilesystemMock.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <stdexcept>
#include <unordered_set>
using AVEVA::RocksDB::Plugin::Core::FileCache;
using AVEVA::RocksDB::Plugin::Core::Mocks::ContainerClientMock;
using AVEVA::RocksDB::Plugin::Core::Mocks::FileMock;
using AVEVA::RocksDB::Plugin::Core::Mocks::FilesystemMock;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;
using ::testing::_;
using ::testing::Return;
class FileCacheTests : public ::testing::Test {
  protected:
    static const constexpr std::string_view m_folderName = "tmp";
    std::shared_ptr<FilesystemMock> m_filesystem;
    std::shared_ptr<ContainerClientMock> m_containerClient;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger;
    std::vector<std::filesystem::path> m_removedFiles;
    FileCache m_cache;

  public:
    FileCacheTests()
        : m_filesystem(std::make_shared<FilesystemMock>()), m_containerClient(std::make_shared<ContainerClientMock>()),
          m_logger(std::make_shared<severity_logger_mt<severity_level>>()),
          m_cache(m_folderName, static_cast<size_t>(1073741824), m_containerClient, m_filesystem, m_logger) {
        ON_CALL(*m_filesystem, DeleteFile(_)).WillByDefault([this](const std::filesystem::path& path) {
            m_removedFiles.push_back(path);
            return true;
        });
    }

    void EnsureReadFromCache(const std::string_view filePath) {
        // The first access only records interest and the second queues the download, so a few reads suffice.
        constexpr int maxAttempts = 4;
        for (int attempt = 0; attempt < maxAttempts; ++attempt) {
            const auto bytesRead = m_cache.ReadFile(filePath, 0, 0, nullptr);
            if (bytesRead) {
                return;
            }
            m_cache.WaitUntilIdle();
        }
        FAIL() << "'" << filePath << "' did not become readable from cache";
    }

    void EnsureReadFromCache(const std::string_view filePath, const size_t expectedCacheSize) {
        char buffer[1];
        // The first access only records interest and the second queues the download, so a few reads suffice.
        constexpr int maxAttempts = 4;
        for (int attempt = 0; attempt < maxAttempts; ++attempt) {
            const auto bytesRead = m_cache.ReadFile(filePath, 0, 1, buffer);
            if (bytesRead) {
                ASSERT_EQ(expectedCacheSize, m_cache.CacheSize());
                return;
            }
            m_cache.WaitUntilIdle();
        }
        FAIL() << "'" << filePath << "' did not become readable from cache";
    }
};

TEST_F(FileCacheTests, ReadEvenSpaced256MbFiles) {
    // Arrange
    const auto fileSize = static_cast<size_t>(268435456);
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());

    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t offset, uint64_t length) {
            return length - offset;
        });

        return file;
    });

    // Act
    EnsureReadFromCache("1.sst", fileSize);
    EnsureReadFromCache("2.sst", fileSize * 2);
    EnsureReadFromCache("3.sst", fileSize * 3);
    EnsureReadFromCache("4.sst", fileSize * 4);
    EnsureReadFromCache("5.sst", fileSize * 4);

    // Assert
    m_cache.WaitUntilIdle();
    std::unordered_set<std::string> expectedFiles = {"1.sst", "2.sst", "3.sst", "4.sst", "5.sst"};

    EXPECT_EQ(1, m_removedFiles.size());
    for (const auto& removed : m_removedFiles) {
        expectedFiles.erase(removed.filename().string());
    }

    for (const auto& expected : expectedFiles) {
        EXPECT_TRUE(m_cache.HasFile(expected));
    }
}

TEST_F(FileCacheTests, ReadRandomLargeFiles_EvictionWorks) {
    // Arrange
    const auto fileSize1 = static_cast<size_t>(268435456);
    const auto fileSize2 = static_cast<size_t>(76612355);
    const auto fileSize3 = static_cast<size_t>(16612355);
    const auto fileSize4 = static_cast<size_t>(176612355);
    const auto fileSize5 = static_cast<size_t>(330812579);
    const auto fileSize6 = static_cast<size_t>(509715200);

    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(fileSize1));
    EXPECT_CALL(*m_containerClient, GetBlobSize("2.sst")).WillRepeatedly(Return(fileSize2));
    EXPECT_CALL(*m_containerClient, GetBlobSize("3.sst")).WillRepeatedly(Return(fileSize3));
    EXPECT_CALL(*m_containerClient, GetBlobSize("4.sst")).WillRepeatedly(Return(fileSize4));
    EXPECT_CALL(*m_containerClient, GetBlobSize("5.sst")).WillRepeatedly(Return(fileSize5));
    EXPECT_CALL(*m_containerClient, GetBlobSize("6.sst")).WillRepeatedly(Return(fileSize6));

    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t offset, uint64_t length) {
            return length - offset;
        });

        return file;
    });

    // Act
    EnsureReadFromCache("1.sst", fileSize1);
    EnsureReadFromCache("2.sst", fileSize1 + fileSize2);
    EnsureReadFromCache("3.sst", fileSize1 + fileSize2 + fileSize3);
    EnsureReadFromCache("4.sst", fileSize1 + fileSize2 + fileSize3 + fileSize4);
    EnsureReadFromCache("5.sst", fileSize1 + fileSize2 + fileSize3 + fileSize4 + fileSize5);
    EnsureReadFromCache("6.sst", fileSize3 + fileSize4 + fileSize5 + fileSize6);

    // Assert
    m_cache.WaitUntilIdle();
    std::unordered_set<std::string> expectedFiles = {"1.sst", "2.sst", "3.sst", "4.sst", "5.sst", "6.sst"};

    for (const auto& removed : m_removedFiles) {
        expectedFiles.erase(removed.filename().string());
    }

    for (const auto& expected : expectedFiles) {
        EXPECT_TRUE(m_cache.HasFile(expected));
    }
}

TEST_F(FileCacheTests, ReadFileFromCache) {
    // Arrange
    std::string fileData = "Hello, World!";
    std::atomic<int> downloadCount{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(fileData.size()));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _))
        .WillRepeatedly(
            [&downloadCount](const std::string&, const std::string&, int64_t, int64_t) { downloadCount++; });
    EXPECT_CALL(*m_filesystem, Open(std::filesystem::path(m_folderName) / "1.sst"))
        .WillRepeatedly([&fileData](const std::filesystem::path&) {
            auto file = std::make_unique<FileMock>();
            EXPECT_CALL(*file, Read(_, _, _))
                .WillRepeatedly([&fileData](char* buffer, uint64_t offset, uint64_t length) -> uint64_t {
                    std::copy(fileData.data() + offset, fileData.data() + offset + length, buffer);
                    return length - offset;
                });
            return file;
        });
    EnsureReadFromCache("1.sst");
    EXPECT_EQ(1, downloadCount) << "DownloadTo should have been called exactly once";

    // Act
    std::vector<char> buffer;
    buffer.resize(fileData.size());
    const auto bytesRead = m_cache.ReadFile("1.sst", 0, static_cast<int64_t>(fileData.size()), buffer.data());

    // Assert
    ASSERT_TRUE(bytesRead);
    ASSERT_EQ(*bytesRead, fileData.size());
    ASSERT_EQ(std::string_view(buffer.begin(), buffer.end()), fileData);
}

TEST_F(FileCacheTests, EmptyBlobIsDownloadedWithZeroLength) {
    // Arrange
    std::atomic<int64_t> lastLength{-1};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(0));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _))
        .WillRepeatedly(
            [&lastLength](const std::string&, const std::string&, int64_t, int64_t length) { lastLength = length; });
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        return std::make_unique<FileMock>();
    });

    // Act
    EnsureReadFromCache("1.sst");

    // Assert
    EXPECT_EQ(0, lastLength);
    EXPECT_EQ(0U, m_cache.CacheSize());
}

TEST_F(FileCacheTests, CacheSizeExceeded) {
    // Arrange
    const auto fileSize = static_cast<size_t>(20000);

    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());

    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t offset, uint64_t length) {
            return length - offset;
        });

        return file;
    });

    EnsureReadFromCache("1.sst");
    EnsureReadFromCache("2.sst");
    EnsureReadFromCache("3.sst");
    EnsureReadFromCache("4.sst");

    // Act
    m_cache.SetCacheSize(fileSize);

    // Assert
    m_cache.WaitUntilIdle();
    ASSERT_EQ(fileSize, m_cache.CacheSize());
    ASSERT_EQ(3, m_removedFiles.size());
}

TEST_F(FileCacheTests, SetCacheSize_EvictsTheOnlyEntry) {
    // Arrange
    const auto fileSize = static_cast<size_t>(20000);
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t offset, uint64_t length) {
            return length - offset;
        });
        return file;
    });
    EnsureReadFromCache("only.sst");
    ASSERT_EQ(fileSize, m_cache.CacheSize());

    // Act
    m_cache.SetCacheSize(0);

    // Assert
    EXPECT_EQ(0U, m_cache.CacheSize());
    EXPECT_FALSE(m_cache.HasFile("only.sst"));
}

TEST_F(FileCacheTests, FailedSizeLookupLeavesTheEntryDownloadableAgain) {
    // Arrange
    std::atomic<int> sizeCalls = 0;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly([&sizeCalls](const std::string&) -> int64_t {
        if (sizeCalls++ == 0) {
            throw std::runtime_error("boom");
        }
        return 10;
    });
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });

    // Act: the first queued download fails, but later reads must be able to queue it again.
    EnsureReadFromCache("retry.sst");

    // Assert
    EXPECT_GE(sizeCalls.load(), 2);
}

TEST_F(FileCacheTests, ReadFile_FirstAccess_DoesNotStartDownload) {
    // Arrange
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).Times(0);

    // Act
    char buffer[1];
    const auto bytesRead = m_cache.ReadFile("1.sst", 0, 1, buffer);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_FALSE(bytesRead);
    EXPECT_TRUE(m_cache.HasFile("1.sst"));
}

TEST_F(FileCacheTests, ReadFile_SecondAccess_QueuesAndDownloads) {
    // Arrange
    std::string fileData = "X";
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).Times(0);

    // First access should only record stale metadata.
    char buffer[1];
    const auto bytesReadOnFirstAccess = m_cache.ReadFile("1.sst", 0, 1, buffer);
    m_cache.WaitUntilIdle();
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_containerClient.get()));

    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst"))
        .Times(::testing::AtLeast(1))
        .WillRepeatedly(Return(fileData.size()));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly(Return());

    EXPECT_CALL(*m_filesystem, Open(std::filesystem::path(m_folderName) / "1.sst"))
        .Times(::testing::AtLeast(1))
        .WillRepeatedly([&fileData](const std::filesystem::path&) {
            auto file = std::make_unique<FileMock>();
            ON_CALL(*file, Read(_, _, _))
                .WillByDefault([&fileData](char* buf, uint64_t offset, uint64_t length) -> uint64_t {
                    std::copy(fileData.data() + offset, fileData.data() + offset + length, buf);
                    return length - offset;
                });
            return file;
        });

    // Act
    const auto bytesReadOnSecondAccess = m_cache.ReadFile("1.sst", 0, 1, buffer);
    EnsureReadFromCache("1.sst");
    const auto bytesReadOnceCached = m_cache.ReadFile("1.sst", 0, 1, buffer);

    // Assert
    EXPECT_EQ(1U, bytesReadOnceCached);
    EXPECT_FALSE(bytesReadOnFirstAccess);
    EXPECT_FALSE(bytesReadOnSecondAccess);
    EXPECT_TRUE(m_cache.HasFile("1.sst"));
}

TEST_F(FileCacheTests, ReadFile_ShortReadFromTheCachedFile_FallsBackAndDropsTheEntry) {
    // Arrange - the cached file is shorter than the blob size recorded for it.
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(8));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(std::filesystem::path(m_folderName) / "1.sst"))
        .WillRepeatedly([](const std::filesystem::path&) {
            auto file = std::make_unique<FileMock>();
            ON_CALL(*file, Read(_, _, _)).WillByDefault(Return(3));
            return file;
        });
    EnsureReadFromCache("1.sst");
    char buffer[8];

    // Act
    const auto bytesRead = m_cache.ReadFile("1.sst", 0, 8, buffer);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_FALSE(bytesRead.has_value());
    EXPECT_FALSE(m_cache.HasFile("1.sst"));
    EXPECT_EQ(0U, m_cache.CacheSize());
}

TEST_F(FileCacheTests, FileReplacedWhileItsSizeIsBeingLookedUp_IsNotActivated) {
    // Arrange - the size lookup blocks until released.
    std::promise<void> started;
    std::promise<void> release;
    std::atomic<int> sizeCalls{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly([&](const std::string&) -> int64_t {
        if (++sizeCalls == 1) {
            started.set_value();
            release.get_future().wait();
        }
        return 1;
    });
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).Times(0);
    EXPECT_FALSE(m_cache.ReadFile("1.sst", 0, 0, nullptr)); // records the entry
    EXPECT_FALSE(m_cache.ReadFile("1.sst", 0, 0, nullptr)); // queues the download
    started.get_future().wait();

    // Act - the downloader has popped the entry, which is still QueuedForDownload, when the blob is replaced.
    m_cache.MarkFileAsStaleIfExists("1.sst");
    release.set_value();
    m_cache.WaitUntilIdle();

    // Assert - nothing was downloaded or activated with the pre-replacement size.
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_containerClient.get()));
    EXPECT_EQ(0U, m_cache.CacheSize());
    EXPECT_FALSE(m_cache.ReadFile("1.sst", 0, 0, nullptr).has_value());
    EXPECT_GE(sizeCalls, 1);
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    m_cache.WaitUntilIdle();
}

TEST_F(FileCacheTests, FileReplacedAndRequeuedDuringADownload_IsDownloadedAgainNotActivatedWithTheOldBytes) {
    // Arrange - the first download blocks until released; later ones return at once.
    std::promise<void> started;
    std::promise<void> release;
    std::atomic<int> downloads{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(1));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly([&](auto&&...) {
        if (++downloads == 1) {
            started.set_value();
            release.get_future().wait();
        }
    });
    EXPECT_FALSE(m_cache.ReadFile("1.sst", 0, 0, nullptr)); // records the entry
    EXPECT_FALSE(m_cache.ReadFile("1.sst", 0, 0, nullptr)); // queues the download
    started.get_future().wait();

    // Act - the blob is replaced mid-download and a reader asks for it again before the first download ends.
    m_cache.MarkFileAsStaleIfExists("1.sst");
    EXPECT_FALSE(m_cache.ReadFile("1.sst", 0, 0, nullptr));
    release.set_value();
    m_cache.WaitUntilIdle();

    // Assert - the bytes fetched before the replacement were discarded and the file was fetched again.
    EXPECT_EQ(downloads, 2);
    EXPECT_TRUE(m_cache.ReadFile("1.sst", 0, 0, nullptr).has_value());
}

TEST_F(FileCacheTests, ReadFile_TooManyUndownloadedStaleEntries_PrunesOldest) {
    // Arrange
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).Times(0);
    constexpr int staleEntriesToInsert = 2050;

    // Act
    char buffer[1];
    for (int i = 0; i < staleEntriesToInsert; ++i) {
        const auto filePath = std::to_string(i) + ".sst";
        const auto bytesRead = m_cache.ReadFile(filePath, 0, 1, buffer);
        ASSERT_FALSE(bytesRead);
    }
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_FALSE(m_cache.HasFile("0.sst"));
    EXPECT_TRUE(m_cache.HasFile(std::to_string(staleEntriesToInsert - 1) + ".sst"));
}

TEST_F(FileCacheTests, ReadFile_ManyReadsOfACachedFile_OpenItOnce) {
    // Arrange
    std::atomic<int> opens = 0;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(8));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([&opens](const std::filesystem::path&) {
        ++opens;
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    EnsureReadFromCache("1.sst", 8);

    // Act
    char buffer[1];
    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(1U, m_cache.ReadFile("1.sst", 0, 1, buffer));
    }

    // Assert
    EXPECT_EQ(1, opens.load());
}

TEST_F(FileCacheTests, EvictedFile_WhoseDeleteFails_IsLoggedRetriedAndNotCounted) {
    // Arrange
    const auto fileSize = static_cast<size_t>(100);
    m_cache.SetCacheSize(fileSize);
    std::atomic<int> deleteCalls = 0;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    ON_CALL(*m_filesystem, DeleteFile(_)).WillByDefault([&deleteCalls](const std::filesystem::path&) -> bool {
        if (deleteCalls++ == 0) {
            throw std::runtime_error("locked");
        }
        return true;
    });
    EnsureReadFromCache("1.sst", fileSize);

    // Act: each download wakes the worker, which retries the delete that failed earlier.
    EnsureReadFromCache("2.sst", fileSize);
    EnsureReadFromCache("3.sst", fileSize);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(fileSize, m_cache.CacheSize());
    EXPECT_FALSE(m_cache.HasFile("1.sst"));
    EXPECT_EQ(3, deleteCalls.load());
}

TEST_F(FileCacheTests, RunningTotals_StayConsistentThroughEvictionAndRefresh) {
    // Arrange
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(10));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });

    // Act (CacheSize cross-checks the totals against a full scan in debug builds)
    for (int i = 0; i < 5; ++i) {
        EnsureReadFromCache(std::to_string(i) + ".sst", static_cast<size_t>(10 * (i + 1)));
    }
    m_cache.MarkFileAsStaleIfExists("2.sst");
    EXPECT_EQ(50U, m_cache.CacheSize());
    m_cache.SetCacheSize(20);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_LE(m_cache.CacheSize(), 20U);
}
TEST_F(FileCacheTests, EvictedFiles_AreDeletedBeforeTheNextFileIsDownloaded) {
    // Arrange
    const auto fileSize = static_cast<size_t>(100);
    m_cache.SetCacheSize(fileSize);
    std::atomic<bool> deletedBeforeDownload = false;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("2.sst", _, _, _)).WillRepeatedly([&](auto&&...) {
        deletedBeforeDownload = m_removedFiles.size() == 1;
    });
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    EnsureReadFromCache("1.sst", fileSize);

    // Act
    EnsureReadFromCache("2.sst", fileSize);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_TRUE(deletedBeforeDownload);
}

TEST_F(FileCacheTests, Eviction_DoesNotRemoveMetadataOnlyStaleEntries) {
    // Arrange
    const auto fileSize = static_cast<size_t>(100);
    m_cache.SetCacheSize(fileSize);
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    EXPECT_FALSE(m_cache.ReadFile("seen.sst", 0, 0, nullptr)); // metadata-only entry, size 0
    EnsureReadFromCache("1.sst", fileSize);

    // Act - forces an eviction
    EnsureReadFromCache("2.sst", fileSize);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_TRUE(m_cache.HasFile("seen.sst"));
    EXPECT_FALSE(m_cache.HasFile("1.sst"));
}

TEST_F(FileCacheTests, SetCacheSize_WhileADownloadIsInFlight_EvictsAfterItActivates) {
    // Arrange - the second file's download blocks until released.
    const auto fileSize = static_cast<size_t>(100);
    std::promise<void> started;
    std::promise<void> release;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("2.sst", _, _, _)).WillRepeatedly([&](auto&&...) {
        started.set_value();
        release.get_future().wait();
    });
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    EnsureReadFromCache("1.sst", fileSize);
    EXPECT_FALSE(m_cache.ReadFile("2.sst", 0, 0, nullptr));
    EXPECT_FALSE(m_cache.ReadFile("2.sst", 0, 0, nullptr));
    started.get_future().wait();

    // Act - the in-flight file is not counted yet, so nothing is evicted by the resize itself.
    m_cache.SetCacheSize(fileSize);
    release.set_value();
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_LE(m_cache.CacheSize(), static_cast<int64_t>(fileSize));
}
