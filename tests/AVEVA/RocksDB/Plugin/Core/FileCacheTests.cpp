// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/ContainerClientMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FileMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FilesystemMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/TempDirectory.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <future>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <unordered_set>
using AVEVA::RocksDB::Plugin::Core::ByteCount;
using AVEVA::RocksDB::Plugin::Core::ByteOffset;
using AVEVA::RocksDB::Plugin::Core::CachePresence;
using AVEVA::RocksDB::Plugin::Core::FileCache;
using AVEVA::RocksDB::Plugin::Core::FilesystemResult;
using AVEVA::RocksDB::Plugin::Core::IdleWaitResult;
using AVEVA::RocksDB::Plugin::Core::Mocks::ContainerClientMock;
using AVEVA::RocksDB::Plugin::Core::Mocks::FileMock;
using AVEVA::RocksDB::Plugin::Core::Mocks::FilesystemMock;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;
using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {
constexpr ByteOffset kStart{0};
constexpr ByteCount kNoBytes{0};
constexpr ByteCount kOneByte{1};
constexpr std::chrono::seconds kIdleTimeout{5};
} // namespace

// A gate a mock blocks on. It opens on destruction so a failed assertion cannot leave a worker blocked forever.
class Gate {
  public:
    Gate() : m_future(m_promise.get_future().share()) {}
    Gate(const Gate&) = delete;
    Gate& operator=(const Gate&) = delete;
    ~Gate() { Open(); }

    void Wait() const { m_future.wait(); }

    void Open() {
        if (!m_opened.exchange(true)) {
            m_promise.set_value();
        }
    }

  private:
    std::promise<void> m_promise;
    std::shared_future<void> m_future;
    std::atomic<bool> m_opened{false};
};

class FileCacheTests : public ::testing::Test {
  protected:
    static const constexpr std::string_view m_folderName = "tmp";
    std::shared_ptr<NiceMock<FilesystemMock>> m_filesystem;
    std::shared_ptr<ContainerClientMock> m_containerClient;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger;
    std::vector<std::filesystem::path> m_removedFiles;
    FileCache m_cache;

  public:
    FileCacheTests()
        : m_filesystem(std::make_shared<NiceMock<FilesystemMock>>()),
          m_containerClient(std::make_shared<ContainerClientMock>()),
          m_logger(std::make_shared<severity_logger_mt<severity_level>>()),
          m_cache(m_folderName, ByteCount{1073741824}, m_containerClient, m_filesystem, m_logger) {
        ON_CALL(*m_filesystem, DeleteFile(_)).WillByDefault([this](const std::filesystem::path& path) {
            m_removedFiles.push_back(path);
            return FilesystemResult::Succeeded;
        });
    }

    void EnsureReadFromCache(const std::string_view filePath) {
        // The first access only records interest and the second queues the download, so a few reads suffice.
        constexpr int maxAttempts = 4;
        for (int attempt = 0; attempt < maxAttempts; ++attempt) {
            const auto bytesRead = m_cache.ReadFile(filePath, kStart, kNoBytes, nullptr);
            if (bytesRead) {
                return;
            }
            m_cache.WaitUntilIdle();
        }
        FAIL() << "'" << filePath << "' did not become readable from cache";
    }

    void EnsureReadFromCache(const std::string_view filePath, const ByteCount expectedCacheSize) {
        char buffer[1];
        // The first access only records interest and the second queues the download, so a few reads suffice.
        constexpr int maxAttempts = 4;
        for (int attempt = 0; attempt < maxAttempts; ++attempt) {
            const auto bytesRead = m_cache.ReadFile(filePath, kStart, kOneByte, buffer);
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
    const ByteCount fileSize{268435456};
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
    EnsureReadFromCache("2.sst", fileSize + fileSize);
    EnsureReadFromCache("3.sst", fileSize + fileSize + fileSize);
    EnsureReadFromCache("4.sst", fileSize + fileSize + fileSize + fileSize);
    EnsureReadFromCache("5.sst", fileSize + fileSize + fileSize + fileSize);

    // Assert
    m_cache.WaitUntilIdle();
    std::unordered_set<std::string> expectedFiles = {"1.sst", "2.sst", "3.sst", "4.sst", "5.sst"};

    EXPECT_EQ(1, m_removedFiles.size());
    for (const auto& removed : m_removedFiles) {
        expectedFiles.erase(removed.filename().string());
    }

    for (const auto& expected : expectedFiles) {
        EXPECT_EQ(CachePresence::Present, m_cache.HasFile(expected));
    }
}

TEST_F(FileCacheTests, ReadRandomLargeFiles_EvictionWorks) {
    // Arrange
    const ByteCount fileSize1{268435456};
    const ByteCount fileSize2{76612355};
    const ByteCount fileSize3{16612355};
    const ByteCount fileSize4{176612355};
    const ByteCount fileSize5{330812579};
    const ByteCount fileSize6{509715200};

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
        EXPECT_EQ(CachePresence::Present, m_cache.HasFile(expected));
    }
}

TEST_F(FileCacheTests, ReadFileFromCache) {
    // Arrange
    std::string fileData = "Hello, World!";
    std::atomic<int> downloadCount{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(ByteCount{fileData.size()}));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _))
        .WillRepeatedly([&downloadCount](const std::string&, const std::filesystem::path&, ByteOffset, ByteCount) {
            downloadCount++;
        });
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
    const auto bytesRead = m_cache.ReadFile("1.sst", kStart, ByteCount{fileData.size()}, buffer.data());

    // Assert
    ASSERT_TRUE(bytesRead);
    ASSERT_EQ(*bytesRead, ByteCount{fileData.size()});
    ASSERT_EQ(std::string_view(buffer.begin(), buffer.end()), fileData);
}

TEST_F(FileCacheTests, EmptyBlobIsDownloadedWithZeroLength) {
    // Arrange
    std::atomic<uint64_t> lastLength{1};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(kNoBytes));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _))
        .WillRepeatedly([&lastLength](const std::string&, const std::filesystem::path&, ByteOffset, ByteCount length) {
            lastLength = length.Value();
        });
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        return std::make_unique<FileMock>();
    });

    // Act
    EnsureReadFromCache("1.sst");

    // Assert
    EXPECT_EQ(0U, lastLength);
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
}

TEST_F(FileCacheTests, CacheSizeExceeded) {
    // Arrange
    const ByteCount fileSize{20000};

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

TEST_F(FileCacheTests, FreshDownload_IsNotTheFirstEntryEvicted) {
    // Arrange
    const ByteCount fileSize{20000};
    const ByteCount threeFiles = fileSize + fileSize + fileSize;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t offset, uint64_t length) {
            return length - offset;
        });
        return file;
    });
    char buffer[1];
    // 4.sst is first seen before the others are used, so it holds the oldest LRU position when it is queued.
    EXPECT_FALSE(m_cache.ReadFile("4.sst", kStart, kOneByte, buffer));
    EnsureReadFromCache("1.sst");
    EnsureReadFromCache("2.sst");
    EnsureReadFromCache("3.sst");
    EXPECT_FALSE(m_cache.ReadFile("4.sst", kStart, kOneByte, buffer));
    m_cache.WaitUntilIdle();
    ASSERT_EQ(threeFiles + fileSize, m_cache.CacheSize());

    // Act - make room for exactly one eviction.
    m_cache.SetCacheSize(threeFiles);

    // Assert - the oldest previously-active file goes, not the download that just finished.
    m_cache.WaitUntilIdle();
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("1.sst"));
    EXPECT_EQ(CachePresence::Present, m_cache.HasFile("4.sst"));
    EXPECT_EQ(threeFiles, m_cache.CacheSize());
}

TEST_F(FileCacheTests, SetCacheSize_EvictsTheOnlyEntry) {
    // Arrange
    const ByteCount fileSize{20000};
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
    m_cache.SetCacheSize(kNoBytes);

    // Assert
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("only.sst"));
}

TEST_F(FileCacheTests, FailedSizeLookupLeavesTheEntryDownloadableAgain) {
    // Arrange
    std::atomic<int> sizeCalls = 0;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly([&sizeCalls](const std::string&) -> ByteCount {
        if (sizeCalls++ == 0) {
            throw std::runtime_error("boom");
        }
        return ByteCount{10};
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
    const auto bytesRead = m_cache.ReadFile("1.sst", kStart, kOneByte, buffer);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_FALSE(bytesRead);
    EXPECT_EQ(CachePresence::Present, m_cache.HasFile("1.sst"));
}

TEST_F(FileCacheTests, ReadFile_SecondAccess_QueuesAndDownloads) {
    // Arrange
    std::string fileData = "X";
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).Times(0);

    // First access should only record stale metadata.
    char buffer[1];
    const auto bytesReadOnFirstAccess = m_cache.ReadFile("1.sst", kStart, kOneByte, buffer);
    m_cache.WaitUntilIdle();
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_containerClient.get()));

    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst"))
        .Times(::testing::AtLeast(1))
        .WillRepeatedly(Return(ByteCount{fileData.size()}));
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
    const auto bytesReadOnSecondAccess = m_cache.ReadFile("1.sst", kStart, kOneByte, buffer);
    EnsureReadFromCache("1.sst");
    const auto bytesReadOnceCached = m_cache.ReadFile("1.sst", kStart, kOneByte, buffer);

    // Assert
    EXPECT_EQ(kOneByte, bytesReadOnceCached);
    EXPECT_FALSE(bytesReadOnFirstAccess);
    EXPECT_FALSE(bytesReadOnSecondAccess);
    EXPECT_EQ(CachePresence::Present, m_cache.HasFile("1.sst"));
}

TEST_F(FileCacheTests, ReadFile_ShortReadFromTheCachedFile_FallsBackAndDropsTheEntry) {
    // Arrange - the cached file is shorter than the blob size recorded for it.
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(ByteCount{8}));
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
    const auto bytesRead = m_cache.ReadFile("1.sst", kStart, ByteCount{8}, buffer);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_FALSE(bytesRead.has_value());
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("1.sst"));
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
}

TEST_F(FileCacheTests, ReadFile_CachedFileThatCannotBeOpened_DropsTheEntryAndIsDownloadedAgain) {
    // Arrange - the entry becomes Active, but every attempt to open its file throws.
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(ByteCount{8}));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(std::filesystem::path(m_folderName) / "1.sst"))
        .WillRepeatedly(
            [](const std::filesystem::path&) -> std::unique_ptr<FileMock> { throw std::runtime_error("cannot open"); });
    EnsureReadFromCache("1.sst");
    char buffer[8];

    // Act
    const auto bytesRead = m_cache.ReadFile("1.sst", kStart, ByteCount{8}, buffer);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_FALSE(bytesRead.has_value());
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("1.sst"));
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
}

TEST_F(FileCacheTests, FileReplacedWhileItsSizeIsBeingLookedUp_IsNotActivated) {
    // Arrange - the size lookup blocks until released.
    std::promise<void> started;
    Gate release;
    std::atomic<int> sizeCalls{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly([&](const std::string&) -> ByteCount {
        if (++sizeCalls == 1) {
            started.set_value();
            release.Wait();
        }
        return kOneByte;
    });
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).Times(0);
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr)); // records the entry
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr)); // queues the download
    started.get_future().wait();

    // Act - the downloader has popped the entry, which is still QueuedForDownload, when the blob is replaced.
    m_cache.MarkFileAsStaleIfExists("1.sst");
    release.Open();
    m_cache.WaitUntilIdle();

    // Assert - nothing was downloaded or activated with the pre-replacement size.
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_containerClient.get()));
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr).has_value());
    EXPECT_GE(sizeCalls, 1);
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    m_cache.WaitUntilIdle();
}

TEST_F(FileCacheTests, FileRemovedAndRecreatedWhileItsSizeIsBeingLookedUp_IsNotActivatedWithTheOldSize) {
    // Arrange - the first size lookup removes and re-creates the entry, which starts out Stale.
    std::atomic<int> sizeCalls{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly([&](const std::string&) -> ByteCount {
        if (++sizeCalls == 1) {
            m_cache.RemoveFile("1.sst");
            EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
        }
        return kOneByte;
    });
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).Times(0);
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr)); // records the entry
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr)); // queues the download

    // Act
    m_cache.WaitUntilIdle();

    // Assert - the new entry did not inherit the old size or become Active.
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_containerClient.get()));
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
    EXPECT_GE(sizeCalls, 1);
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    m_cache.WaitUntilIdle();
}

TEST_F(FileCacheTests, FileReplacedAndRequeuedDuringADownload_IsDownloadedAgainNotActivatedWithTheOldBytes) {
    // Arrange - the first download blocks until released; later ones return at once.
    std::promise<void> started;
    Gate release;
    std::atomic<int> downloads{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(kOneByte));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly([&](auto&&...) {
        if (++downloads == 1) {
            started.set_value();
            release.Wait();
        }
    });
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr)); // records the entry
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr)); // queues the download
    started.get_future().wait();

    // Act - the blob is replaced mid-download and a reader asks for it again before the first download ends.
    m_cache.MarkFileAsStaleIfExists("1.sst");
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    release.Open();
    m_cache.WaitUntilIdle();

    // Assert - the bytes fetched before the replacement were discarded and the file was fetched again.
    EXPECT_EQ(downloads, 2);
    EXPECT_TRUE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr).has_value());
}

TEST_F(FileCacheTests, RemoveFile_WhileAReaderIsInsideTheCachedFile_DoesNotWaitForItAndTheReaderFinishes) {
    // Arrange - a cached file whose read blocks until released.
    std::promise<void> readStarted;
    Gate release;
    std::atomic<bool> signalled{false};
    constexpr ByteCount fileSize{4};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([&](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        ON_CALL(*file, Read(_, _, _)).WillByDefault([&](char*, uint64_t, uint64_t length) -> uint64_t {
            if (!signalled.exchange(true)) {
                readStarted.set_value();
            }
            release.Wait();
            return length;
        });
        return file;
    });
    EnsureReadFromCache("1.sst");
    auto reader = std::async(std::launch::async, [this, fileSize] {
        char buffer[4];
        return m_cache.ReadFile("1.sst", kStart, fileSize, buffer);
    });
    readStarted.get_future().wait();

    // Act - removal must not block on the in-flight read.
    auto removal = std::async(std::launch::async, [this] { m_cache.RemoveFile("1.sst"); });
    const auto removed = removal.wait_for(std::chrono::seconds(5));
    release.Open();

    // Assert
    ASSERT_EQ(removed, std::future_status::ready);
    ASSERT_EQ(reader.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    reader.get();
    EXPECT_EQ(IdleWaitResult::Idle, m_cache.WaitUntilIdle(kIdleTimeout));
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("1.sst"));
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
}

extern thread_local size_t g_failAllocationIn;

TEST_F(FileCacheTests, RemoveFile_WhenAnAllocationFails_NeverThrowsAndNeverServesTheOldFile) {
    // Arrange - long names, so the path copy is a heap allocation, and a cached file with an open handle.
    constexpr ByteCount fileSize{4};
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        ON_CALL(*file, Read(_, _, _)).WillByDefault([](char*, uint64_t, uint64_t length) -> uint64_t {
            return length;
        });
        return file;
    });

    for (size_t failAt = 1; failAt <= 4; ++failAt) {
        const std::string name = std::string(70, 'a') + std::to_string(failAt) + ".sst";
        char buffer[4];
        EnsureReadFromCache(name, fileSize);
        ASSERT_TRUE(m_cache.ReadFile(name, kStart, fileSize, buffer).has_value());

        // Act
        g_failAllocationIn = failAt;
        EXPECT_NO_THROW(m_cache.RemoveFile(name)) << "failAt=" << failAt;
        g_failAllocationIn = 0;

        // Assert - either removed, or invalidated so that the old bytes are no longer served.
        EXPECT_FALSE(m_cache.ReadFile(name, kStart, fileSize, buffer).has_value() &&
                     m_cache.HasFile(name) == CachePresence::Present)
            << "failAt=" << failAt;
        m_cache.RemoveFile(name);
        EXPECT_EQ(IdleWaitResult::Idle, m_cache.WaitUntilIdle(kIdleTimeout));
        EXPECT_EQ(CachePresence::Absent, m_cache.HasFile(name)) << "failAt=" << failAt;
        EXPECT_EQ(kNoBytes, m_cache.CacheSize()) << "failAt=" << failAt;

        // The file can be cached again afterwards.
        EnsureReadFromCache(name, fileSize);
        m_cache.RemoveFile(name);
        EXPECT_EQ(IdleWaitResult::Idle, m_cache.WaitUntilIdle(kIdleTimeout));
        EXPECT_EQ(kNoBytes, m_cache.CacheSize()) << "failAt=" << failAt;
    }
}

TEST_F(FileCacheTests, MarkFileAsStale_WhenAnAllocationFails_StillClosesTheHandleAndDoesNotServeTheOldFile) {
    constexpr ByteCount fileSize{4};
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        ON_CALL(*file, Read(_, _, _)).WillByDefault([](char*, uint64_t, uint64_t length) -> uint64_t {
            return length;
        });
        return file;
    });

    // Failures in the debug logging that precedes the handoff propagate as before; the ones that hit the handoff
    // itself must be absorbed.
    size_t absorbed = 0;
    for (size_t failAt = 1; failAt <= 4; ++failAt) {
        const std::string name = std::string(70, 'b') + std::to_string(failAt) + ".sst";
        char buffer[4];
        EnsureReadFromCache(name, fileSize);
        ASSERT_TRUE(m_cache.ReadFile(name, kStart, fileSize, buffer).has_value());

        g_failAllocationIn = failAt;
        bool threw = false;
        try {
            m_cache.MarkFileAsStaleIfExists(name);
        } catch (const std::bad_alloc&) {
            threw = true;
        }
        const bool consumed = g_failAllocationIn == 0;
        g_failAllocationIn = 0;

        if (!threw) {
            absorbed += consumed ? 1 : 0;
            EXPECT_FALSE(m_cache.ReadFile(name, kStart, fileSize, buffer).has_value()) << "failAt=" << failAt;
        }
        m_cache.RemoveFile(name);
        EXPECT_EQ(IdleWaitResult::Idle, m_cache.WaitUntilIdle(kIdleTimeout));
        EXPECT_EQ(kNoBytes, m_cache.CacheSize()) << "failAt=" << failAt;
    }
    EXPECT_GT(absorbed, 0U);
}

TEST_F(FileCacheTests, DownloadThatFailsAfterTheFileWasMarkedStale_LeavesNothingActiveAndCanBeRetried) {
    // Arrange - the first download blocks, then fails; later ones succeed.
    std::promise<void> started;
    Gate release;
    std::atomic<int> downloads{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(kOneByte));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly([&](auto&&...) {
        if (++downloads == 1) {
            started.set_value();
            release.Wait();
            throw std::runtime_error("download failed");
        }
    });
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    started.get_future().wait();

    // Act
    m_cache.MarkFileAsStaleIfExists("1.sst");
    release.Open();
    ASSERT_EQ(IdleWaitResult::Idle, m_cache.WaitUntilIdle(kIdleTimeout));

    // Assert - the failed download activated nothing and counted no bytes.
    EXPECT_EQ(kNoBytes, m_cache.CacheSize());
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr).has_value());

    // A later read queues the download again and this time it succeeds.
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        ON_CALL(*file, Read(_, _, _)).WillByDefault([](char*, uint64_t, uint64_t length) -> uint64_t {
            return length;
        });
        return file;
    });
    EnsureReadFromCache("1.sst");
    EXPECT_GE(downloads.load(), 2);
}

TEST_F(FileCacheTests, DownloadThatFailsAfterTheFileWasMarkedStaleAndRequeued_KeepsTheQueuedEntryAndRetries) {
    // Arrange - the first download blocks, then fails; the retry succeeds.
    std::promise<void> started;
    Gate release;
    std::atomic<int> downloads{0};
    EXPECT_CALL(*m_containerClient, GetBlobSize("1.sst")).WillRepeatedly(Return(kOneByte));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly([&](auto&&...) {
        if (++downloads == 1) {
            started.set_value();
            release.Wait();
            throw std::runtime_error("download failed");
        }
    });
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        ON_CALL(*file, Read(_, _, _)).WillByDefault([](char*, uint64_t, uint64_t length) -> uint64_t {
            return length;
        });
        return file;
    });
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    started.get_future().wait();

    // Act - replaced and queued again while the first download is still running, then that download fails.
    m_cache.MarkFileAsStaleIfExists("1.sst");
    EXPECT_FALSE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    release.Open();
    ASSERT_EQ(IdleWaitResult::Idle, m_cache.WaitUntilIdle(kIdleTimeout));

    // Assert - the failure did not discard the newer queued state, so the queued download ran and activated it.
    EXPECT_EQ(downloads, 2);
    EXPECT_EQ(CachePresence::Present, m_cache.HasFile("1.sst"));
    EXPECT_TRUE(m_cache.ReadFile("1.sst", kStart, kNoBytes, nullptr).has_value());
}

TEST_F(FileCacheTests, ReadFile_TooManyUndownloadedStaleEntries_PrunesOldest) {
    // Arrange
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).Times(0);
    constexpr int staleEntriesToInsert = 2050;

    // Act
    char buffer[1];
    for (int i = 0; i < staleEntriesToInsert; ++i) {
        const auto filePath = std::to_string(i) + ".sst";
        const auto bytesRead = m_cache.ReadFile(filePath, kStart, kOneByte, buffer);
        ASSERT_FALSE(bytesRead);
    }
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("0.sst"));
    EXPECT_EQ(CachePresence::Present, m_cache.HasFile(std::to_string(staleEntriesToInsert - 1) + ".sst"));
}

TEST_F(FileCacheTests, ReadFile_ManyReadsOfACachedFile_OpenItOnce) {
    // Arrange
    std::atomic<int> opens = 0;
    constexpr ByteCount fileSize{8};
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([&opens](const std::filesystem::path&) {
        ++opens;
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    EnsureReadFromCache("1.sst", fileSize);

    // Act
    char buffer[1];
    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(kOneByte, m_cache.ReadFile("1.sst", kStart, kOneByte, buffer));
    }

    // Assert
    EXPECT_EQ(1, opens.load());
}

TEST_F(FileCacheTests, EvictedFile_WhoseDeleteFails_IsLoggedRetriedAndNotCounted) {
    // Arrange
    const ByteCount fileSize{100};
    m_cache.SetCacheSize(fileSize);
    std::atomic<int> deleteCalls = 0;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    ON_CALL(*m_filesystem, DeleteFile(_))
        .WillByDefault([&deleteCalls](const std::filesystem::path&) -> FilesystemResult {
            if (deleteCalls++ == 0) {
                throw std::runtime_error("locked");
            }
            return FilesystemResult::Succeeded;
        });
    EnsureReadFromCache("1.sst", fileSize);

    // Act: each download wakes the worker, which retries the delete that failed earlier.
    EnsureReadFromCache("2.sst", fileSize);
    EnsureReadFromCache("3.sst", fileSize);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(fileSize, m_cache.CacheSize());
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("1.sst"));
    EXPECT_EQ(3, deleteCalls.load());
}

TEST_F(FileCacheTests, RunningTotals_StayConsistentThroughEvictionAndRefresh) {
    // Arrange
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(ByteCount{10}));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });

    // Act (CacheSize cross-checks the totals against a full scan in debug builds)
    for (uint64_t i = 0; i < 5; ++i) {
        EnsureReadFromCache(std::to_string(i) + ".sst", ByteCount{10 * (i + 1)});
    }
    m_cache.MarkFileAsStaleIfExists("2.sst");
    EXPECT_EQ(ByteCount{50}, m_cache.CacheSize());
    m_cache.SetCacheSize(ByteCount{20});
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_LE(m_cache.CacheSize(), ByteCount{20});
}

TEST_F(FileCacheTests, SetCacheSize_ShrinkingBelowTheCurrentSizeEvictsAgainstTheNewMaximum) {
    // Arrange
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(ByteCount{20}));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    m_cache.SetCacheSize(ByteCount{100});
    for (uint64_t i = 0; i < 3; ++i) {
        EnsureReadFromCache(std::to_string(i) + ".sst", ByteCount{20 * (i + 1)});
    }
    ASSERT_EQ(ByteCount{60}, m_cache.CacheSize());

    // Act: evicting 50 bytes is more than the new maximum of 10, which must not be mistaken for an oversized file.
    m_cache.SetCacheSize(ByteCount{10});
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_LE(m_cache.CacheSize(), ByteCount{10});
}

TEST_F(FileCacheTests, ShortRead_DoesNotRemoveAnEntryThatWasReplacedDuringTheRead) {
    // Arrange
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(ByteCount{10}));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([this](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([this](char*, uint64_t, uint64_t) {
            // The blob changes while this read is in flight, so the entry being read is no longer the current one.
            m_cache.MarkFileAsStaleIfExists("1.sst");
            return 0;
        });
        return file;
    });
    std::array<char, 10> buffer{};
    (void)m_cache.ReadFile("1.sst", kStart, ByteCount{10}, buffer.data());
    m_cache.WaitUntilIdle();

    // Act
    (void)m_cache.ReadFile("1.sst", kStart, ByteCount{10}, buffer.data());
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(CachePresence::Present, m_cache.HasFile("1.sst"));
}

TEST_F(FileCacheTests, EvictedFiles_AreDeletedBeforeTheNextFileIsDownloaded) {
    // Arrange
    const ByteCount fileSize{100};
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
    const ByteCount fileSize{100};
    m_cache.SetCacheSize(fileSize);
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    EXPECT_FALSE(m_cache.ReadFile("seen.sst", kStart, kNoBytes, nullptr)); // metadata-only entry, size 0
    EnsureReadFromCache("1.sst", fileSize);

    // Act - forces an eviction
    EnsureReadFromCache("2.sst", fileSize);
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(CachePresence::Present, m_cache.HasFile("seen.sst"));
    EXPECT_EQ(CachePresence::Absent, m_cache.HasFile("1.sst"));
}

TEST_F(FileCacheTests, SetCacheSize_WhileADownloadIsInFlight_EvictsAfterItActivates) {
    // Arrange - the second file's download blocks until released.
    const ByteCount fileSize{100};
    std::promise<void> started;
    Gate release;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(fileSize));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("2.sst", _, _, _)).WillRepeatedly([&](auto&&...) {
        started.set_value();
        release.Wait();
    });
    EXPECT_CALL(*m_filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    EnsureReadFromCache("1.sst", fileSize);
    EXPECT_FALSE(m_cache.ReadFile("2.sst", kStart, kNoBytes, nullptr));
    EXPECT_FALSE(m_cache.ReadFile("2.sst", kStart, kNoBytes, nullptr));
    started.get_future().wait();

    // Act - the in-flight file is not counted yet, so nothing is evicted by the resize itself.
    m_cache.SetCacheSize(fileSize);
    release.Open();
    m_cache.WaitUntilIdle();

    // Assert
    EXPECT_LE(m_cache.CacheSize(), fileSize);
}

TEST_F(FileCacheTests, Download_PassesTheCachePathThroughWithoutNarrowing) {
    // Arrange - a directory name that is not representable in the ANSI code page.
    const std::filesystem::path cacheDir = std::filesystem::path(u8"tmp-\u65E5\u672C\u8A9E");
    FileCache cache(cacheDir, ByteCount{1000}, m_containerClient, m_filesystem, m_logger);
    std::promise<std::filesystem::path> destination;
    EXPECT_CALL(*m_containerClient, GetBlobSize(_)).WillRepeatedly(Return(kOneByte));
    EXPECT_CALL(*m_containerClient, DownloadBlobTo("1.sst", _, _, _))
        .WillOnce([&](const std::string&, const std::filesystem::path& path, ByteOffset, ByteCount) {
            destination.set_value(path);
        });

    // Act
    EXPECT_FALSE(cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    EXPECT_FALSE(cache.ReadFile("1.sst", kStart, kNoBytes, nullptr));
    const auto path = destination.get_future().get();
    cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(cacheDir / "1.sst", path);
}

TEST(FileCacheShutdownTests, DestroyingTheCacheWithQueuedDownloads_StopsWithoutHangingOrCrashing) {
    // Arrange - the first download blocks while two more are queued behind it.
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    auto containerClient = std::make_shared<ContainerClientMock>();
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();
    std::promise<void> started;
    Gate release;
    std::atomic<int> downloads{0};
    EXPECT_CALL(*containerClient, GetBlobSize(_)).WillRepeatedly(Return(kOneByte));
    EXPECT_CALL(*containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly([&](auto&&...) {
        if (++downloads == 1) {
            started.set_value();
            release.Wait();
        }
    });
    auto cache = std::make_unique<FileCache>("tmp", ByteCount{1000}, containerClient, filesystem, logger);
    for (const auto* name : {"1.sst", "2.sst", "3.sst"}) {
        (void)cache->ReadFile(name, kStart, kNoBytes, nullptr);
        (void)cache->ReadFile(name, kStart, kNoBytes, nullptr);
    }
    started.get_future().wait();

    // Act - destruction must wait for the running download, then drop the queued ones.
    auto destruction = std::async(std::launch::async, [&cache] { cache.reset(); });
    EXPECT_EQ(destruction.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    release.Open();

    // Assert
    ASSERT_EQ(destruction.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    destruction.get();
}

TEST(FileCacheShutdownTests, DestroyingTheCacheCancelsARunningDownloadThatHonoursTheStopToken) {
    // Arrange - a client whose download only ends when it is asked to stop.
    class BlockingClient final : public AVEVA::RocksDB::Plugin::Core::ContainerClient {
      public:
        std::promise<void> Started;
        ByteCount GetBlobSize(const std::string&, std::stop_token) override { return ByteCount{1}; }
        void DownloadBlobTo(const std::string&, const std::filesystem::path&, ByteOffset, ByteCount,
                            std::stop_token stopToken) override {
            Started.set_value();
            while (!stopToken.stop_requested()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            throw std::runtime_error("download stopped");
        }
    };
    auto client = std::make_shared<BlockingClient>();
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();
    auto cache = std::make_unique<FileCache>("tmp", ByteCount{1000}, client, filesystem, logger);
    (void)cache->ReadFile("1.sst", kStart, kNoBytes, nullptr);
    (void)cache->ReadFile("1.sst", kStart, kNoBytes, nullptr);
    client->Started.get_future().wait();

    // Act
    auto destruction = std::async(std::launch::async, [&cache] { cache.reset(); });

    // Assert
    ASSERT_EQ(destruction.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    destruction.get();
}

TEST(FileCacheShutdownTests, DestroyingTheCacheCancelsASizeLookupThatHonoursTheStopToken) {
    // Arrange - a client whose size lookup only ends when it is asked to stop.
    class BlockingClient final : public AVEVA::RocksDB::Plugin::Core::ContainerClient {
      public:
        std::promise<void> Started;
        ByteCount GetBlobSize(const std::string&, std::stop_token stopToken) override {
            Started.set_value();
            while (!stopToken.stop_requested()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            throw std::runtime_error("size lookup stopped");
        }
        void DownloadBlobTo(const std::string&, const std::filesystem::path&, ByteOffset, ByteCount,
                            std::stop_token) override {}
    };
    auto client = std::make_shared<BlockingClient>();
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();
    auto cache = std::make_unique<FileCache>("tmp", ByteCount{1000}, client, filesystem, logger);
    (void)cache->ReadFile("1.sst", kStart, kNoBytes, nullptr);
    (void)cache->ReadFile("1.sst", kStart, kNoBytes, nullptr);
    client->Started.get_future().wait();

    // Act
    auto destruction = std::async(std::launch::async, [&cache] { cache.reset(); });

    // Assert
    ASSERT_EQ(destruction.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    destruction.get();
}
TEST(FileCacheShutdownTests, PartialDownloadsFromAnEarlierRunAreDeletedAtStartup) {
    // Arrange
    const AVEVA::RocksDB::Plugin::Core::Mocks::TempDirectory directory("filecache_partial");
    std::filesystem::create_directories(directory.Path() / "db");
    const auto partial = directory.Path() / "db" / "1.sst.partial-0f8fad5b-d9cb-469f-a165-70867728950e";
    const auto lookAlike = directory.Path() / "db" / "report.partial-backup.txt";
    const auto finished = directory.Path() / "db" / "2.sst";
    std::ofstream{partial} << "truncated";
    std::ofstream{lookAlike} << "keep";
    std::ofstream{finished} << "complete";
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    auto containerClient = std::make_shared<ContainerClientMock>();
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();

    // Act
    const FileCache cache(directory.Path(), ByteCount{1000}, containerClient, filesystem, logger);

    // Assert
    EXPECT_FALSE(std::filesystem::exists(partial));
    EXPECT_TRUE(std::filesystem::exists(lookAlike));
    EXPECT_TRUE(std::filesystem::exists(finished));
}

namespace {
// Records every delete the cache asks the filesystem for.
struct PruneDeleteCounter {
    std::mutex Mutex;
    std::vector<std::filesystem::path> Deleted;

    void Attach(FilesystemMock& filesystem) {
        ON_CALL(filesystem, DeleteFile(_)).WillByDefault([this](const std::filesystem::path& path) {
            std::scoped_lock lock(Mutex);
            Deleted.push_back(path);
            return FilesystemResult::Succeeded;
        });
    }
};

void TouchOnce(FileCache& cache, const int count, const std::string& prefix) {
    char buffer[1];
    for (int i = 0; i < count; ++i) {
        ASSERT_FALSE(cache.ReadFile(prefix + std::to_string(i) + ".sst", kStart, kOneByte, buffer));
    }
}
} // namespace

TEST(FileCacheShutdownTests, PruningOneTouchEntries_InAnEmptyCacheDirectory_DeletesNothing) {
    // Arrange
    const AVEVA::RocksDB::Plugin::Core::Mocks::TempDirectory directory("filecache_prune_empty");
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    PruneDeleteCounter counter;
    counter.Attach(*filesystem);
    auto containerClient = std::make_shared<ContainerClientMock>();
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();
    FileCache cache(directory.Path(), ByteCount{1000}, containerClient, filesystem, logger);

    // Act
    TouchOnce(cache, 3000, "");
    cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(CachePresence::Absent, cache.HasFile("0.sst"));
    EXPECT_EQ(CachePresence::Present, cache.HasFile("2999.sst"));
    EXPECT_TRUE(counter.Deleted.empty());
}

TEST(FileCacheShutdownTests, PruningOneTouchEntries_WhenAnEarlierRunLeftFiles_StillDeletesThem) {
    // Arrange
    const AVEVA::RocksDB::Plugin::Core::Mocks::TempDirectory directory("filecache_prune_prior");
    std::ofstream{directory.Path() / "0.sst"} << "from an earlier run";
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    PruneDeleteCounter counter;
    counter.Attach(*filesystem);
    auto containerClient = std::make_shared<ContainerClientMock>();
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();
    FileCache cache(directory.Path(), ByteCount{1000}, containerClient, filesystem, logger);

    // Act
    TouchOnce(cache, 2050, "");
    cache.WaitUntilIdle();

    // Assert
    ASSERT_FALSE(counter.Deleted.empty());
    EXPECT_EQ("0.sst", counter.Deleted.front().filename().string());
}

TEST(FileCacheShutdownTests, PruningAStaleZeroLengthDownloadedEntry_StillDeletesItsFile) {
    // Arrange
    const AVEVA::RocksDB::Plugin::Core::Mocks::TempDirectory directory("filecache_prune_zero");
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    PruneDeleteCounter counter;
    counter.Attach(*filesystem);
    auto containerClient = std::make_shared<ContainerClientMock>();
    EXPECT_CALL(*containerClient, GetBlobSize(_)).WillRepeatedly(Return(kNoBytes));
    EXPECT_CALL(*containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();
    FileCache cache(directory.Path(), ByteCount{1000}, containerClient, filesystem, logger);
    for (int attempt = 0; attempt < 4 && !cache.ReadFile("zero.sst", kStart, kNoBytes, nullptr); ++attempt) {
        cache.WaitUntilIdle();
    }
    ASSERT_TRUE(cache.ReadFile("zero.sst", kStart, kNoBytes, nullptr));
    cache.MarkFileAsStaleIfExists("zero.sst");

    // Act
    TouchOnce(cache, 2049, "other");
    cache.WaitUntilIdle();

    // Assert
    EXPECT_EQ(CachePresence::Absent, cache.HasFile("zero.sst"));
    ASSERT_EQ(1U, counter.Deleted.size());
    EXPECT_EQ("zero.sst", counter.Deleted.front().filename().string());
}

TEST(FileCacheShutdownTests, FailedDelete_IsRetriedOnceAtShutdown) {
    // Arrange
    auto filesystem = std::make_shared<NiceMock<FilesystemMock>>();
    auto containerClient = std::make_shared<ContainerClientMock>();
    auto logger = std::make_shared<severity_logger_mt<severity_level>>();
    std::atomic<int> deleteCalls = 0;
    EXPECT_CALL(*containerClient, GetBlobSize(_)).WillRepeatedly(Return(ByteCount{100}));
    EXPECT_CALL(*containerClient, DownloadBlobTo(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*filesystem, Open(_)).WillRepeatedly([](const std::filesystem::path&) {
        auto file = std::make_unique<FileMock>();
        EXPECT_CALL(*file, Read(_, _, _)).WillRepeatedly([](char*, uint64_t, uint64_t length) { return length; });
        return file;
    });
    ON_CALL(*filesystem, DeleteFile(_)).WillByDefault([&deleteCalls](const std::filesystem::path&) -> FilesystemResult {
        // The first attempt fails; only the shutdown retry succeeds.
        return deleteCalls++ > 0 ? FilesystemResult::Succeeded : FilesystemResult::Failed;
    });

    {
        FileCache cache("tmp", ByteCount{100}, containerClient, filesystem, logger);
        for (const auto* name : {"1.sst", "2.sst"}) {
            for (int attempt = 0; attempt < 4; ++attempt) {
                (void)cache.ReadFile(name, kStart, kNoBytes, nullptr);
                cache.WaitUntilIdle();
            }
        }

        // Act: destruction stops the downloader
    }

    // Assert
    EXPECT_EQ(2, deleteCalls.load());
}
