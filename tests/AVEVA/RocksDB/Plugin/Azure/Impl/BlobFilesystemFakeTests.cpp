// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "FakeBlobServer.hpp"
#include "FakeHttpClient.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <span>
#include <thread>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
namespace {
using FakeFilesystem = BasicBlobFilesystemImpl<FakeHttpClient>;

constexpr const char* AccountUrl = "https://account.blob.core.windows.net";
// Shared-key credentials must be base64; the fake service does not verify the signature.
constexpr const char* AccountKey = "dGVzdC1rZXk=";
constexpr const char* Container = "testdb";

// The whole stack except the network: the production filesystem over the library clients, talking through the fake
// HTTP transport to an in-memory page blob service.
class BlobFilesystemFakeTests : public ::testing::Test {
  protected:
    BlobFilesystemFakeTests() {
        m_runtime->HttpClient().CompleteInline() = true;
        m_runtime->HttpClient().StartBackgroundPump();
        m_server = std::make_unique<FakeBlobServer>(m_runtime->HttpClient(), Container, "unused");
        m_fs = std::make_unique<FakeFilesystem>(
            m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
            std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>());
        m_prefix = StorageAccount::UniquePrefix(AccountUrl, Container);
    }

    ~BlobFilesystemFakeTests() override {
        m_fs.reset();
        m_server.reset();
    }

    [[nodiscard]] std::string Path(const std::string& name) const { return m_prefix + "/" + name; }

    void Write(const std::string& name, const std::string& content) {
        auto file = m_fs->CreateWriteableFile(Path(name));
        file.Append(std::span<const char>(content.data(), content.size()));
        file.Close();
    }

    [[nodiscard]] std::string Read(const std::string& name) {
        auto file = m_fs->CreateSequentialFile(Path(name));
        std::string content(static_cast<size_t>(file.GetSize()), '\0');
        const auto read = file.SequentialRead(static_cast<int64_t>(content.size()), content.data());
        content.resize(static_cast<size_t>(read));
        return content;
    }

    std::shared_ptr<BasicClientRuntime<FakeHttpClient>> m_runtime =
        std::make_shared<BasicClientRuntime<FakeHttpClient>>(std::make_unique<FakeHttpClient>());
    std::unique_ptr<FakeBlobServer> m_server;
    std::unique_ptr<FakeFilesystem> m_fs;
    std::string m_prefix;
};

TEST_F(BlobFilesystemFakeTests, WrittenFileCanBeReadBackAndReportsItsLogicalSize) {
    Write("000001.sst", "hello page blob");

    EXPECT_TRUE(m_fs->FileExists(Path("000001.sst")));
    EXPECT_EQ(m_fs->GetFileSize(Path("000001.sst")), 15);
    EXPECT_EQ(Read("000001.sst"), "hello page blob");
}

TEST_F(BlobFilesystemFakeTests, MissingFileDoesNotExist) { EXPECT_FALSE(m_fs->FileExists(Path("missing.sst"))); }

TEST_F(BlobFilesystemFakeTests, ChildrenAreListedRelativeToTheirDirectory) {
    Write("dir/a.log", "a");
    Write("dir/b.log", "bb");
    Write("other/c.log", "c");

    auto children = m_fs->GetChildren(Path("dir"));
    std::ranges::sort(children);
    EXPECT_EQ(children, (std::vector<std::string>{"a.log", "b.log"}));

    auto attributes = m_fs->GetChildrenFileAttributes(Path("dir"));
    ASSERT_EQ(attributes.size(), 2U);
    std::ranges::sort(attributes, {}, [](const auto& attribute) { return attribute.GetName(); });
    EXPECT_EQ(attributes[0].GetSize(), 1U);
    EXPECT_EQ(attributes[1].GetSize(), 2U);
}

TEST_F(BlobFilesystemFakeTests, DirectoryWithChildrenExistsAndDeleteDirRemovesThem) {
    Write("dir/a.log", "a");
    Write("dir/b.log", "b");

    EXPECT_TRUE(m_fs->FileExists(Path("dir")));
    EXPECT_EQ(m_fs->DeleteDir(Path("dir")), 0U);
    EXPECT_FALSE(m_fs->FileExists(Path("dir/a.log")));
    EXPECT_FALSE(m_fs->FileExists(Path("dir")));
}

TEST_F(BlobFilesystemFakeTests, DeleteFileReportsWhetherAnythingWasDeleted) {
    Write("a.log", "a");

    EXPECT_TRUE(m_fs->DeleteFile(Path("a.log")));
    EXPECT_FALSE(m_fs->DeleteFile(Path("a.log")));
}

TEST_F(BlobFilesystemFakeTests, RenameMovesTheContentAndRemovesTheSource) {
    Write("old.sst", "payload");

    m_fs->RenameFile(Path("old.sst"), Path("new.sst"));

    EXPECT_FALSE(m_fs->FileExists(Path("old.sst")));
    EXPECT_EQ(Read("new.sst"), "payload");
    EXPECT_EQ(m_fs->GetFileSize(Path("new.sst")), 7);
}

TEST_F(BlobFilesystemFakeTests, RenameCopiesInsideTheServiceWithoutDownloadingTheData) {
    Write("old.sst", "payload");

    m_fs->RenameFile(Path("old.sst"), Path("new.sst"));

    EXPECT_FALSE(m_server->PageCopies.empty());
    EXPECT_TRUE(m_server->Downloads.empty());
    EXPECT_EQ(Read("new.sst"), "payload");
}

TEST_F(BlobFilesystemFakeTests, RenameFallsBackToCopyingThroughTheClientWhenTheSourceCannotBeAuthorized) {
    Write("old.sst", "payload");
    m_server->RequireCopySourceAuthorization = true;

    m_fs->RenameFile(Path("old.sst"), Path("new.sst"));

    EXPECT_FALSE(m_fs->FileExists(Path("old.sst")));
    EXPECT_EQ(Read("new.sst"), "payload");
    EXPECT_EQ(m_fs->GetFileSize(Path("new.sst")), 7);
}

TEST_F(BlobFilesystemFakeTests, TruncateShrinksTheLogicalSize) {
    Write("a.log", "0123456789");

    m_fs->Truncate(Path("a.log"), 4);

    EXPECT_EQ(m_fs->GetFileSize(Path("a.log")), 4);
}

TEST_F(BlobFilesystemFakeTests, TruncateToALargerSizeIsRejectedAndKeepsTheContent) {
    Write("a.log", "0123456789");

    EXPECT_THROW(m_fs->Truncate(Path("a.log"), 4096), std::invalid_argument);

    EXPECT_EQ(m_fs->GetFileSize(Path("a.log")), 10);
    EXPECT_EQ(Read("a.log"), "0123456789");
}
TEST_F(BlobFilesystemFakeTests, TruncateDropsTheCachedCopyOfTheFile) {
    const auto cacheDir = std::filesystem::temp_directory_path() / ("aveva_truncate_cache_" + m_prefix.substr(0, 8));
    std::filesystem::create_directories(cacheDir);
    struct Cleanup {
        std::filesystem::path Path;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(Path, ec);
        }
    } cleanup{cacheDir};
    const auto cacheDirString = cacheDir.string();
    FakeFilesystem cached(
        m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(),
        std::string_view(cacheDirString));
    {
        auto writer = cached.CreateWriteableFile(Path("t.sst"));
        const std::string content(1024, 'C');
        writer.Append(std::span<const char>(content.data(), content.size()));
        writer.Close();
    }
    auto file = cached.CreateReadableFile(Path("t.sst"));
    std::vector<char> buffer(1024);

    // The cache downloads a file in the background once it is accessed; wait for that download instead of polling.
    std::optional<size_t> hit;
    for (int attempt = 0; attempt < 3 && !hit; ++attempt) {
        hit = file.TryReadFromCache(0, static_cast<int64_t>(buffer.size()), buffer.data());
        if (!hit) {
            ASSERT_TRUE(cached.WaitForFileCachesIdleForTesting(std::chrono::seconds(30)))
                << "Background download did not finish";
        }
    }
    ASSERT_TRUE(hit.has_value()) << "File was not cached after the background download finished";

    cached.Truncate(Path("t.sst"), 512);

    EXPECT_FALSE(file.TryReadFromCache(0, 512, buffer.data()).has_value());
}
TEST_F(BlobFilesystemFakeTests, FailedTruncateResizeKeepsTheOriginalLogicalSize) {
    Write("a.log", "0123456789");
    m_server->Fault = [](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::Resize) {
            return FakeBlobServer::Error(400U, "InvalidInput");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW(m_fs->Truncate(Path("a.log"), 4));

    m_server->Fault = nullptr;
    EXPECT_EQ(m_fs->GetFileSize(Path("a.log")), 10);
    EXPECT_EQ(Read("a.log"), "0123456789");
}

// A filesystem with a disk cache, to check that failed mutations still drop cached copies.
class CachedFailureTests : public BlobFilesystemFakeTests {
  protected:
    CachedFailureTests()
        : m_cacheDir(std::filesystem::temp_directory_path() / ("aveva_failure_cache_" + m_prefix.substr(0, 8))) {
        std::filesystem::create_directories(m_cacheDir);
        const auto dir = m_cacheDir.string();
        m_cached = std::make_unique<FakeFilesystem>(
            m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
            std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(),
            std::string_view(dir));
    }

    ~CachedFailureTests() override {
        m_cached.reset();
        std::error_code ec;
        std::filesystem::remove_all(m_cacheDir, ec);
    }

    void WriteCached(const std::string& name, const std::string& content) {
        auto file = m_cached->CreateWriteableFile(Path(name));
        file.Append(std::span<const char>(content.data(), content.size()));
        file.Close();
    }

    // Reads until the background download has cached the file; returns whether it did.
    template <typename TFile> bool WarmCache(TFile& file, const size_t size) {
        std::vector<char> buffer(size);
        for (int attempt = 0; attempt < 3; ++attempt) {
            if (file.TryReadFromCache(0, static_cast<int64_t>(size), buffer.data())) {
                return true;
            }
            if (!m_cached->WaitForFileCachesIdleForTesting(std::chrono::seconds(30))) {
                return false;
            }
        }
        return false;
    }

    std::filesystem::path m_cacheDir;
    std::unique_ptr<FakeFilesystem> m_cached;
};

TEST_F(CachedFailureTests, TruncateThatFailsAfterShrinkingStillDropsTheCachedCopy) {
    WriteCached("t.sst", std::string(1024, 'C'));
    auto file = m_cached->CreateReadableFile(Path("t.sst"));
    ASSERT_TRUE(WarmCache(file, 1024));
    m_server->Fault = [](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::SetMetadata) {
            return FakeBlobServer::Error(400U, "InvalidInput");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW(m_cached->Truncate(Path("t.sst"), 512));

    std::vector<char> buffer(512);
    EXPECT_FALSE(file.TryReadFromCache(0, 512, buffer.data()).has_value());
}

TEST_F(CachedFailureTests, RenameThatFailsMidCommitStillDropsTheDestinationsCachedCopy) {
    WriteCached("old.sst", "source content");
    WriteCached("new.sst", std::string(1024, 'N'));
    auto file = m_cached->CreateReadableFile(Path("new.sst"));
    ASSERT_TRUE(WarmCache(file, 1024));
    int sizeWrites = 0;
    m_server->Fault = [&sizeWrites](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::SetMetadata && ++sizeWrites == 2) {
            return FakeBlobServer::Error(400U, "InvalidInput");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW(m_cached->RenameFile(Path("old.sst"), Path("new.sst")));

    std::vector<char> buffer(1024);
    EXPECT_FALSE(file.TryReadFromCache(0, 1024, buffer.data()).has_value());
}
// Page blobs are 512-byte aligned, so sizes straddling a page boundary exercise the padding/partial-page paths.
class PageBoundaryTests : public BlobFilesystemFakeTests, public ::testing::WithParamInterface<size_t> {
  protected:
    static std::string Pattern(const size_t size) {
        std::string data(size, '\0');
        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<char>('a' + (i * 7 + i / 512) % 26);
        }
        return data;
    }
};

TEST_P(PageBoundaryTests, WriteThenReadReturnsExactBytesAndSize) {
    const auto content = Pattern(GetParam());
    Write("p.log", content);

    EXPECT_EQ(m_fs->GetFileSize(Path("p.log")), static_cast<int64_t>(content.size()));
    EXPECT_EQ(Read("p.log"), content);
}

TEST_P(PageBoundaryTests, TruncateToTheBoundarySizeKeepsThePrefix) {
    const auto content = Pattern(1500);
    Write("p.log", content);

    m_fs->Truncate(Path("p.log"), static_cast<int64_t>(GetParam()));

    EXPECT_EQ(m_fs->GetFileSize(Path("p.log")), static_cast<int64_t>(GetParam()));
    EXPECT_EQ(Read("p.log"), content.substr(0, GetParam()));
}

TEST_P(PageBoundaryTests, ReopenAfterSyncAndAppendPreservesTheExistingBytes) {
    const auto first = Pattern(GetParam());
    const std::string second = "tail-bytes";
    {
        auto file = m_fs->CreateWriteableFile(Path("p.log"));
        file.Append(std::span<const char>(first.data(), first.size()));
        file.Sync();
        file.Close();
    }
    {
        auto file = m_fs->ReopenWriteableFile(Path("p.log"));
        file.Append(std::span<const char>(second.data(), second.size()));
        file.Close();
    }

    EXPECT_EQ(Read("p.log"), first + second);
}

TEST_P(PageBoundaryTests, ReopenAfterFlushOnlyThenCloseKeepsTheBytes) {
    const auto first = Pattern(GetParam());
    {
        auto file = m_fs->CreateWriteableFile(Path("p.log"));
        file.Append(std::span<const char>(first.data(), first.size()));
        file.Flush();
        file.Close();
    }

    auto file = m_fs->ReopenWriteableFile(Path("p.log"));
    EXPECT_EQ(file.GetFileSize(), static_cast<int64_t>(first.size()));
    file.Close();
    EXPECT_EQ(Read("p.log"), first);
}

INSTANTIATE_TEST_SUITE_P(Sizes, PageBoundaryTests, ::testing::Values<size_t>(0, 1, 511, 512, 513, 1023, 1024, 1025),
                         [](const auto& info) { return "Size" + std::to_string(info.param); });

// Rename failures: whichever step fails, the source survives and an existing destination keeps its content.
class RenameFailureTests : public BlobFilesystemFakeTests, public ::testing::WithParamInterface<FakeBlobServer::Operation> {
  protected:
    // Fails the `occurrence`-th (1-based) request of the given operation, and only that one.
    void FailNth(const FakeBlobServer::Operation target, const int occurrence) {
        m_server->Fault = [target, occurrence, seen = 0](const auto operation) mutable
            -> std::optional<AzureClient::BlobStorageError> {
            if (operation == target && ++seen == occurrence) {
                return FakeBlobServer::Error(400U, "InvalidInput");
            }
            return std::nullopt;
        };
    }

    [[nodiscard]] std::vector<std::string> Children() {
        auto children = m_fs->GetChildren(Path("d"));
        std::ranges::sort(children);
        return children;
    }
};

TEST_P(RenameFailureTests, FailureWhileCopyingOverAnExistingDestinationLeavesBothFilesIntactAndNoTemporaryBlob) {
    Write("d/old", "source content");
    Write("d/new", "original destination");
    FailNth(GetParam(), 1);

    EXPECT_ANY_THROW(m_fs->RenameFile(Path("d/old"), Path("d/new")));

    m_server->Fault = nullptr;
    EXPECT_EQ(Read("d/old"), "source content");
    EXPECT_EQ(Read("d/new"), "original destination");
    EXPECT_EQ(Children(), (std::vector<std::string>{"new", "old"}));
}

TEST_P(RenameFailureTests, FailureWhileCopyingToANewDestinationLeavesNoPartialDestination) {
    Write("d/old", "source content");
    FailNth(GetParam(), 1);

    EXPECT_ANY_THROW(m_fs->RenameFile(Path("d/old"), Path("d/new")));

    m_server->Fault = nullptr;
    EXPECT_EQ(Read("d/old"), "source content");
    EXPECT_EQ(Children(), (std::vector<std::string>{"old"}));
}

INSTANTIATE_TEST_SUITE_P(CopyAndSizeSteps, RenameFailureTests,
                         ::testing::Values(FakeBlobServer::Operation::UploadPagesFromUrl,
                                           FakeBlobServer::Operation::SetMetadata));

TEST_F(BlobFilesystemFakeTests, FailureToDeleteTheSourceKeepsTheNewDestinationAndLeavesNoTemporaryBlob) {
    Write("d/old", "source content");
    Write("d/new", "original destination");
    // The first delete removes the temporary blob; the second is the source.
    int deletes = 0;
    m_server->Fault = [&deletes](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::DeleteBlob && ++deletes == 2) {
            return FakeBlobServer::Error(400U, "InvalidInput");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW(m_fs->RenameFile(Path("d/old"), Path("d/new")));

    m_server->Fault = nullptr;
    EXPECT_EQ(Read("d/new"), "source content");
    auto children = m_fs->GetChildren(Path("d"));
    std::ranges::sort(children);
    EXPECT_EQ(children, (std::vector<std::string>{"new", "old"}));
}

TEST_F(BlobFilesystemFakeTests, FailureOfTheFinalSizeCommitLeavesTheOverwrittenDestinationEmptyNotMixed) {
    Write("d/old", "source content");
    Write("d/new", "original destination");
    int sizeWrites = 0;
    m_server->Fault = [&sizeWrites](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        // The first size write belongs to the temporary blob; the second is the destination's final commit.
        if (operation == FakeBlobServer::Operation::SetMetadata && ++sizeWrites == 2) {
            return FakeBlobServer::Error(400U, "InvalidInput");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW(m_fs->RenameFile(Path("d/old"), Path("d/new")));

    m_server->Fault = nullptr;
    const auto size = m_fs->GetFileSize(Path("d/new"));
    EXPECT_EQ(size, 0);
    EXPECT_EQ(Read("d/old"), "source content");
}
TEST_F(BlobFilesystemFakeTests, RenameOverAnExistingDestinationReplacesItsContent) {
    Write("d/old", "short");
    Write("d/new", "a much longer original destination");

    m_fs->RenameFile(Path("d/old"), Path("d/new"));

    EXPECT_EQ(Read("d/new"), "short");
    EXPECT_EQ(m_fs->GetFileSize(Path("d/new")), 5);
    EXPECT_EQ(m_fs->GetChildren(Path("d")), (std::vector<std::string>{"new"}));
}

TEST_F(BlobFilesystemFakeTests, LockFileHoldsTheLeaseUntilUnlocked) {
    auto lock = m_fs->LockFile(Path("LOCK"));

    EXPECT_EQ(m_fs->GetLeaseClientCount(), 1U);
    EXPECT_EQ(m_server->LeaseAcquisitions, 1);

    m_fs->UnlockFile(*lock);

    EXPECT_EQ(m_fs->GetLeaseClientCount(), 0U);
    EXPECT_EQ(m_server->LeaseReleases, 1);
}

TEST_F(BlobFilesystemFakeTests, SecondFilesystemCannotLockWhileTheLeaseIsHeldAndCanOnceItIsReleased) {
    auto first = m_fs->LockFile(Path("LOCK"));
    ASSERT_EQ(m_server->LeaseAcquisitions, 1);

    // A short timeout bounds how long the contender waits on the held 20 second lease.
    FakeFilesystem second(
        m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(), {},
        Configuration::MaxCacheSize, std::chrono::milliseconds(500));

    const auto start = std::chrono::steady_clock::now();
    EXPECT_THROW((void)second.LockFile(Path("LOCK")), std::runtime_error);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(10));
    EXPECT_EQ(second.GetLeaseClientCount(), 0U);
    EXPECT_EQ(m_server->LeaseAcquisitions, 1);

    m_fs->UnlockFile(*first);

    auto acquired = second.LockFile(Path("LOCK"));
    EXPECT_EQ(m_server->LeaseAcquisitions, 2);
    EXPECT_EQ(second.GetLeaseClientCount(), 1U);
    second.UnlockFile(*acquired);
    EXPECT_EQ(m_server->LeaseReleases, 2);
}

TEST_F(BlobFilesystemFakeTests, LockFileOnADifferentBlobIsNotBlockedByAHeldLease) {
    auto first = m_fs->LockFile(Path("LOCK"));

    auto other = m_fs->LockFile(Path("OTHER"));

    EXPECT_EQ(m_server->LeaseAcquisitions, 2);
    m_fs->UnlockFile(*other);
    m_fs->UnlockFile(*first);
}

TEST_F(BlobFilesystemFakeTests, LoggerWritesRateLimitedLinesToTheBlob) {
    {
        auto logger = m_fs->CreateLogger(Path("LOG"), 0);
        logger.Logv(1, "hello %s", "logger");
        logger.Flush();
    }

    EXPECT_NE(Read("LOG").find("hello logger"), std::string::npos);
}

TEST(BlobFilesystemFakeRuntimeTests, RuntimeBuiltOnAnIoContextRunsTheTransportOnIt) {
    boost::asio::io_context context;
    BasicClientRuntime<FakeHttpClient> runtime(context);

    EXPECT_EQ(runtime.HttpClient().get_executor(), context.get_executor());
}

TEST_F(BlobFilesystemFakeTests, FencedRuntimeRejectsFurtherOperations) {
    m_runtime->FenceWrites();

    EXPECT_THROW((void)m_fs->FileExists(Path("a.log")), std::runtime_error);
}
} // namespace
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
