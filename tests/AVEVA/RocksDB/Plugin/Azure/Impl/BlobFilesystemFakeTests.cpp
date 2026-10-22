// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/TempDirectory.hpp"
#include "BasicReadableFile.ipp"
#include "BlobFilesystemImpl.ipp"
#include "ClientFactory.ipp"
#include "FakeBlobEnvironment.hpp"
#include "FakeBlobServer.hpp"
#include "FakeHttpClient.hpp"
#include "LockFileImpl.ipp"
#include "ReadWriteFileImpl.ipp"
#include "ReadableFileImpl.ipp"
#include "SequentialFileImpl.ipp"
#include "WriteableFileImpl.ipp"

#include <gtest/gtest.h>

#include <boost/asio/post.hpp>
#include <boost/log/core.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
namespace {
using TempDirectory = Core::Mocks::TempDirectory;
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
        m_fs = std::make_unique<FakeFilesystem>(
            m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
            std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>());
        m_prefix = StorageAccount::UniquePrefix(AccountUrl, Container);
    }

    ~BlobFilesystemFakeTests() override { m_fs.reset(); }

    [[nodiscard]] std::string Path(const std::string& name) const { return m_prefix + "/" + name; }

    // Larger than Transfer::MaxInlineRenameSize, so a rename takes the server-side copy path.
    [[nodiscard]] static const std::string& LargeContent() {
        static const std::string content(static_cast<size_t>(Configuration::Transfer::MaxInlineRenameSize) + 1024, 'L');
        return content;
    }

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

    // Runtime, fake transport (completing inline, with a background pump) and in-memory service, assembled once for
    // every Impl test; declared before m_fs so the filesystem is released first.
    FakeBlobEnvironment m_env{Container, "unused"};
    std::shared_ptr<BasicClientRuntime<FakeHttpClient>>& m_runtime = m_env.Runtime;
    std::shared_ptr<FakeBlobServer>& m_server = m_env.Blob;
    std::unique_ptr<FakeFilesystem> m_fs;
    std::string m_prefix;
};

TEST_F(BlobFilesystemFakeTests, WrittenFileCanBeReadBackAndReportsItsLogicalSize) {
    Write("000001.sst", "hello page blob");

    EXPECT_TRUE(m_fs->FileExists(Path("000001.sst")));
    EXPECT_EQ(m_fs->GetFileSize(Path("000001.sst")), 15);
    EXPECT_EQ(Read("000001.sst"), "hello page blob");
}

TEST_F(BlobFilesystemFakeTests, RepeatedGetFileSizeIsServedFromTheShortLivedCache) {
    Write("a.sst", "12345");
    EXPECT_EQ(m_fs->GetFileSize(Path("a.sst")), 5);
    const auto before = m_server->PropertiesRequests;

    EXPECT_EQ(m_fs->GetFileSize(Path("a.sst")), 5);
    EXPECT_EQ(m_fs->GetFileSize(Path("a.sst")), 5);

    EXPECT_EQ(m_server->PropertiesRequests, before);
}

TEST_F(BlobFilesystemFakeTests, OpeningAFileRepeatedlyCostsOnePropertiesRequestAndWritesInvalidateIt) {
    Write("a.sst", "12345");
    const auto before = m_server->PropertiesRequests;

    EXPECT_EQ(Read("a.sst"), "12345");
    const auto afterFirstOpen = m_server->PropertiesRequests;
    EXPECT_EQ(Read("a.sst"), "12345");
    EXPECT_EQ(Read("a.sst"), "12345");
    EXPECT_LE(afterFirstOpen - before, 1);
    EXPECT_EQ(m_server->PropertiesRequests, afterFirstOpen);

    Write("a.sst", "123456789");
    EXPECT_EQ(Read("a.sst"), "123456789");
}

TEST_F(BlobFilesystemFakeTests, WritesTruncatesAndDeletesInvalidateTheCachedSize) {
    Write("a.sst", "12345");
    EXPECT_EQ(m_fs->GetFileSize(Path("a.sst")), 5);

    m_fs->Truncate(Path("a.sst"), 2);
    EXPECT_EQ(m_fs->GetFileSize(Path("a.sst")), 2);

    Write("a.sst", "123456789");
    EXPECT_EQ(m_fs->GetFileSize(Path("a.sst")), 9);

    m_fs->RenameFile(Path("a.sst"), Path("b.sst"));
    EXPECT_THROW((void)m_fs->GetFileSize(Path("a.sst")), RequestFailedException);

    EXPECT_EQ(m_fs->GetFileSize(Path("b.sst")), 9);
    EXPECT_TRUE(m_fs->DeleteFile(Path("b.sst")));
    EXPECT_THROW((void)m_fs->GetFileSize(Path("b.sst")), RequestFailedException);
}

TEST_F(BlobFilesystemFakeTests, MissingFileDoesNotExist) { EXPECT_FALSE(m_fs->FileExists(Path("missing.sst"))); }

TEST_F(BlobFilesystemFakeTests, HasChildrenIssuesOneListRequestRegardlessOfDirectorySize) {
    for (int i = 0; i < 5000; ++i) {
        (void)m_server->AddBlob("big/" + std::to_string(i), 1);
    }
    std::atomic<int> lists = 0;
    m_server->Fault = [&lists](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::ListBlobs) {
            ++lists;
        }
        return std::nullopt;
    };

    EXPECT_TRUE(m_fs->HasChildren(Path("big")));
    EXPECT_EQ(lists.load(), 1);
    EXPECT_FALSE(m_fs->HasChildren(Path("bi")));
    EXPECT_FALSE(m_fs->HasChildren(Path("missing")));
    m_server->Fault = nullptr;
}

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

TEST_F(BlobFilesystemFakeTests, RenameOfASmallFileDownloadsOnceAndCopiesNothingInsideTheService) {
    Write("old.sst", "payload");
    Write("new.sst", "to be replaced");
    const auto downloadsBefore = m_server->Downloads.size();

    m_fs->RenameFile(Path("old.sst"), Path("new.sst"));

    EXPECT_TRUE(m_server->PageCopies.empty());
    EXPECT_EQ(m_server->Downloads.size(), downloadsBefore + 1);
    EXPECT_EQ(Read("new.sst"), "payload");
    for (const auto& name : m_server->BlobNames()) {
        EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName(name)) << name;
    }
}

TEST(RenameTempNameTests, OnlyExactGeneratedUuidSuffixesAreRecognized) {
    const std::string uuid = "123e4567-e89b-12d3-a456-426614174000";
    EXPECT_TRUE(BlobFilesystemDetail::IsRenameTempName("d/new.rename-" + uuid));
    EXPECT_TRUE(BlobFilesystemDetail::IsRenameTempName("new.rename-" + uuid));

    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName("d/new.rename-" + std::string(36, 'x')));
    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName("d/new.rename-123e4567-e89b-12d3-a456-42661417400g"));
    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName("d/new.rename-123E4567-E89B-12D3-A456-426614174000"));
    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName("d/new.rename-123e4567e-89b-12d3-a456-426614174000"));
    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName("d/new.rename-123e4567-e89b-12d3-a456-426614174000x"));
    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName("d/new.rename-" + uuid.substr(1)));
    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName(".rename-" + uuid));
    EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName("d/new.rename" + uuid + "-"));
}

TEST_F(BlobFilesystemFakeTests, RenameOfAnEmptyFileCreatesAnEmptyDestination) {
    Write("old.sst", "");

    m_fs->RenameFile(Path("old.sst"), Path("new.sst"));

    EXPECT_FALSE(m_fs->FileExists(Path("old.sst")));
    EXPECT_EQ(m_fs->GetFileSize(Path("new.sst")), 0);
}

TEST_F(BlobFilesystemFakeTests, FailureWhileUploadingASmallRenameLeavesTheSourceAndNoPartialDestination) {
    Write("old.sst", "payload");
    m_server->Fault = [](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        return operation == FakeBlobServer::Operation::UploadPages
                   ? std::optional{FakeBlobServer::Error(400U, "InvalidInput")}
                   : std::nullopt;
    };

    EXPECT_ANY_THROW(m_fs->RenameFile(Path("old.sst"), Path("new.sst")));

    m_server->Fault = nullptr;
    EXPECT_EQ(Read("old.sst"), "payload");
    EXPECT_FALSE(m_fs->FileExists(Path("new.sst")));
}

TEST_F(BlobFilesystemFakeTests, CrashDuringALargeRenameLeavesATemporaryBlobThatIsHiddenAndPurgedOnStart) {
    Write("d/old", LargeContent());
    Write("d/new", "original destination");
    // The process "dies" at the destination's final size write: nothing after it reaches the service, so the
    // temporary blob and the emptied destination stay behind.
    int sizeWrites = 0;
    bool crashed = false;
    m_server->Fault = [&](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::SetMetadata && ++sizeWrites == 2) {
            crashed = true;
        }
        return crashed ? std::optional{FakeBlobServer::Error(400U, "ProcessCrashed")} : std::nullopt;
    };
    EXPECT_ANY_THROW(m_fs->RenameFile(Path("d/old"), Path("d/new")));
    m_server->Fault = nullptr;

    const auto orphans = [&] {
        return std::ranges::count_if(m_server->BlobNames(),
                                     [](const auto& name) { return BlobFilesystemDetail::IsRenameTempName(name); });
    };
    EXPECT_EQ(orphans(), 1);
    auto children = m_fs->GetChildren(Path("d"));
    std::ranges::sort(children);
    EXPECT_EQ(children, (std::vector<std::string>{"new", "old"}));
    for (const auto& attribute : m_fs->GetChildrenFileAttributes(Path("d"))) {
        EXPECT_FALSE(BlobFilesystemDetail::IsRenameTempName(attribute.GetName()));
    }

    // The restarted process still has the source, so the rename can simply be repeated.
    FakeFilesystem restarted(
        m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>());
    EXPECT_EQ(orphans(), 1) << "Constructing a second filesystem must not delete a temp it does not own the lock for";
    const auto lock = restarted.LockFile(Path("LOCK"));
    EXPECT_EQ(orphans(), 0);
    restarted.UnlockFile(*lock);
    EXPECT_EQ(Read("d/old"), LargeContent());
    restarted.RenameFile(Path("d/old"), Path("d/new"));
    EXPECT_EQ(Read("d/new"), LargeContent());
}

TEST_F(BlobFilesystemFakeTests, LockingOneDirectoryOnlyPurgesItsOwnOrphanedRenameTemps) {
    for (const std::string dir : {"a", "b"}) {
        Write(dir + "/old", LargeContent());
        Write(dir + "/new", "original destination");
        int sizeWrites = 0;
        bool crashed = false;
        m_server->Fault = [&](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
            if (operation == FakeBlobServer::Operation::SetMetadata && ++sizeWrites == 2) {
                crashed = true;
            }
            return crashed ? std::optional{FakeBlobServer::Error(400U, "ProcessCrashed")} : std::nullopt;
        };
        EXPECT_ANY_THROW(m_fs->RenameFile(Path(dir + "/old"), Path(dir + "/new")));
        m_server->Fault = nullptr;
    }
    const auto orphansIn = [&](const std::string& dir) {
        return std::ranges::count_if(m_server->BlobNames(), [&](const auto& name) {
            return name.starts_with(dir + "/") && BlobFilesystemDetail::IsRenameTempName(name);
        });
    };
    ASSERT_EQ(orphansIn("a"), 1);
    ASSERT_EQ(orphansIn("b"), 1);

    const auto lock = m_fs->LockFile(Path("a/LOCK"));

    EXPECT_EQ(orphansIn("a"), 0);
    EXPECT_EQ(orphansIn("b"), 1);
    m_fs->UnlockFile(*lock);
}

TEST_F(BlobFilesystemFakeTests, LockingAParentDoesNotPurgeTheTempOfANestedDatabaseThatHoldsItsOwnLock) {
    const auto childLock = m_fs->LockFile(Path("a/child/LOCK"));
    Write("a/child/old", LargeContent());
    Write("a/child/new", "original destination");
    int sizeWrites = 0;
    bool crashed = false;
    m_server->Fault = [&](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::SetMetadata && ++sizeWrites == 2) {
            crashed = true;
        }
        return crashed ? std::optional{FakeBlobServer::Error(400U, "ProcessCrashed")} : std::nullopt;
    };
    EXPECT_ANY_THROW(m_fs->RenameFile(Path("a/child/old"), Path("a/child/new")));
    m_server->Fault = nullptr;
    const auto orphans = [&] {
        return std::ranges::count_if(m_server->BlobNames(),
                                     [](const auto& name) { return BlobFilesystemDetail::IsRenameTempName(name); });
    };
    ASSERT_EQ(orphans(), 1);

    FakeFilesystem parent(
        m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>());
    const auto parentLock = parent.LockFile(Path("a/LOCK"));
    EXPECT_EQ(orphans(), 1);
    parent.UnlockFile(*parentLock);
    const auto rootLock = parent.LockFile(Path("LOCK"));
    EXPECT_EQ(orphans(), 1);
    parent.UnlockFile(*rootLock);
    m_fs->UnlockFile(*childLock);
}

// The filesystem authenticates with Shared Key, which cannot authorize a SAS-less copy source, so the rename copies
// through the client without first sending a Put Page From URL that is bound to be refused.
TEST_F(BlobFilesystemFakeTests, SharedKeyRenameCopiesThroughTheClientWithoutSendingAServerSideCopy) {
    Write("old.sst", LargeContent());

    m_fs->RenameFile(Path("old.sst"), Path("new.sst"));

    EXPECT_TRUE(m_server->PageCopies.empty());
    EXPECT_EQ(Read("new.sst"), LargeContent());
}

TEST_F(BlobFilesystemFakeTests, RenameFallsBackToCopyingThroughTheClientWhenTheSourceCannotBeAuthorized) {
    Write("old.sst", LargeContent());
    m_server->RequireCopySourceAuthorization = true;

    m_fs->RenameFile(Path("old.sst"), Path("new.sst"));

    EXPECT_FALSE(m_fs->FileExists(Path("old.sst")));
    EXPECT_EQ(Read("new.sst"), LargeContent());
    EXPECT_EQ(m_fs->GetFileSize(Path("new.sst")), static_cast<int64_t>(LargeContent().size()));
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
    const TempDirectory tempDir("truncate_cache");
    const auto cacheString = tempDir.Path().string();
    FakeFilesystem cached(
        m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(),
        std::string_view(cacheString));
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
        hit = file->TryReadFromCache(0, static_cast<int64_t>(buffer.size()), buffer.data());
        if (!hit) {
            ASSERT_TRUE(cached.WaitForFileCachesIdleForTesting(std::chrono::seconds(30)))
                << "Background download did not finish";
        }
    }
    ASSERT_TRUE(hit.has_value()) << "File was not cached after the background download finished";

    cached.Truncate(Path("t.sst"), 512);

    EXPECT_FALSE(file->TryReadFromCache(0, 512, buffer.data()).has_value());
}
TEST_F(BlobFilesystemFakeTests, RecreatingAFileDropsItsCachedCopy) {
    const TempDirectory tempDir("recreate_cache");
    const auto cacheString = tempDir.Path().string();
    FakeFilesystem cached(
        m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(),
        std::string_view(cacheString));
    {
        auto writer = cached.CreateWriteableFile(Path("r.sst"));
        const std::string content(1024, 'O');
        writer.Append(std::span<const char>(content.data(), content.size()));
        writer.Close();
    }
    auto file = cached.CreateReadableFile(Path("r.sst"));
    std::vector<char> buffer(1024);
    std::optional<size_t> hit;
    for (int attempt = 0; attempt < 3 && !hit; ++attempt) {
        hit = file->TryReadFromCache(0, static_cast<int64_t>(buffer.size()), buffer.data());
        if (!hit) {
            ASSERT_TRUE(cached.WaitForFileCachesIdleForTesting(std::chrono::seconds(30)))
                << "Background download did not finish";
        }
    }
    ASSERT_TRUE(hit.has_value()) << "File was not cached after the background download finished";

    cached.CreateWriteableFile(Path("r.sst")).Close();
    EXPECT_FALSE(file->TryReadFromCache(0, static_cast<int64_t>(buffer.size()), buffer.data()).has_value());
}
TEST_F(BlobFilesystemFakeTests, DeleteDirDropsTheCachedCopiesOfItsFiles) {
    const TempDirectory tempDir("deletedir_cache");
    const auto cacheString = tempDir.Path().string();
    FakeFilesystem cached(
        m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(),
        std::string_view(cacheString));
    {
        auto writer = cached.CreateWriteableFile(Path("dir/d.sst"));
        const std::string content(1024, 'O');
        writer.Append(std::span<const char>(content.data(), content.size()));
        writer.Close();
    }
    auto file = cached.CreateReadableFile(Path("dir/d.sst"));
    std::vector<char> buffer(1024);
    std::optional<size_t> hit;
    for (int attempt = 0; attempt < 3 && !hit; ++attempt) {
        hit = file->TryReadFromCache(0, static_cast<int64_t>(buffer.size()), buffer.data());
        if (!hit) {
            ASSERT_TRUE(cached.WaitForFileCachesIdleForTesting(std::chrono::seconds(30)))
                << "Background download did not finish";
        }
    }
    ASSERT_TRUE(hit.has_value()) << "File was not cached after the background download finished";

    ASSERT_EQ(cached.DeleteDir(Path("dir")), 0U);
    EXPECT_FALSE(file->TryReadFromCache(0, static_cast<int64_t>(buffer.size()), buffer.data()).has_value());
    {
        auto writer = cached.CreateWriteableFile(Path("dir/d.sst"));
        const std::string content(1024, 'N');
        writer.Append(std::span<const char>(content.data(), content.size()));
        writer.Close();
    }
    auto recreated = cached.CreateReadableFile(Path("dir/d.sst"));

    // Whatever the cache serves for the recreated blob must be the new content, never the deleted one.
    std::vector<char> after(1024);
    if (recreated->TryReadFromCache(0, static_cast<int64_t>(after.size()), after.data())) {
        EXPECT_EQ(std::string(after.begin(), after.end()), std::string(1024, 'N'));
    }
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
    CachedFailureTests() : m_tempDir("failure_cache") {
        const auto dir = m_tempDir.Path().string();
        m_cached = std::make_unique<FakeFilesystem>(
            m_runtime, Container, AccountUrl, AccountKey, 4096, 1024,
            std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>(),
            std::string_view(dir));
    }

    ~CachedFailureTests() override { m_cached.reset(); }

    void WriteCached(const std::string& name, const std::string& content) {
        auto file = m_cached->CreateWriteableFile(Path(name));
        file.Append(std::span<const char>(content.data(), content.size()));
        file.Close();
    }

    // Reads until the background download has cached the file; returns whether it did.
    template <typename TFile> bool WarmCache(TFile& file, const size_t size) {
        std::vector<char> buffer(size);
        for (int attempt = 0; attempt < 3; ++attempt) {
            if (file->TryReadFromCache(0, static_cast<int64_t>(size), buffer.data())) {
                return true;
            }
            if (!m_cached->WaitForFileCachesIdleForTesting(std::chrono::seconds(30))) {
                return false;
            }
        }
        return false;
    }

    // Declared before m_cached so the filesystem releases the directory before it is removed.
    Core::Mocks::TempDirectory m_tempDir;
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
    EXPECT_FALSE(file->TryReadFromCache(0, 512, buffer.data()).has_value());
}

TEST_F(CachedFailureTests, RenameThatFailsMidCommitStillDropsTheDestinationsCachedCopy) {
    WriteCached("old.sst", LargeContent());
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
    EXPECT_FALSE(file->TryReadFromCache(0, 1024, buffer.data()).has_value());
}

TEST_F(CachedFailureTests, DeleteThatFailsStillDropsTheCachedCopy) {
    WriteCached("d.sst", std::string(1024, 'D'));
    auto file = m_cached->CreateReadableFile(Path("d.sst"));
    ASSERT_TRUE(WarmCache(file, 1024));
    m_server->Fault = [](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::DeleteBlob) {
            return FakeBlobServer::Error(400U, "InvalidInput");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW((void)m_cached->DeleteFile(Path("d.sst")));

    std::vector<char> buffer(1024);
    EXPECT_FALSE(file->TryReadFromCache(0, 1024, buffer.data()).has_value());
}

TEST_F(CachedFailureTests, OverwritingCreateThatFailsStillDropsTheCachedCopy) {
    WriteCached("o.sst", std::string(1024, 'O'));
    auto file = m_cached->CreateReadableFile(Path("o.sst"));
    ASSERT_TRUE(WarmCache(file, 1024));
    m_server->Fault = [](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::Create) {
            return FakeBlobServer::Error(400U, "InvalidInput");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW((void)m_cached->CreateWriteableFile(Path("o.sst")));
    EXPECT_ANY_THROW((void)m_cached->ReuseWritableFile(Path("o.sst")));

    std::vector<char> buffer(1024);
    EXPECT_FALSE(file->TryReadFromCache(0, 1024, buffer.data()).has_value());
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

TEST_F(BlobFilesystemFakeTests, ReopenWriteableFile_OfAMissingFile_CreatesItAndAcceptsAppends) {
    ASSERT_FALSE(m_fs->FileExists(Path("missing.log")));
    const std::string data = "appended after reopen";
    {
        auto file = m_fs->ReopenWriteableFile(Path("missing.log"));
        EXPECT_EQ(0, file.GetFileSize());
        file.Append(std::span<const char>(data.data(), data.size()));
        file.Close();
    }

    EXPECT_TRUE(m_fs->FileExists(Path("missing.log")));
    EXPECT_EQ(Read("missing.log"), data);
}

TEST_F(BlobFilesystemFakeTests, ReopenWriteableFile_OfAMissingFile_DoesNotLookUpTheBlobItJustCreated) {
    const auto before = m_server->PropertiesRequests;

    const auto file = m_fs->ReopenWriteableFile(Path("missing.log"));

    EXPECT_EQ(0, file.GetFileSize());
    EXPECT_EQ(m_server->PropertiesRequests, before);
}

TEST_F(BlobFilesystemFakeTests, ReopenWriteableFile_ThenCloseWithoutAppending_WritesNothing) {
    const std::string data(700, 'x');
    {
        auto file = m_fs->CreateWriteableFile(Path("p.log"));
        file.Append(std::span<const char>(data.data(), data.size()));
        file.Close();
    }
    std::vector<FakeBlobServer::Operation> operations;
    m_server->Fault = [&operations](const auto operation) -> std::optional<AzureClient::BlobStorageError> {
        operations.push_back(operation);
        return std::nullopt;
    };
    const auto uploadsBefore = m_server->Uploads.size();

    {
        auto file = m_fs->ReopenWriteableFile(Path("p.log"));
        file.Close();
    }
    m_server->Fault = nullptr;

    EXPECT_EQ(m_server->Uploads.size(), uploadsBefore);
    EXPECT_EQ(0, std::ranges::count(operations, FakeBlobServer::Operation::UploadPages));
    EXPECT_EQ(0, std::ranges::count(operations, FakeBlobServer::Operation::SetMetadata));
    EXPECT_EQ(Read("p.log"), data);
}

// Rename failures: whichever step fails, the source survives and an existing destination keeps its content.
class RenameFailureTests : public BlobFilesystemFakeTests,
                           public ::testing::WithParamInterface<FakeBlobServer::Operation> {
  protected:
    // Fails the `occurrence`-th (1-based) request of the given operation, and only that one.
    void FailNth(const FakeBlobServer::Operation target, const int occurrence) {
        m_server->Fault = [target, occurrence,
                           seen = 0](const auto operation) mutable -> std::optional<AzureClient::BlobStorageError> {
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
    Write("d/old", LargeContent());
    Write("d/new", "original destination");
    FailNth(GetParam(), 1);

    EXPECT_ANY_THROW(m_fs->RenameFile(Path("d/old"), Path("d/new")));

    m_server->Fault = nullptr;
    EXPECT_EQ(Read("d/old"), LargeContent());
    EXPECT_EQ(Read("d/new"), "original destination");
    EXPECT_EQ(Children(), (std::vector<std::string>{"new", "old"}));
}

TEST_P(RenameFailureTests, FailureWhileCopyingToANewDestinationLeavesNoPartialDestination) {
    Write("d/old", LargeContent());
    FailNth(GetParam(), 1);

    EXPECT_ANY_THROW(m_fs->RenameFile(Path("d/old"), Path("d/new")));

    m_server->Fault = nullptr;
    EXPECT_EQ(Read("d/old"), LargeContent());
    EXPECT_EQ(Children(), (std::vector<std::string>{"old"}));
}

INSTANTIATE_TEST_SUITE_P(CopyAndSizeSteps, RenameFailureTests,
                         ::testing::Values(FakeBlobServer::Operation::UploadPages,
                                           FakeBlobServer::Operation::SetMetadata));

TEST_F(BlobFilesystemFakeTests, FailureToDeleteTheSourceKeepsTheNewDestinationAndLeavesNoTemporaryBlob) {
    Write("d/old", LargeContent());
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
    EXPECT_EQ(Read("d/new"), LargeContent());
    auto children = m_fs->GetChildren(Path("d"));
    std::ranges::sort(children);
    EXPECT_EQ(children, (std::vector<std::string>{"new", "old"}));
}

TEST_F(BlobFilesystemFakeTests, FailureOfTheFinalSizeCommitLeavesTheOverwrittenDestinationEmptyNotMixed) {
    Write("d/old", LargeContent());
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
    EXPECT_EQ(Read("d/old"), LargeContent());
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
    try {
        (void)second.LockFile(Path("LOCK"));
        ADD_FAILURE() << "expected the lock to be contended";
    } catch (const std::runtime_error& ex) {
        EXPECT_NE(std::string(ex.what()).find("LOCK"), std::string::npos) << ex.what();
        EXPECT_NE(std::string(ex.what()).find(Container), std::string::npos) << ex.what();
    }
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

TEST_F(BlobFilesystemFakeTests,
       DestroyingTheFilesystemOnTheClientExecutorWithAReadInFlightLogsThePreconditionViolation) {
    namespace sinks = boost::log::sinks;
    Write("a.sst", std::string(256, 'D'));
    m_server->DeferDownloads = true;

    auto stream = std::make_shared<std::ostringstream>();
    auto backend = boost::make_shared<sinks::text_ostream_backend>();
    backend->add_stream(boost::shared_ptr<std::ostream>(stream.get(), [](std::ostream*) {}));
    backend->auto_flush(true);
    auto sink = boost::make_shared<sinks::synchronous_sink<sinks::text_ostream_backend>>(backend);
    const auto logged = [&] {
        const auto locked = sink->locked_backend();
        return stream->str().find("destroyed on a thread running the client io_context") != std::string::npos;
    };

    std::vector<char> scratch(64);
    rocksdb::FSReadRequest req;
    req.offset = 0;
    req.len = scratch.size();
    req.scratch = scratch.data();
    void* handle = nullptr;
    rocksdb::IOHandleDeleter deleter;
    {
        auto impl = m_fs->CreateReadableFile(Path("a.sst"));
        BasicReadableFile<typename decltype(impl)::element_type> file{std::move(impl)};
        ASSERT_TRUE(file.ReadAsync(
                            req, rocksdb::IOOptions{}, [](rocksdb::FSReadRequest&, void*) {}, nullptr, &handle,
                            &deleter, nullptr)
                        .ok());
    }
    ASSERT_TRUE(m_env.Http.RunUntil([&] { return m_server->PendingDownloads() > 0; }));
    auto download = m_server->TakeDownload();

    // The pump thread is stuck in the destructor, so a second thread must run the context for the read to drain.
    auto& context = m_env.Http.get_executor().context();
    std::atomic<bool> stop{false};
    std::jthread secondRunner([&] {
        while (!stop) {
            context.run_one_for(std::chrono::milliseconds(10));
        }
    });

    boost::log::core::get()->add_sink(sink);
    std::promise<void> destroyed;
    boost::asio::post(context, [&] {
        m_fs.reset();
        destroyed.set_value();
    });

    bool seen = false;
    for (auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
         !seen && std::chrono::steady_clock::now() < deadline; seen = logged()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    download.Succeed(std::string(64, 'D'));
    const auto status = destroyed.get_future().wait_for(std::chrono::seconds(30));
    boost::log::core::get()->remove_sink(sink);
    stop = true;
    secondRunner.join();
    if (handle != nullptr && deleter) {
        deleter(handle);
    }

    EXPECT_TRUE(seen) << stream->str();
    EXPECT_EQ(std::future_status::ready, status);
}

TEST_F(BlobFilesystemFakeTests, UnlockFileStillReleasesTheLeaseWhenWritesAreFencedAndIsIdempotent) {
    auto lock = m_fs->LockFile(Path("LOCK"));
    m_runtime->FenceWrites();

    EXPECT_NO_THROW(m_fs->UnlockFile(*lock));
    EXPECT_EQ(m_server->LeaseReleases, 1);

    EXPECT_NO_THROW(m_fs->UnlockFile(*lock));
    EXPECT_EQ(m_server->LeaseReleases, 1);
}

TEST_F(BlobFilesystemFakeTests, FencedRuntimeRejectsFurtherOperations) {
    m_runtime->FenceWrites();

    EXPECT_THROW((void)m_fs->FileExists(Path("a.log")), std::runtime_error);
}
} // namespace
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
