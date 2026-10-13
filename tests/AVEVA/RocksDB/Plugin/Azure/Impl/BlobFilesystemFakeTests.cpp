// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "FakeBlobServer.hpp"
#include "FakeHttpClient.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
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

TEST_F(BlobFilesystemFakeTests, TruncateShrinksTheLogicalSize) {
    Write("a.log", "0123456789");

    m_fs->Truncate(Path("a.log"), 4);

    EXPECT_EQ(m_fs->GetFileSize(Path("a.log")), 4);
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
