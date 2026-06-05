// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

// --------------------------------------------------------------------------
// [Mock I/O] WriteFileAtomic failure: Insert returns IOError and leaves
// usage at zero — the entry must not be tracked in the index
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheMockTests, WriteFileAtomicFailure_InsertReturnsIOError)
{
    // NiceMock returns false for bool by default; explicit ON_CALL documents intent.
    ON_CALL(*m_mockFs, WriteFileAtomic(_, _, _)).WillByDefault(Return(false));

    auto cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_mockFs,
        FileBasedCompressedSecondaryCache::kDefaultCapacity,
        MakeNullLogger());

    TestPayload payload{"data that will fail to write"};
    auto s = cache->Insert(MakeKey("io_fail_key"), &payload, &m_helper, /*force_insert=*/true);

    EXPECT_TRUE(s.IsIOError())
        << "WriteFileAtomic failure must surface as IOError; got: " << s.ToString();

    size_t usage = 0;
    ASSERT_TRUE(cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u) << "No bytes must be tracked when the write fails";

    bool kept = false;
    EXPECT_EQ(cache->Lookup(MakeKey("io_fail_key"), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr) << "Failed Insert must not leave the key findable";
}

// --------------------------------------------------------------------------
// [Mock I/O] ReadFileContents returns nullopt: Lookup returns nullptr,
// removes the entry from the index, and zeroes usage so subsequent lookups
// also miss
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheMockTests, ReadFileContentsFailure_LookupReturnsNullAndCleansIndex)
{
    // Make WriteFileAtomic succeed so the entry is added to the in-memory index.
    ON_CALL(*m_mockFs, WriteFileAtomic(_, _, _)).WillByDefault(Return(true));
    // ReadFileContents returns nullopt — simulates the file going missing between
    // Insert (index registration) and Lookup.
    ON_CALL(*m_mockFs, ReadFileContents(_))
        .WillByDefault([](const std::filesystem::path&) noexcept
                       -> std::optional<std::string> {
            return std::nullopt;
        });

    auto cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_mockFs,
        FileBasedCompressedSecondaryCache::kDefaultCapacity,
        MakeNullLogger());

    // Insert succeeds (WriteFileAtomic returns true): entry is registered in the index.
    TestPayload payload{"real data"};
    auto s = cache->Insert(MakeKey("read_fail_key"), &payload, &m_helper, /*force_insert=*/true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    size_t usage = 0;
    ASSERT_TRUE(cache->GetUsage(usage).ok());
    EXPECT_GT(usage, 0u) << "Entry must be tracked in the index after a successful Insert";

    // Lookup fails because ReadFileContents returns nullopt.
    bool kept = false;
    auto handle = cache->Lookup(MakeKey("read_fail_key"), &m_helper,
                                nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr) << "Lookup must return nullptr when ReadFileContents fails";
    EXPECT_FALSE(kept);

    // The failed read must have removed the entry from the index.
    ASSERT_TRUE(cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u) << "Entry must be removed from the index after a failed read";

    // A second Lookup must also miss — no phantom index entry remains.
    auto handle2 = cache->Lookup(MakeKey("read_fail_key"), &m_helper,
                                 nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle2, nullptr) << "Subsequent Lookup must also miss after index cleanup";
}

// --------------------------------------------------------------------------
// [Mock I/O] InsertSaved calls WriteFileAtomic — verifies the cache writes
// pre-serialised block data to disk when RocksDB hands it to the secondary tier.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheMockTests, InsertSaved_CallsWriteFileAtomic)
{
    EXPECT_CALL(*m_mockFs, WriteFileAtomic(_, _, _))
        .Times(1)
        .WillOnce(Return(true));

    auto cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_mockFs,
        FileBasedCompressedSecondaryCache::kDefaultCapacity,
        MakeNullLogger());

    const std::string keyData(16, '\x42');
    const std::string payload(64, 'X');

    auto status = cache->InsertSaved(
        rocksdb::Slice(keyData),
        rocksdb::Slice(payload),
        rocksdb::CompressionType::kNoCompression,
        rocksdb::CacheTier::kVolatileTier);

    EXPECT_TRUE(status.ok()) << status.ToString();
}

