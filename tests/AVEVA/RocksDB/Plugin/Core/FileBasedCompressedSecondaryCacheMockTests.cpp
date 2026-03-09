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

    auto cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_mockFs);

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
// [Mock I/O] MapReadOnly returns nullptr: Lookup returns nullptr, removes
// the entry from the index, and zeroes usage so subsequent lookups also miss
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheMockTests, MapReadOnlyFailure_LookupReturnsNullAndCleansIndex)
{
    // Make WriteFileAtomic succeed so the entry is added to the in-memory index.
    ON_CALL(*m_mockFs, WriteFileAtomic(_, _, _)).WillByDefault(Return(true));
    // NiceMock returns nullptr (default-constructed unique_ptr) for MapReadOnly; explicit
    // ON_CALL documents that this is the failure under test.
    ON_CALL(*m_mockFs, MapReadOnly(_))
        .WillByDefault([](const std::filesystem::path&) noexcept
                       -> std::unique_ptr<AVEVA::RocksDB::Plugin::Core::MappedFileView> {
            return nullptr;
        });

    auto cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_mockFs);

    // Insert succeeds (WriteFileAtomic returns true): entry is registered in the index.
    TestPayload payload{"real data"};
    auto s = cache->Insert(MakeKey("map_fail_key"), &payload, &m_helper, /*force_insert=*/true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    size_t usage = 0;
    ASSERT_TRUE(cache->GetUsage(usage).ok());
    EXPECT_GT(usage, 0u) << "Entry must be tracked in the index after a successful Insert";

    // Lookup fails because MapReadOnly returns nullptr.
    bool kept = false;
    auto handle = cache->Lookup(MakeKey("map_fail_key"), &m_helper,
                                nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr) << "Lookup must return nullptr when MapReadOnly fails";
    EXPECT_FALSE(kept);

    // The failed mapping must have removed the entry from the index.
    ASSERT_TRUE(cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u) << "Entry must be removed from the index after a failed mapping";

    // A second Lookup must also miss — no phantom index entry remains.
    auto handle2 = cache->Lookup(MakeKey("map_fail_key"), &m_helper,
                                 nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle2, nullptr) << "Subsequent Lookup must also miss after index cleanup";
}
