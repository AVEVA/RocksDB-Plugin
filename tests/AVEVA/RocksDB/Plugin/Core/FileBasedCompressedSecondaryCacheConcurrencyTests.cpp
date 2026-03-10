// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

// --------------------------------------------------------------------------
// Concurrent same-key inserts must not double-count m_currentSize.
// Each WriteEntry call uses a unique staging path, so concurrent writes for
// the same key no longer clobber each other's in-progress file.  After all
// threads join, the key must be present and usage must reflect a single entry.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConcurrentSameKeyInsert_UsageAccountedOnce)
{
    const std::string sharedKey = "concurrent_same_key";
    const std::string data(1024, 'C');

    constexpr int kThreadCount = 8;
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (int i = 0; i < kThreadCount; ++i)
    {
        threads.emplace_back([&] {
            TestPayload p{data};
            m_cache->Insert(MakeKey(sharedKey), &p, &m_helper, /*force_insert=*/true);
        });
    }
    for (auto& t : threads)
        t.join();

    // The key must be present: unique staging paths prevent file corruption.
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(sharedKey), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr) << "sharedKey must be present after concurrent same-key inserts";
    delete static_cast<TestPayload*>(handle->Value());

    // Usage must reflect at most one entry, not kThreadCount copies.
    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_LE(usage, FileBasedCompressedSecondaryCache::kFileHeaderSize + data.size())
        << "m_currentSize must not exceed one entry after concurrent same-key inserts "
        << "(usage=" << usage << " B, max=" << FileBasedCompressedSecondaryCache::kFileHeaderSize + data.size() << " B)";
}

// --------------------------------------------------------------------------
// Fix #1 regression: concurrent inserts of different keys must not leave
// m_currentSize above capacity.  Before the fix, each thread could pass
// Phase 1's capacity check independently; Phase 3 now calls
// EvictToCapacityLocked() to correct any such overshoot.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConcurrentDifferentKeyInserts_UsageWithinCapacity)
{
    // 10-byte payloads: at this size zstd adds overhead so entries are stored
    // uncompressed, giving a predictable on-disk size of (kFileHeaderSize + 10) bytes each.
    constexpr size_t kEntrySize = 10;
    constexpr int    kThreadCount = 16;

    // Capacity allows exactly 3 entries; the remaining 13 must be evicted.
    const size_t capacity = 3 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + kEntrySize);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (int i = 0; i < kThreadCount; ++i)
    {
        threads.emplace_back([&, i] {
            // Each thread gets a distinct 10-character payload.
            TestPayload p{std::string(kEntrySize, static_cast<char>('A' + (i % 26)))};
            m_cache->Insert(MakeKey("cc_diff_" + std::to_string(i)), &p, &m_helper, true);
        });
    }
    for (auto& t : threads)
        t.join();

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_LE(usage, capacity)
        << "m_currentSize must never exceed capacity after concurrent inserts "
        << "(usage=" << usage << " B, capacity=" << capacity << " B)";
}

// --------------------------------------------------------------------------
// Fix #2 regression: concurrent Lookup calls on distinct keys must all
// return correct data without deadlocking or corrupting the LRU state.
// Before the fix, concurrent Lookups serialised on the upgrade_lock; now
// Phase 1 uses shared_lock so all readers proceed simultaneously.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConcurrentLookups_AllReturnCorrectData)
{
    constexpr int kEntryCount  = 8;
    constexpr int kReaderCount = 4; // reader threads per entry

    // Pre-populate the cache with kEntryCount distinct entries.
    std::vector<std::string> keys(kEntryCount);
    std::vector<std::string> expectedData(kEntryCount);
    for (int i = 0; i < kEntryCount; ++i)
    {
        keys[i]         = "conc_lookup_key_" + std::to_string(i);
        expectedData[i] = "payload_" + std::to_string(i) + "_data";
        TestPayload p{expectedData[i]};
        ASSERT_TRUE(m_cache->Insert(MakeKey(keys[i]), &p, &m_helper, true).ok());
    }

    // Each reader thread looks up every key and records hits/misses.
    std::atomic<int> hits{0};
    std::atomic<int> dataErrors{0};

    std::vector<std::thread> threads;
    threads.reserve(kEntryCount * kReaderCount);
    for (int r = 0; r < kReaderCount; ++r)
    {
        for (int i = 0; i < kEntryCount; ++i)
        {
            threads.emplace_back([&, i] {
                bool kept = false;
                auto handle = m_cache->Lookup(MakeKey(keys[i]), &m_helper,
                                              nullptr, true, false, nullptr, kept);
                if (handle)
                {
                    hits.fetch_add(1, std::memory_order_relaxed);
                    auto* result = static_cast<TestPayload*>(handle->Value());
                    if (!result || result->data != expectedData[i])
                        dataErrors.fetch_add(1, std::memory_order_relaxed);
                    delete result;
                }
            });
        }
    }
    for (auto& t : threads)
        t.join();

    EXPECT_EQ(dataErrors.load(), 0)
        << "No concurrent Lookup must return incorrect data";
    EXPECT_GT(hits.load(), 0)
        << "At least some concurrent Lookups must succeed";
}

// --------------------------------------------------------------------------
// Concurrent mixed Insert + Erase + Lookup on overlapping keys must not
// crash, deadlock, or corrupt internal state
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConcurrentMixedInsertEraseLookup)
{
    constexpr int kKeyCount = 8;
    constexpr int kThreadsPerOp = 4;

    // Pre-populate half the keys so Erase and Lookup have something to hit.
    for (int i = 0; i < kKeyCount / 2; ++i)
    {
        TestPayload p{"prepop_" + std::to_string(i)};
        ASSERT_TRUE(m_cache->Insert(MakeKey("mixed_k" + std::to_string(i)),
                                     &p, &m_helper, true).ok());
    }

    std::vector<std::thread> threads;
    threads.reserve(kKeyCount * kThreadsPerOp * 2 + kKeyCount);

    // Insert threads.
    for (int i = 0; i < kKeyCount; ++i)
    {
        for (int t = 0; t < kThreadsPerOp; ++t)
        {
            threads.emplace_back([&, i] {
                TestPayload p{"insert_" + std::to_string(i)};
                m_cache->Insert(MakeKey("mixed_k" + std::to_string(i)),
                                &p, &m_helper, true);
            });
        }
    }

    // Erase threads.
    for (int i = 0; i < kKeyCount; ++i)
    {
        threads.emplace_back([&, i] {
            m_cache->Erase(MakeKey("mixed_k" + std::to_string(i)));
        });
    }

    // Lookup threads.
    for (int i = 0; i < kKeyCount; ++i)
    {
        for (int t = 0; t < kThreadsPerOp; ++t)
        {
            threads.emplace_back([&, i] {
                bool kept = false;
                auto handle = m_cache->Lookup(
                    MakeKey("mixed_k" + std::to_string(i)),
                    &m_helper, nullptr, true, false, nullptr, kept);
                if (handle)
                {
                    auto* result = static_cast<TestPayload*>(handle->Value());
                    delete result;
                }
            });
        }
    }

    for (auto& t : threads)
        t.join();

    // The cache must remain internally consistent: usage <= capacity.
    size_t usage = 0, cap = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_LE(usage, cap)
        << "Internal state must remain consistent after concurrent mixed operations";

    // The cache must remain fully operational after the concurrent stress.
    TestPayload postStress{"post-stress data"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("post_stress_key"), &postStress, &m_helper, true).ok());
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("post_stress_key"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}
