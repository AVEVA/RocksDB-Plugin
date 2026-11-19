// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "ScanThenMultiGet.hpp"

#include <gtest/gtest.h>
#include <rocksdb/env.h>

#include <memory>
#include <string>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Bench {
namespace {

class ScanThenMultiGetTests : public testing::Test {
  protected:
    void SetUp() override {
        std::string testDirectory;
        ASSERT_TRUE(rocksdb::Env::Default()->GetTestDirectory(&testDirectory).ok());
        m_path = testDirectory + "/aveva-scan-multiget-" + std::to_string(rocksdb::Env::Default()->NowMicros());
        rocksdb::Options options;
        options.create_if_missing = true;
        ASSERT_TRUE(rocksdb::DB::Open(options, m_path, &m_db).ok());
        for (const auto key : {"a", "b", "c", "d", "e"}) {
            ASSERT_TRUE(m_db->Put({}, key, std::string("value-") + key).ok());
        }
        ASSERT_TRUE(m_db->Flush({}).ok());
    }

    void TearDown() override {
        m_db.reset();
        EXPECT_TRUE(rocksdb::DestroyDB(m_path, {}).ok());
    }

    std::string m_path;
    std::unique_ptr<rocksdb::DB> m_db;
};

TEST_F(ScanThenMultiGetTests, ScansBeforeFetchingOriginalBatch) {
    ScanThenMultiGetCounters counters;
    ScanThenMultiGetDB db(std::move(m_db), 3, counters);
    std::vector<std::string> values;
    auto statuses = db.MultiGet({}, std::vector<rocksdb::Slice>{"b", "e"}, &values);
    ASSERT_EQ(statuses.size(), 2U);
    EXPECT_TRUE(statuses[0].ok());
    EXPECT_TRUE(statuses[1].ok());

    EXPECT_EQ(values[0], "value-b");
    EXPECT_EQ(values[1], "value-e");
    EXPECT_EQ(counters.queries.load(), 1ULL);
    EXPECT_EQ(counters.scannedRecords.load(), 3ULL);
    EXPECT_EQ(counters.scannedBytes.load(), 24ULL); // b, c, d: 1-byte key + 7-byte value each
    EXPECT_EQ(counters.fetchedKeys.load(), 2ULL);
    EXPECT_EQ(counters.foundKeys.load(), 2ULL);
}

TEST_F(ScanThenMultiGetTests, EndOfDatabaseDoesNotInventScannedRecords) {
    ScanThenMultiGetCounters counters;
    ScanThenMultiGetDB db(std::move(m_db), 2000, counters);
    std::vector<std::string> values;
    auto statuses = db.MultiGet({}, std::vector<rocksdb::Slice>{"e", "missing"}, &values);
    ASSERT_EQ(statuses.size(), 2U);
    EXPECT_TRUE(statuses[0].ok());
    EXPECT_TRUE(statuses[1].IsNotFound());
    EXPECT_EQ(counters.scannedRecords.load(), 1ULL);
    EXPECT_EQ(counters.fetchedKeys.load(), 2ULL);
    EXPECT_EQ(counters.foundKeys.load(), 1ULL);
}

TEST_F(ScanThenMultiGetTests, EmptyBatchDoesNotScan) {
    ScanThenMultiGetCounters counters;
    ScanThenMultiGetDB db(std::move(m_db), 2000, counters);
    std::vector<std::string> values;
    EXPECT_TRUE(db.MultiGet({}, std::vector<rocksdb::Slice>{}, &values).empty());
    EXPECT_EQ(counters.queries.load(), 0ULL);
    EXPECT_EQ(counters.scannedRecords.load(), 0ULL);
}

} // namespace
} // namespace AVEVA::RocksDB::Plugin::Bench
