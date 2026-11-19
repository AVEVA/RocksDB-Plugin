// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA
#pragma once

#include <rocksdb/tool_hooks.h>
#include <rocksdb/utilities/stackable_db.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace AVEVA::RocksDB::Plugin::Bench {

struct ScanThenMultiGetCounters {
    std::atomic<uint64_t> queries{0};
    std::atomic<uint64_t> scannedRecords{0};
    std::atomic<uint64_t> scannedBytes{0};
    std::atomic<uint64_t> fetchedKeys{0};
    std::atomic<uint64_t> foundKeys{0};
};

// A DB wrapper that scans before each MultiGet, then does the normal MultiGet.
// The data has no link between scanned records and fetched keys, so this copies
// the order of calls, not real queries.
class ScanThenMultiGetDB final : public rocksdb::StackableDB {
  public:
    ScanThenMultiGetDB(std::unique_ptr<rocksdb::DB> db, size_t scanLength, ScanThenMultiGetCounters& counters);

    using StackableDB::MultiGet;

    void MultiGet(const rocksdb::ReadOptions& options, size_t numKeys, rocksdb::ColumnFamilyHandle** families,
                  const rocksdb::Slice* keys, rocksdb::PinnableSlice* values, std::string* timestamps,
                  rocksdb::Status* statuses, bool sortedInput = false) override;

  private:
    size_t m_scanLength;
    ScanThenMultiGetCounters& m_counters;
};

// Override Open so db_bench_tool uses ScanThenMultiGetDB.
class ScanThenMultiGetHooks final : public rocksdb::DefaultHooks {
  public:
    explicit ScanThenMultiGetHooks(size_t scanLength);

    using DefaultHooks::Open;

    rocksdb::Status Open(const rocksdb::Options& options, const std::string& name,
                         std::unique_ptr<rocksdb::DB>* db) override;

    ScanThenMultiGetCounters counters;

  private:
    size_t m_scanLength;
};

} // namespace AVEVA::RocksDB::Plugin::Bench
