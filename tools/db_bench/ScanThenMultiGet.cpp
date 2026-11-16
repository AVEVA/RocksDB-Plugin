// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "ScanThenMultiGet.hpp"

#include <rocksdb/iterator.h>

namespace AVEVA::RocksDB::Plugin::Bench {

ScanThenMultiGetDB::ScanThenMultiGetDB(std::unique_ptr<rocksdb::DB> db, size_t scanLength,
                                       ScanThenMultiGetCounters& counters)
    : StackableDB(std::move(db)), m_scanLength(scanLength), m_counters(counters) {}

// Scan first, then do the normal MultiGet. Mimics a query that reads a range
// and then looks up individual keys, so we can measure how the scan affects
// the lookups (cache contents, I/O). Releasing the iterator before the lookup
// keeps the scan from holding resources during it. If the scan fails, the
// whole batch fails instead of quietly benchmarking something else.
void ScanThenMultiGetDB::MultiGet(const rocksdb::ReadOptions& options, size_t numKeys,
                                  rocksdb::ColumnFamilyHandle** families, const rocksdb::Slice* keys,
                                  rocksdb::PinnableSlice* values, std::string* timestamps, rocksdb::Status* statuses,
                                  bool sortedInput) {
    if (numKeys == 0) {
        return;
    }
    // count multiget batches
    ++m_counters.queries;
    uint64_t records = 0;
    uint64_t bytes = 0;
    auto iterator = std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(options, families[0]));
    iterator->Seek(keys[0]);
    while (iterator->Valid() && records < m_scanLength) {
        bytes += iterator->key().size() + iterator->value().size();
        ++records;
        if (records < m_scanLength) {
            iterator->Next();
        }
    }
    const auto status = iterator->status();
    iterator.reset();
    m_counters.scannedRecords += records;
    m_counters.scannedBytes += bytes;
    if (!status.ok()) {
        for (size_t i = 0; i < numKeys; ++i) {
            statuses[i] = status;
        }
        return;
    }
    m_counters.fetchedKeys += numKeys;
    db_->MultiGet(options, numKeys, families, keys, values, timestamps, statuses, sortedInput);
    uint64_t found = 0;
    for (size_t i = 0; i < numKeys; ++i) {
        found += statuses[i].ok();
    }
    m_counters.foundKeys += found;
}

ScanThenMultiGetHooks::ScanThenMultiGetHooks(size_t scanLength) : m_scanLength(scanLength) {}

rocksdb::Status ScanThenMultiGetHooks::Open(const rocksdb::Options& options, const std::string& name,
                                            std::unique_ptr<rocksdb::DB>* db) {
    // open db
    auto status = DefaultHooks::Open(options, name, db);
    if (status.ok()) {
        // wrap it so db_bench_tool uses ScanThenMultiGetDB
        *db = std::make_unique<ScanThenMultiGetDB>(std::move(*db), m_scanLength, counters);
    }
    return status;
}

} // namespace AVEVA::RocksDB::Plugin::Bench
