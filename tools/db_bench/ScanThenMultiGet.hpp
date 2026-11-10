// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA
#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/tool_hooks.h>
#include <rocksdb/utilities/stackable_db.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace AVEVA::RocksDB::Plugin::Bench {

struct ScanThenMultiGetCounters {
    std::atomic<uint64_t> queries{0};
    std::atomic<uint64_t> scannedRecords{0};
    std::atomic<uint64_t> scannedBytes{0};
    std::atomic<uint64_t> fetchedKeys{0};
    std::atomic<uint64_t> foundKeys{0};
};

// The existing db_bench dataset has no index-to-node mapping. Use the first
// requested key as the scan start, then fetch the original random batch.
// This models call sequencing, not GraphDB's schema or record dependencies.
class ScanThenMultiGetDB final : public rocksdb::StackableDB {
  public:
    ScanThenMultiGetDB(std::unique_ptr<rocksdb::DB> db, size_t scanLength, ScanThenMultiGetCounters& counters)
        : StackableDB(std::move(db)), m_scanLength(scanLength), m_counters(counters) {}

    using StackableDB::MultiGet;

    void MultiGet(const rocksdb::ReadOptions& options, size_t numKeys, rocksdb::ColumnFamilyHandle** families,
                  const rocksdb::Slice* keys, rocksdb::PinnableSlice* values, std::string* timestamps,
                  rocksdb::Status* statuses, bool sortedInput = false) override {
        if (numKeys == 0) {
            return;
        }
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

  private:
    size_t m_scanLength;
    ScanThenMultiGetCounters& m_counters;
};

// Only the normal single-column-family Open used by this experiment is
// wrapped. The runner verifies counters so unsupported paths fail visibly.
class ScanThenMultiGetHooks final : public rocksdb::DefaultHooks {
  public:
    explicit ScanThenMultiGetHooks(size_t scanLength) : m_scanLength(scanLength) {}

    using DefaultHooks::Open;

    rocksdb::Status Open(const rocksdb::Options& options, const std::string& name,
                         std::unique_ptr<rocksdb::DB>* db) override {
        auto status = DefaultHooks::Open(options, name, db);
        if (status.ok()) {
            *db = std::make_unique<ScanThenMultiGetDB>(std::move(*db), m_scanLength, counters);
        }
        return status;
    }

    ScanThenMultiGetCounters counters;

  private:
    size_t m_scanLength;
};

} // namespace AVEVA::RocksDB::Plugin::Bench
