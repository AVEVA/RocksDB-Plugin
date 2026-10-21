// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/RegionBasedSecondaryCache.hpp"

#include "ResultHandle.hpp"
#include "SecondaryCache/RegionManager.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>

#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core {
using namespace boost::log::trivial;
namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;

namespace {
using Logger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;

[[nodiscard]] uint64_t HashKey(const rocksdb::Slice& key) noexcept {
    constexpr uint64_t kOffset = 14695981039346656037ull;
    constexpr uint64_t kPrime = 1099511628211ull;
    uint64_t hash = kOffset;
    for (size_t index = 0; index < key.size(); ++index) {
        hash ^= static_cast<uint8_t>(key.data()[index]);
        hash *= kPrime;
    }
    return hash;
}

void RecordHitStats(rocksdb::Statistics* stats, const rocksdb::CacheEntryRole role) noexcept {
    if (stats == nullptr) {
        return;
    }

    stats->recordTick(rocksdb::SECONDARY_CACHE_HITS);
    switch (role) {
    case rocksdb::CacheEntryRole::kFilterBlock:
        stats->recordTick(rocksdb::SECONDARY_CACHE_FILTER_HITS);
        break;
    case rocksdb::CacheEntryRole::kIndexBlock:
        stats->recordTick(rocksdb::SECONDARY_CACHE_INDEX_HITS);
        break;
    case rocksdb::CacheEntryRole::kDataBlock:
        stats->recordTick(rocksdb::SECONDARY_CACHE_DATA_HITS);
        break;
    default:
        break;
    }
}

} // namespace

class RegionBasedSecondaryCache::Impl {
    RegionBasedSecondaryCacheOptions m_options;
    std::shared_ptr<Logger> m_logger;
    mutable Secondary::CacheStats m_stats;
    Secondary::MemoryBudget m_transient;
    Secondary::RegionManager m_regions;

  public:
    Impl(std::filesystem::path cacheDir, const RegionBasedSecondaryCacheOptions& options,
         std::shared_ptr<Logger> logger)
        : m_options(options), m_logger(std::move(logger)), m_transient{options.transientBudget},
          m_regions(
              [cacheDir = std::move(cacheDir)](Secondary::MemoryBudget& budget) {
                  return std::make_unique<Secondary::FileBlockDevice>(cacheDir, budget);
              },
              options, m_stats) {}

    const char* Name() const noexcept { return "RegionBasedSecondaryCache"; }

    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, const bool /*forceInsert*/) noexcept {
        ++m_stats.inserts;
        try {
            if (!helper || !helper->IsSecondaryCacheCompatible()) {
                return rocksdb::Status::OK();
            }
            const size_t dataSize = helper->size_cb(obj);
            if (!CanEncode(key.size(), dataSize)) {
                return rocksdb::Status::OK();
            }
            Secondary::RecordBytes payload{Secondary::BudgetAllocator<std::byte>(m_transient)};
            payload.resize(dataSize);
            if (dataSize != 0) {
                if (auto s = helper->saveto_cb(obj, 0, dataSize, reinterpret_cast<char*>(payload.data())); !s.ok()) {
                    ReportFailure("serialization callback failed");
                    return s;
                }
            }
            return InsertEncoded(key, payload, rocksdb::kNoCompression, rocksdb::CacheTier::kVolatileTier);
        } catch (const Secondary::BudgetExceeded&) {
            ++m_stats.droppedPressure;
            return rocksdb::Status::OK();
        } catch (...) {
            return ExceptionStatus();
        }
    }

    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved,
                                const rocksdb::CompressionType type, const rocksdb::CacheTier source) noexcept {
        ++m_stats.inserts;
        try {
            if (!CanEncode(key.size(), saved.size())) {
                return rocksdb::Status::OK();
            }
            return InsertEncoded(
                key, std::span<const std::byte>(reinterpret_cast<const std::byte*>(saved.data()), saved.size()), type,
                source);
        } catch (const Secondary::BudgetExceeded&) {
            ++m_stats.droppedPressure;
            return rocksdb::Status::OK();
        } catch (...) {
            return ExceptionStatus();
        }
    }

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* createContext, bool /*wait*/, const bool adviseErase,
           rocksdb::Statistics* stats, bool& keptInSecondaryCache) noexcept {
        keptInSecondaryCache = false;
        ++m_stats.lookups;
        try {
            if (!helper || !helper->IsSecondaryCacheCompatible()) {
                return nullptr;
            }

            const uint64_t keyHash = HashKey(key);
            Secondary::RegionManager::Identity identity;
            Secondary::RecordBytes record{Secondary::BudgetAllocator<std::byte>(m_transient)};
            const auto readStatus = m_regions.Read(keyHash, record, identity);
            if (readStatus != Secondary::RegionManager::ReadStatus::kOk) {
                if (readStatus == Secondary::RegionManager::ReadStatus::kIoError) {
                    ReportFailure("record read failed");
                }
                return nullptr;
            }

            Secondary::DecodedRecordView decoded{};
            const auto decodeResult = Secondary::DecodeRecord(
                std::span<const std::byte>(record.data(), record.size()),
                std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()), decoded);
            if (decodeResult != Secondary::DecodeResult::kOk) {
                if (decodeResult == Secondary::DecodeResult::kKeyMismatch) {
                    ++m_stats.keyMismatches;
                } else {
                    ++m_stats.corruptions;
                    m_regions.Remove(keyHash, identity);
                    ReportFailure("record integrity check failed");
                }
                return nullptr;
            }

            rocksdb::Cache::ObjectPtr outObj = nullptr;
            size_t outCharge = 0;
            const rocksdb::Slice payloadSlice(reinterpret_cast<const char*>(decoded.payload.data()),
                                              decoded.payload.size());
            auto createStatus = helper->create_cb(payloadSlice, decoded.compressionType, decoded.sourceTier,
                                                  createContext, nullptr, &outObj, &outCharge);
            if (!createStatus.ok() || outObj == nullptr) {
                // RocksDB's callback owns cleanup on failure; ignore its out_obj.
                ReportFailure("creation callback failed");
                return nullptr;
            }
            auto cleanup = boost::scope::make_scope_exit([&] noexcept {
                if (helper->del_cb != nullptr) {
                    helper->del_cb(outObj, nullptr);
                }
            });
            auto result = std::make_unique<ResultHandle>(outObj, outCharge);
            if (adviseErase) {
                m_regions.Remove(keyHash, identity);
            }
            keptInSecondaryCache = !adviseErase && m_regions.Contains(keyHash, identity);
            ++m_stats.hits;
            RecordHitStats(stats, helper->role);
            cleanup.set_active(false);
            return result;
        } catch (const Secondary::BudgetExceeded&) {
            ++m_stats.droppedPressure;
            return nullptr;
        } catch (...) {
            keptInSecondaryCache = false;
            ExceptionStatus().PermitUncheckedError();
            return nullptr;
        }
    }

    void Erase(const rocksdb::Slice& key) noexcept {
        try {
            m_regions.Erase(HashKey(key));
        } catch (...) {
            ExceptionStatus().PermitUncheckedError();
        }
    }

    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> /*handles*/) noexcept {}

    rocksdb::Status SetCapacity(const size_t capacity) noexcept {
        try {
            return m_regions.SetCapacity(capacity);
        } catch (...) {
            return ExceptionStatus();
        }
    }

    rocksdb::Status GetCapacity(size_t& capacity) noexcept {
        try {
            capacity = m_regions.CapacityBytes();
            return rocksdb::Status::OK();
        } catch (...) {
            return ExceptionStatus();
        }
    }

    rocksdb::Status Deflate(size_t) noexcept { return rocksdb::Status::NotSupported(); }
    rocksdb::Status Inflate(size_t) noexcept { return rocksdb::Status::NotSupported(); }

    rocksdb::Status GetUsage(size_t& usage) const noexcept {
        try {
            usage = m_regions.UsageBytes();
            return rocksdb::Status::OK();
        } catch (...) {
            return ExceptionStatus();
        }
    }

    rocksdb::Status GetStats(RegionBasedSecondaryCacheStats& stats) const noexcept {
        try {
            stats = m_regions.Snapshot();
            stats.transientBytes = m_transient.used.load(std::memory_order_relaxed);
            return rocksdb::Status::OK();
        } catch (...) {
            return ExceptionStatus();
        }
    }

  private:
    // Translate without constructing another allocating status message during OOM.
    rocksdb::Status ExceptionStatus() const noexcept {
        ReportFailure("operation threw; cache operation failed");
        try {
            throw;
        } catch (const std::bad_alloc&) {
            return rocksdb::Status::MemoryLimit();
        } catch (const std::invalid_argument&) {
            return rocksdb::Status::InvalidArgument();
        } catch (const Secondary::DeviceError&) {
            ++m_stats.ioErrors;
            return rocksdb::Status::IOError();
        } catch (const std::filesystem::filesystem_error&) {
            ++m_stats.ioErrors;
            return rocksdb::Status::IOError();
        } catch (...) {
            return rocksdb::Status::Aborted();
        }
    }

    void ReportFailure(const char* reason) const noexcept {
        // First failure and powers of two: bounded log traffic, no status formatting on OOM.
        const uint64_t count = m_stats.errors.fetch_add(1, std::memory_order_relaxed) + 1;
        if ((count & (count - 1)) == 0) {
            try {
                BOOST_LOG_SEV(*m_logger, error) << Name() << ": " << reason << " (failure " << count << ")";
            } catch (...) {
                // Logging must not escape RocksDB's noexcept boundary; the counter remains observable.
            }
        }
    }

    bool CanEncode(const size_t keySize, const size_t payloadSize) {
        const auto size = Secondary::RecordSize(keySize, payloadSize);
        if (size == 0 || size > m_options.maxEntrySize) {
            ++m_stats.droppedTooLarge;
            return false;
        }
        const auto capacity = m_regions.CapacityBytes();
        if (capacity == 0) {
            ++m_stats.droppedDisabled;
            return false;
        }
        if (size > capacity) {
            ++m_stats.droppedTooLarge;
            return false;
        }
        return true;
    }

    rocksdb::Status InsertEncoded(const rocksdb::Slice& key, const std::span<const std::byte> payload,
                                  const rocksdb::CompressionType type, const rocksdb::CacheTier source) {
        const uint64_t keyHash = HashKey(key);
        const size_t recordSize = Secondary::RecordSize(key.size(), payload.size());
        Secondary::RecordBytes record{Secondary::BudgetAllocator<std::byte>(m_transient)};
        record.resize(recordSize);
        if (!Secondary::EncodeRecord(
                record, std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()), payload,
                type, source, keyHash)) {
            return rocksdb::Status::InvalidArgument("secondary cache record cannot be encoded");
        }
        auto status = m_regions.Append(keyHash, record, payload.size());
        if (!status.ok()) {
            ReportFailure("record append failed");
        }
        return status;
    }
};

RegionBasedSecondaryCache::RegionBasedSecondaryCache(std::filesystem::path cacheDir,
                                                     RegionBasedSecondaryCacheOptions options,
                                                     std::shared_ptr<Logger> logger) {
    if (!logger) {
        throw std::invalid_argument("RegionBasedSecondaryCache: logger cannot be null");
    }
    m_impl = std::make_unique<Impl>(std::move(cacheDir), options, std::move(logger));
}

RegionBasedSecondaryCache::~RegionBasedSecondaryCache() = default;

const char* RegionBasedSecondaryCache::Name() const noexcept { return m_impl->Name(); }

rocksdb::Status RegionBasedSecondaryCache::Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                                                  const rocksdb::Cache::CacheItemHelper* helper,
                                                  const bool forceInsert) noexcept {
    return m_impl->Insert(key, obj, helper, forceInsert);
}

rocksdb::Status RegionBasedSecondaryCache::InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved,
                                                       const rocksdb::CompressionType type,
                                                       const rocksdb::CacheTier source) noexcept {
    return m_impl->InsertSaved(key, saved, type, source);
}

std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
RegionBasedSecondaryCache::Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* cacheItemHelper,
                                  rocksdb::Cache::CreateContext* createContext, const bool wait, const bool adviseErase,
                                  rocksdb::Statistics* stats, bool& keptInSecondaryCache) noexcept {
    return m_impl->Lookup(key, cacheItemHelper, createContext, wait, adviseErase, stats, keptInSecondaryCache);
}

void RegionBasedSecondaryCache::Erase(const rocksdb::Slice& key) noexcept { m_impl->Erase(key); }

void RegionBasedSecondaryCache::WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept {
    m_impl->WaitAll(std::move(handles));
}

rocksdb::Status RegionBasedSecondaryCache::SetCapacity(const size_t capacity) noexcept {
    return m_impl->SetCapacity(capacity);
}

rocksdb::Status RegionBasedSecondaryCache::GetCapacity(size_t& capacity) noexcept {
    return m_impl->GetCapacity(capacity);
}

rocksdb::Status RegionBasedSecondaryCache::GetUsage(size_t& usage) const noexcept { return m_impl->GetUsage(usage); }

rocksdb::Status RegionBasedSecondaryCache::GetStats(RegionBasedSecondaryCacheStats& stats) const noexcept {
    return m_impl->GetStats(stats);
}

rocksdb::Status RegionBasedSecondaryCache::Deflate(const size_t decrease) noexcept { return m_impl->Deflate(decrease); }

rocksdb::Status RegionBasedSecondaryCache::Inflate(const size_t increase) noexcept { return m_impl->Inflate(increase); }
} // namespace AVEVA::RocksDB::Plugin::Core