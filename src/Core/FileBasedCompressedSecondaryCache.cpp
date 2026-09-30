// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"

#include "LruFileIndex.hpp"
#include "ResultHandle.hpp"
#include "SecondaryCache/AdmissionPolicy.hpp"
#include "SecondaryCache/BlockDevice.hpp"
#include "SecondaryCache/CacheStats.hpp"
#include "SecondaryCache/RecordFormat.hpp"
#include "SecondaryCache/RegionManager.hpp"
#include "SecondaryCache/ShardedIndex.hpp"

#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>

#include <boost/algorithm/hex.hpp>
#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <boost/container/small_vector.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core {
using namespace boost::log::trivial;
namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;

namespace {
using Logger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;
constexpr size_t kReservedRegionFooterBytes = 1ULL << 20;

struct FileUtil {
    static void CommitEviction(Filesystem& fs, const std::pair<std::string, std::string>& p) noexcept {
        if (p.first.empty()) {
            return;
        }
        if (fs.RenameFile(p.first, p.second)) {
            fs.DeleteFile(p.second);
        }
    }

    static void CommitEvictions(Filesystem& fs,
                                const std::vector<std::pair<std::string, std::string>>& pairs) noexcept {
        for (const auto& p : pairs) {
            CommitEviction(fs, p);
        }
    }
};

struct StatusUtil {
    static rocksdb::Status CurrentExceptionToStatus() noexcept {
        try {
            throw;
        } catch (const std::bad_alloc&) {
            return rocksdb::Status::MemoryLimit("out of memory");
        } catch (const std::exception& e) {
            return rocksdb::Status::Aborted(e.what());
        } catch (...) {
            return rocksdb::Status::Aborted("unknown exception in secondary cache");
        }
    }
};

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

class CacheEngine {
  public:
    virtual ~CacheEngine() = default;
    virtual const char* Name() const noexcept = 0;
    virtual rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                                   const rocksdb::Cache::CacheItemHelper* helper, bool forceInsert) noexcept = 0;
    virtual rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved,
                                        rocksdb::CompressionType type, rocksdb::CacheTier source) noexcept = 0;
    virtual std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* createContext, bool wait, bool adviseErase, rocksdb::Statistics* stats,
           bool& keptInSecondaryCache) noexcept = 0;
    virtual bool SupportForceErase() const noexcept = 0;
    virtual void Erase(const rocksdb::Slice& key) noexcept = 0;
    virtual void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept = 0;
    virtual rocksdb::Status SetCapacity(size_t capacity) noexcept = 0;
    virtual rocksdb::Status GetCapacity(size_t& capacity) noexcept = 0;
    virtual rocksdb::Status Deflate(size_t decrease) noexcept = 0;
    virtual rocksdb::Status Inflate(size_t increase) noexcept = 0;
    virtual rocksdb::Status GetUsage(size_t& usage) const noexcept = 0;
};

class LegacyEngine final : public CacheEngine {
    std::filesystem::path m_cacheDir;
    std::shared_ptr<Filesystem> m_fs;
    LruFileIndex m_lruIndex;
    std::shared_ptr<Logger> m_logger;

    struct ReadEntryResult {
        enum class Status { Miss, Corrupt, Ok };
        Status status;
        std::string contents;
        std::optional<LruFileIndex::ScopedPin> pin;
    };

  public:
    LegacyEngine(std::filesystem::path cacheDir, std::shared_ptr<Filesystem> fs, const size_t capacity,
                 std::shared_ptr<Logger> logger)
        : m_cacheDir(std::move(cacheDir)), m_fs(std::move(fs)), m_lruIndex(m_cacheDir.string(), capacity),
          m_logger(std::move(logger)) {
        if (!m_logger) {
            throw std::invalid_argument("FileBasedCompressedSecondaryCache: logger cannot be null");
        }
        m_fs->DeleteDir(m_cacheDir);
        m_fs->CreateDir(m_cacheDir);
        BOOST_LOG_SEV(*m_logger, info) << "FileBasedCompressedSecondaryCache: initialized dir='" << m_cacheDir.string()
                                       << "', capacity=" << capacity << " bytes";
    }

    const char* Name() const noexcept override { return "FileBasedCompressedSecondaryCache"; }

    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, const bool forceInsert) noexcept override {
        try {
            if (!helper || !helper->IsSecondaryCacheCompatible()) {
                return rocksdb::Status::OK();
            }
            if (IsKeyTooLong(key)) {
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
            }
            const size_t dataSize = helper->size_cb(obj);
            if (dataSize == 0) {
                return rocksdb::Status::OK();
            }

            boost::container::small_vector<char, 4096> buf(dataSize);
            auto s = helper->saveto_cb(obj, 0, dataSize, buf.data());
            if (!s.ok()) {
                return s;
            }

            return WriteEntry(key, rocksdb::CompressionType::kNoCompression, buf.data(), dataSize, forceInsert);
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved,
                                const rocksdb::CompressionType type,
                                rocksdb::CacheTier /*source*/) noexcept override {
        try {
            if (saved.size() == 0) {
                return rocksdb::Status::OK();
            }
            if (IsKeyTooLong(key)) {
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
            }
            return WriteEntry(key, type, saved.data(), saved.size());
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* cacheItemHelper,
           rocksdb::Cache::CreateContext* createContext, bool /*wait*/, bool adviseErase, rocksdb::Statistics* stats,
           bool& keptInSecondaryCache) noexcept override {
        try {
            keptInSecondaryCache = false;
            if (!cacheItemHelper || !cacheItemHelper->IsSecondaryCacheCompatible()) {
                return nullptr;
            }
            if (IsKeyTooLong(key)) {
                return nullptr;
            }

            const auto filename = KeyToFilename(key);
            const std::string pathStr = m_lruIndex.MakePath(filename);
            auto readResult = ReadEntryForLookup(filename, pathStr);
            if (readResult.status == ReadEntryResult::Status::Miss) {
                return nullptr;
            } else if (readResult.status == ReadEntryResult::Status::Corrupt) {
                CleanupCorruptEntry(filename);
                return nullptr;
            }

            std::pair<std::string, std::string> deferredEviction;
            if (adviseErase) {
                deferredEviction = m_lruIndex.Remove(filename);
                if (deferredEviction.first.empty()) {
                    return nullptr;
                }
            } else if (!m_lruIndex.Touch(filename)) {
                return nullptr;
            }

            auto evictionCleanup = boost::scope::make_scope_exit([&] noexcept {
                if (!deferredEviction.first.empty()) {
                    FileUtil::CommitEviction(*m_fs, deferredEviction);
                }
            });

            bool entryIsValid = false;
            auto corruptionCleanup = boost::scope::make_scope_exit([&] noexcept {
                if (!entryIsValid && !adviseErase) {
                    CleanupCorruptEntry(filename);
                }
            });

            const auto& contents = readResult.contents;
            if (contents.empty()) {
                return nullptr;
            }

            const auto compressionType = static_cast<rocksdb::CompressionType>(static_cast<uint8_t>(contents[0]));
            const rocksdb::Slice dataSlice{contents.data() + 1, contents.size() - 1};

            rocksdb::Cache::ObjectPtr outObj = nullptr;
            size_t outCharge = 0;
            auto objCleanup = boost::scope::make_scope_exit([&] noexcept {
                if (outObj) {
                    cacheItemHelper->del_cb(outObj, nullptr);
                }
            });

            auto s = cacheItemHelper->create_cb(dataSlice, compressionType, rocksdb::CacheTier::kNonVolatileBlockTier,
                                                createContext, nullptr, &outObj, &outCharge);
            if (!s.ok() || outObj == nullptr) {
                return nullptr;
            }

            RecordHitStats(stats, cacheItemHelper->role);
            entryIsValid = true;
            keptInSecondaryCache = !adviseErase;
            auto result = std::make_unique<ResultHandle>(outObj, outCharge);
            objCleanup.set_active(false);
            return result;
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
            return nullptr;
        }
    }

    bool SupportForceErase() const noexcept override { return true; }

    void Erase(const rocksdb::Slice& key) noexcept override {
        try {
            if (IsKeyTooLong(key)) {
                return;
            }
            const auto filename = KeyToFilename(key);
            FileUtil::CommitEviction(*m_fs, m_lruIndex.Remove(filename));
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
        }
    }

    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> /*handles*/) noexcept override {}

    rocksdb::Status SetCapacity(const size_t capacity) noexcept override {
        try {
            FileUtil::CommitEvictions(*m_fs, m_lruIndex.SetCapacity(capacity));
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status GetCapacity(size_t& capacity) noexcept override {
        try {
            capacity = m_lruIndex.GetCapacity();
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status Deflate(const size_t decrease) noexcept override {
        try {
            FileUtil::CommitEvictions(*m_fs, m_lruIndex.Deflate(decrease));
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status Inflate(const size_t increase) noexcept override {
        try {
            m_lruIndex.Inflate(increase);
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status GetUsage(size_t& usage) const noexcept override {
        try {
            usage = m_lruIndex.GetUsage();
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

  private:
    [[nodiscard]] static boost::static_string<LruFileIndex::kMaxFilenameLen>
    KeyToFilename(const rocksdb::Slice& key) noexcept {
        if (IsKeyTooLong(key)) {
            return {};
        }
        boost::static_string<LruFileIndex::kMaxFilenameLen> result;
        boost::algorithm::hex_lower(key.data(), key.data() + key.size(), std::back_inserter(result));
        return result;
    }

    [[nodiscard]] static bool IsKeyTooLong(const rocksdb::Slice& key) noexcept {
        return key.size() > LruFileIndex::kMaxFilenameLen / 2;
    }

    rocksdb::Status WriteEntry(const rocksdb::Slice& key, const rocksdb::CompressionType type, const char* data,
                               const size_t dataSize, const bool forceInsert = true) noexcept {
        try {
            const auto filename = KeyToFilename(key);
            const size_t storedSize = dataSize + FileBasedCompressedSecondaryCache::kFileHeaderSize;
            auto reserved = m_lruIndex.ReserveCapacity(filename, storedSize, forceInsert);
            if (!reserved) {
                return rocksdb::Status::OK();
            }
            FileUtil::CommitEvictions(*m_fs, *reserved);
            if (auto s = WriteToDisk(filename, type, data, dataSize, storedSize); !s.ok()) {
                return s;
            }
            const auto evictList = m_lruIndex.RegisterEntry(filename, storedSize);
            FileUtil::CommitEvictions(*m_fs, evictList);
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    [[nodiscard]] rocksdb::Status WriteToDisk(const std::string_view filename, const rocksdb::CompressionType type,
                                              const char* data, const size_t dataSize, const size_t storedSize) {
        const std::string pathStr = m_lruIndex.MakePath(filename);
        boost::container::small_vector<char, 1 + 4096> writeBuf(storedSize);
        writeBuf[0] = static_cast<char>(static_cast<uint8_t>(type));
        std::memcpy(writeBuf.data() + 1, data, dataSize);
        if (!m_fs->WriteFileAtomic(pathStr, writeBuf.data(), writeBuf.size())) {
            return rocksdb::Status::IOError("Failed to write cache entry file", pathStr);
        }
        return rocksdb::Status::OK();
    }

    [[nodiscard]] ReadEntryResult ReadEntryForLookup(const std::string_view filename, const std::string& pathStr) {
        auto pin = m_lruIndex.TryPin(filename);
        if (!pin) {
            return {ReadEntryResult::Status::Miss};
        }
        auto contents = m_fs->ReadFileContents(pathStr);
        if (!contents) {
            return {ReadEntryResult::Status::Corrupt};
        }
        return {ReadEntryResult::Status::Ok, std::move(*contents), std::move(pin)};
    }

    void CleanupCorruptEntry(std::string_view filename) noexcept {
        try {
            FileUtil::CommitEviction(*m_fs, m_lruIndex.Remove(filename));
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
        }
    }
};

class RegionEngine final : public CacheEngine {
    std::filesystem::path m_cacheDir;
    FileBasedSecondaryCacheOptions m_options;
    std::shared_ptr<Logger> m_logger;
    Secondary::CacheStats m_stats;
    Secondary::ShardedIndex m_index;
    Secondary::AdmissionPolicy m_admission;
    Secondary::RegionManager m_regions;
    size_t m_maxDramBudget;
    size_t m_currentDramBudget;

  public:
    RegionEngine(std::filesystem::path cacheDir, const FileBasedSecondaryCacheOptions& options,
                 std::shared_ptr<Logger> logger)
        : m_cacheDir(std::move(cacheDir)), m_options(options), m_logger(std::move(logger)),
          m_index(options.indexShards),
          m_admission(options.admissionPolicy == FileBasedSecondaryCacheAdmissionPolicy::kSecondChance
                          ? Secondary::AdmissionPolicyKind::kSecondChance
                          : Secondary::AdmissionPolicyKind::kAdmitAll,
                      options.indexShards),
          m_regions(std::make_unique<Secondary::FileBlockDevice>(
                        m_cacheDir, ComputeRegionCount(options.capacity, options.regionSize), options.regionSize),
                    m_index, m_stats,
                    Secondary::RegionManagerOptions{options.capacity, options.regionSize, options.flushBlockSize,
                                                    options.maxEntrySize}),
          m_maxDramBudget(options.flushBlockSize),
          m_currentDramBudget(options.flushBlockSize) {
        if (!m_logger) {
            throw std::invalid_argument("FileBasedCompressedSecondaryCache: logger cannot be null");
        }
        const size_t usableRegionBytes = options.regionSize - kReservedRegionFooterBytes;
        if (options.regionSize <= kReservedRegionFooterBytes || options.maxEntrySize > usableRegionBytes) {
            throw std::invalid_argument("FileBasedCompressedSecondaryCache: invalid region-engine sizing");
        }
    }

    const char* Name() const noexcept override { return "FileBasedCompressedSecondaryCache"; }

    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, const bool forceInsert) noexcept override {
        try {
            m_stats.inserts.value.fetch_add(1, std::memory_order_relaxed);
            if (!helper || !helper->IsSecondaryCacheCompatible()) {
                return rocksdb::Status::OK();
            }

            const size_t dataSize = helper->size_cb(obj);
            if (dataSize == 0) {
                return rocksdb::Status::OK();
            }

            std::vector<std::byte> payload(dataSize);
            if (auto s = helper->saveto_cb(obj, 0, dataSize, reinterpret_cast<char*>(payload.data())); !s.ok()) {
                return s;
            }

            return InsertEncoded(key, std::span<const std::byte>(payload.data(), payload.size()),
                                 rocksdb::CompressionType::kNoCompression, rocksdb::CacheTier::kVolatileTier,
                                 forceInsert);
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, const rocksdb::CompressionType type,
                                const rocksdb::CacheTier source) noexcept override {
        try {
            m_stats.inserts.value.fetch_add(1, std::memory_order_relaxed);
            if (saved.size() == 0) {
                return rocksdb::Status::OK();
            }
            return InsertEncoded(key,
                                 std::span<const std::byte>(reinterpret_cast<const std::byte*>(saved.data()), saved.size()),
                                 type, source, true);
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* createContext, bool /*wait*/, const bool adviseErase,
           rocksdb::Statistics* stats, bool& keptInSecondaryCache) noexcept override {
        try {
            m_stats.lookups.value.fetch_add(1, std::memory_order_relaxed);
            keptInSecondaryCache = false;
            if (!helper || !helper->IsSecondaryCacheCompatible()) {
                return nullptr;
            }

            const uint64_t keyHash = HashKey(key);
            Secondary::Location location{};
            if (!m_index.FindAndTouch(keyHash, location)) {
                m_stats.missesIndex.value.fetch_add(1, std::memory_order_relaxed);
                return nullptr;
            }

            std::vector<std::byte> record;
            const auto readStatus = m_regions.Read(location, record);
            if (readStatus == Secondary::RegionManager::ReadStatus::kRegionReclaimed) {
                m_stats.missesRegionReclaimed.value.fetch_add(1, std::memory_order_relaxed);
                return nullptr;
            }
            if (readStatus != Secondary::RegionManager::ReadStatus::kOk) {
                return nullptr;
            }

            Secondary::DecodedRecordView decoded{};
            const auto decodeResult = Secondary::DecodeRecord(
                std::span<const std::byte>(record.data(), record.size()),
                std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()), decoded);
            if (decodeResult != Secondary::DecodeResult::kOk) {
                if (decodeResult == Secondary::DecodeResult::kKeyMismatch) {
                    m_stats.missesKeyMismatch.value.fetch_add(1, std::memory_order_relaxed);
                } else {
                    m_stats.missesCrc.value.fetch_add(1, std::memory_order_relaxed);
                    if (const auto removed = m_index.Remove(keyHash)) {
                        m_regions.AdjustLiveBytes(-static_cast<int64_t>(Secondary::LocationLengthBytes(*removed)));
                    }
                }
                return nullptr;
            }

            rocksdb::Cache::ObjectPtr outObj = nullptr;
            size_t outCharge = 0;
            auto cleanup = boost::scope::make_scope_exit([&] noexcept {
                if (outObj != nullptr) {
                    helper->del_cb(outObj, nullptr);
                }
            });

            const rocksdb::Slice payloadSlice(reinterpret_cast<const char*>(decoded.payload.data()), decoded.payload.size());
            auto createStatus =
                helper->create_cb(payloadSlice, decoded.compressionType, decoded.sourceTier, createContext, nullptr,
                                  &outObj, &outCharge);
            if (!createStatus.ok() || outObj == nullptr) {
                return nullptr;
            }

            if (adviseErase) {
                if (const auto removed = m_index.Remove(keyHash)) {
                    m_regions.AdjustLiveBytes(-static_cast<int64_t>(Secondary::LocationLengthBytes(*removed)));
                }
            } else {
                keptInSecondaryCache = true;
            }

            m_stats.hits.value.fetch_add(1, std::memory_order_relaxed);
            RecordHitStats(stats, helper->role);
            cleanup.set_active(false);
            return std::make_unique<ResultHandle>(outObj, outCharge);
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
            return nullptr;
        }
    }

    bool SupportForceErase() const noexcept override { return true; }

    void Erase(const rocksdb::Slice& key) noexcept override {
        try {
            if (const auto removed = m_index.Remove(HashKey(key))) {
                m_regions.AdjustLiveBytes(-static_cast<int64_t>(Secondary::LocationLengthBytes(*removed)));
            }
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
        }
    }

    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> /*handles*/) noexcept override {}

    rocksdb::Status SetCapacity(const size_t capacity) noexcept override {
        try {
            return m_regions.SetCapacity(capacity);
        } catch (...) {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    rocksdb::Status GetCapacity(size_t& capacity) noexcept override {
        capacity = m_regions.CapacityBytes();
        return rocksdb::Status::OK();
    }

    rocksdb::Status Deflate(const size_t decrease) noexcept override {
        m_currentDramBudget = decrease >= m_currentDramBudget ? 0 : m_currentDramBudget - decrease;
        return rocksdb::Status::OK();
    }

    rocksdb::Status Inflate(const size_t increase) noexcept override {
        m_currentDramBudget = std::min(m_maxDramBudget, m_currentDramBudget + increase);
        return rocksdb::Status::OK();
    }

    rocksdb::Status GetUsage(size_t& usage) const noexcept override {
        usage = m_regions.UsageBytes();
        return rocksdb::Status::OK();
    }

  private:
    [[nodiscard]] static uint32_t ComputeRegionCount(const size_t capacity, const size_t regionSize) {
        if (regionSize <= kReservedRegionFooterBytes) {
            throw std::invalid_argument("FileBasedCompressedSecondaryCache: regionSize must exceed 1 MiB footer");
        }
        const size_t usableRegionBytes = regionSize - kReservedRegionFooterBytes;
        return std::max<uint32_t>(1, static_cast<uint32_t>((capacity + usableRegionBytes - 1) / usableRegionBytes));
    }

    rocksdb::Status InsertEncoded(const rocksdb::Slice& key, const std::span<const std::byte> payload,
                                  const rocksdb::CompressionType type, const rocksdb::CacheTier source,
                                  const bool forceInsert) {
        const uint64_t keyHash = HashKey(key);
        if (!m_admission.ShouldAdmit(keyHash, forceInsert)) {
            m_stats.insertsRejectedByPolicy.value.fetch_add(1, std::memory_order_relaxed);
            return rocksdb::Status::OK();
        }

        const size_t recordSize = Secondary::RecordSize(key.size(), payload.size());
        if (recordSize > m_options.maxEntrySize) {
            m_stats.insertsRejectedTooLarge.value.fetch_add(1, std::memory_order_relaxed);
            return rocksdb::Status::OK();
        }

        Secondary::RegionManager::Reservation reservation{};
        if (!m_regions.Reserve(keyHash, static_cast<uint32_t>(recordSize), forceInsert, reservation)) {
            m_stats.insertsDroppedNoBuffer.value.fetch_add(1, std::memory_order_relaxed);
            return rocksdb::Status::OK();
        }

        std::vector<std::byte> record(recordSize, std::byte{0});
        Secondary::EncodeRecord(
            std::span<std::byte>(record.data(), record.size()),
            std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()), payload, type, source,
            keyHash);
        const auto previous = m_index.Peek(keyHash);
        if (auto status = m_regions.Publish(reservation, std::span<const std::byte>(record.data(), record.size()), previous);
            !status.ok()) {
            return status;
        }

        Secondary::Location newLocation{};
        newLocation.regionId = reservation.regionId;
        newLocation.offsetInAlignUnits = static_cast<uint32_t>(reservation.offset / Secondary::kRecordAlign);
        newLocation.lengthInAlignUnits = static_cast<uint32_t>(reservation.length / Secondary::kRecordAlign);
        newLocation.generation = reservation.generation;
        newLocation.hitCount = 0;
        newLocation.flags = static_cast<uint8_t>(Secondary::LocationFlags::kNone);

        const auto replaced = m_index.Upsert(keyHash, newLocation);
        if (replaced) {
            static_cast<void>(*replaced);
        }
        m_admission.Forget(keyHash);
        m_stats.insertsAdmitted.value.fetch_add(1, std::memory_order_relaxed);
        return rocksdb::Status::OK();
    }
};
} // namespace

class FileBasedCompressedSecondaryCache::Impl {
  public:
    explicit Impl(std::unique_ptr<CacheEngine> engine) : m_engine(std::move(engine)) {}

    const char* Name() const noexcept { return m_engine->Name(); }
    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, bool forceInsert) noexcept {
        return m_engine->Insert(key, obj, helper, forceInsert);
    }
    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, rocksdb::CompressionType type,
                                rocksdb::CacheTier source) noexcept {
        return m_engine->InsertSaved(key, saved, type, source);
    }
    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* createContext, bool wait, bool adviseErase, rocksdb::Statistics* stats,
           bool& keptInSecondaryCache) noexcept {
        return m_engine->Lookup(key, helper, createContext, wait, adviseErase, stats, keptInSecondaryCache);
    }
    bool SupportForceErase() const noexcept { return m_engine->SupportForceErase(); }
    void Erase(const rocksdb::Slice& key) noexcept { m_engine->Erase(key); }
    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept { m_engine->WaitAll(std::move(handles)); }
    rocksdb::Status SetCapacity(size_t capacity) noexcept { return m_engine->SetCapacity(capacity); }
    rocksdb::Status GetCapacity(size_t& capacity) noexcept { return m_engine->GetCapacity(capacity); }
    rocksdb::Status Deflate(size_t decrease) noexcept { return m_engine->Deflate(decrease); }
    rocksdb::Status Inflate(size_t increase) noexcept { return m_engine->Inflate(increase); }
    rocksdb::Status GetUsage(size_t& usage) const noexcept { return m_engine->GetUsage(usage); }

  private:
    std::unique_ptr<CacheEngine> m_engine;
};

FileBasedCompressedSecondaryCache::FileBasedCompressedSecondaryCache(
    std::filesystem::path cacheDir, std::shared_ptr<Filesystem> fs, const size_t capacity,
    std::shared_ptr<Logger> logger)
    : m_impl(std::make_unique<Impl>(std::make_unique<LegacyEngine>(std::move(cacheDir), std::move(fs), capacity,
                                                                  std::move(logger)))) {}

FileBasedCompressedSecondaryCache::FileBasedCompressedSecondaryCache(std::filesystem::path cacheDir,
                                                                     FileBasedSecondaryCacheOptions options,
                                                                     std::shared_ptr<Logger> logger) {
    if (options.engine == FileBasedSecondaryCacheEngine::kRegion) {
        m_impl = std::make_unique<Impl>(std::make_unique<RegionEngine>(std::move(cacheDir), options, std::move(logger)));
    } else {
        m_impl = std::make_unique<Impl>(std::make_unique<LegacyEngine>(
            std::move(cacheDir), std::make_shared<LocalFilesystem>(), options.capacity, std::move(logger)));
    }
}

FileBasedCompressedSecondaryCache::~FileBasedCompressedSecondaryCache() = default;

const char* FileBasedCompressedSecondaryCache::Name() const noexcept { return m_impl->Name(); }

rocksdb::Status FileBasedCompressedSecondaryCache::Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                                                          const rocksdb::Cache::CacheItemHelper* helper,
                                                          const bool forceInsert) noexcept {
    return m_impl->Insert(key, obj, helper, forceInsert);
}

rocksdb::Status FileBasedCompressedSecondaryCache::InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved,
                                                               const rocksdb::CompressionType type,
                                                               const rocksdb::CacheTier source) noexcept {
    return m_impl->InsertSaved(key, saved, type, source);
}

std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
FileBasedCompressedSecondaryCache::Lookup(const rocksdb::Slice& key,
                                          const rocksdb::Cache::CacheItemHelper* cacheItemHelper,
                                          rocksdb::Cache::CreateContext* createContext, bool wait, bool adviseErase,
                                          rocksdb::Statistics* stats, bool& keptInSecondaryCache) noexcept {
    return m_impl->Lookup(key, cacheItemHelper, createContext, wait, adviseErase, stats, keptInSecondaryCache);
}

void FileBasedCompressedSecondaryCache::Erase(const rocksdb::Slice& key) noexcept { m_impl->Erase(key); }

void FileBasedCompressedSecondaryCache::WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept {
    m_impl->WaitAll(std::move(handles));
}

rocksdb::Status FileBasedCompressedSecondaryCache::SetCapacity(const size_t capacity) noexcept {
    return m_impl->SetCapacity(capacity);
}

rocksdb::Status FileBasedCompressedSecondaryCache::GetCapacity(size_t& capacity) noexcept {
    return m_impl->GetCapacity(capacity);
}

rocksdb::Status FileBasedCompressedSecondaryCache::GetUsage(size_t& usage) const noexcept {
    return m_impl->GetUsage(usage);
}

rocksdb::Status FileBasedCompressedSecondaryCache::Deflate(const size_t decrease) noexcept {
    return m_impl->Deflate(decrease);
}

rocksdb::Status FileBasedCompressedSecondaryCache::Inflate(const size_t increase) noexcept {
    return m_impl->Inflate(increase);
}
} // namespace AVEVA::RocksDB::Plugin::Core
