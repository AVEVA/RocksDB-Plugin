// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>

#include <cachelib/allocator/nvmcache/NavyConfig.h>
#include <cachelib/allocator/nvmcache/NavySetup.h>
#include <cachelib/navy/AbstractCache.h>
#include <cachelib/navy/common/Buffer.h>
#include <cachelib/navy/common/Hash.h>
#include <cachelib/navy/common/Types.h>

#include <folly/logging/LogLevel.h>
#include <folly/logging/LoggerDB.h>
#include <folly/synchronization/Baton.h>

#include <boost/log/trivial.hpp>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Core {
namespace {
namespace navy = facebook::cachelib::navy;
using namespace boost::log::trivial;

// Navy's default maximum key size.
constexpr size_t kMaxKeySize = 255;

rocksdb::Status CurrentExceptionToStatus() noexcept {
    try {
        throw;
    } catch (const std::bad_alloc&) {
        return rocksdb::Status::MemoryLimit("out of memory");
    } catch (const std::exception& e) {
        return rocksdb::Status::Aborted(e.what());
    } catch (...) {
        return rocksdb::Status::Aborted("unknown exception in CacheLibSecondaryCache");
    }
}

facebook::cachelib::HashedKey ToHashedKey(const rocksdb::Slice& key) { return navy::makeHK(key.data(), key.size()); }

// Decodes the entry header and reconstructs the object through the helper.
struct CreatedObject {
    rocksdb::Cache::ObjectPtr value = nullptr;
    size_t charge = 0;
};

rocksdb::Status CreateObject(navy::BufferView entry, const rocksdb::Cache::CacheItemHelper* helper,
                             rocksdb::Cache::CreateContext* createContext, CreatedObject& out) {
    if (entry.size() < CacheLibSecondaryCache::kEntryHeaderSize) {
        return rocksdb::Status::Corruption("CacheLibSecondaryCache: entry too small");
    }
    const auto* bytes = entry.data();
    const auto type = static_cast<rocksdb::CompressionType>(bytes[0]);
    const auto source = static_cast<rocksdb::CacheTier>(bytes[1]);
    const rocksdb::Slice payload(reinterpret_cast<const char*>(bytes + CacheLibSecondaryCache::kEntryHeaderSize),
                                 entry.size() - CacheLibSecondaryCache::kEntryHeaderSize);
    return helper->create_cb(payload, type, source, createContext, nullptr, &out.value, &out.charge);
}

// State shared between an asynchronous lookup and the Navy callback. The key
// must stay alive until the callback runs because Navy only keeps a view of it.
struct AsyncLookupState {
    std::string key;
    folly::Baton<> done;
    navy::Status status = navy::Status::NotFound;
    navy::Buffer value;
};

class CacheLibResultHandle final : public rocksdb::SecondaryCacheResultHandle {
  public:
    // Ready handle (synchronous lookup).
    CacheLibResultHandle(rocksdb::Cache::ObjectPtr value, size_t charge)
        : m_ready(true), m_value(value), m_charge(charge) {}

    // Pending handle (asynchronous lookup).
    CacheLibResultHandle(std::shared_ptr<AsyncLookupState> state, const rocksdb::Cache::CacheItemHelper* helper,
                         rocksdb::Cache::CreateContext* createContext, navy::AbstractCache* navy)
        : m_state(std::move(state)), m_helper(helper), m_createContext(createContext), m_navy(navy) {}

    ~CacheLibResultHandle() override = default;
    CacheLibResultHandle(const CacheLibResultHandle&) = delete;
    CacheLibResultHandle& operator=(const CacheLibResultHandle&) = delete;
    CacheLibResultHandle(CacheLibResultHandle&&) = delete;
    CacheLibResultHandle& operator=(CacheLibResultHandle&&) = delete;

    bool IsReady() noexcept override {
        if (!m_ready && m_state->done.ready()) {
            Complete();
        }
        return m_ready;
    }

    void Wait() noexcept override {
        if (!m_ready) {
            m_state->done.wait();
            Complete();
        }
    }

    rocksdb::Cache::ObjectPtr Value() noexcept override { return m_value; }

    size_t Size() noexcept override { return m_charge; }

  private:
    // Runs create_cb on the waiting (RocksDB) thread rather than on a Navy
    // fiber, whose small stack is not sized for block construction.
    void Complete() noexcept {
        m_ready = true;
        if (m_state->status != navy::Status::Ok) {
            m_state.reset();
            return;
        }
        CreatedObject created;
        rocksdb::Status s;
        try {
            s = CreateObject(m_state->value.view(), m_helper, m_createContext, created);
        } catch (...) {
            s = CurrentExceptionToStatus();
        }
        if (s.ok()) {
            m_value = created.value;
            m_charge = created.charge;
        } else {
            RemoveAsync();
        }
        m_state->value = navy::Buffer{};
        m_state.reset();
    }

    void RemoveAsync() noexcept {
        try {
            auto state = m_state;
            const auto hk = navy::makeHK(state->key.data(), state->key.size());
            m_navy->removeAsync(hk, [state](navy::Status, facebook::cachelib::HashedKey) {});
        } catch (...) {
        }
    }

    bool m_ready = false;
    rocksdb::Cache::ObjectPtr m_value = nullptr;
    size_t m_charge = 0;
    std::shared_ptr<AsyncLookupState> m_state;
    const rocksdb::Cache::CacheItemHelper* m_helper = nullptr;
    rocksdb::Cache::CreateContext* m_createContext = nullptr;
    navy::AbstractCache* m_navy = nullptr;
};

folly::LogLevel ParseLogLevel(const std::string& level) {
    try {
        return folly::stringToLogLevel(level);
    } catch (const std::exception&) {
        throw std::invalid_argument("CacheLibSecondaryCache: invalid cachelibLogLevel '" + level + "'");
    }
}

void Validate(const SecondaryCacheOptions& options) {
    const auto& o = options.cachelib;
    if (!options.logger) {
        throw std::invalid_argument("CacheLibSecondaryCache: logger cannot be null");
    }
    if (options.cacheDir.empty()) {
        throw std::invalid_argument("CacheLibSecondaryCache: cacheDir cannot be empty");
    }
    if (o.fileName.empty() || std::filesystem::path(o.fileName).has_parent_path()) {
        throw std::invalid_argument("CacheLibSecondaryCache: fileName must be a plain file name");
    }
    if (o.regionSizeBytes == 0 || o.blockSize == 0 || o.regionSizeBytes % o.blockSize != 0) {
        throw std::invalid_argument("CacheLibSecondaryCache: regionSizeBytes must be a non-zero multiple of blockSize");
    }
    if (o.cleanRegions == 0) {
        throw std::invalid_argument("CacheLibSecondaryCache: cleanRegions must be at least 1");
    }
    // BlockCache needs the clean regions plus in-flight buffers plus room to evict.
    const uint64_t minCapacity = static_cast<uint64_t>(o.regionSizeBytes) * (2ULL * o.cleanRegions + 2);
    if (options.capacity < minCapacity) {
        throw std::invalid_argument("CacheLibSecondaryCache: capacity must be at least " + std::to_string(minCapacity) +
                                    " bytes for the configured regionSizeBytes/cleanRegions");
    }
    if (o.readerThreads == 0 || o.writerThreads == 0) {
        throw std::invalid_argument("CacheLibSecondaryCache: readerThreads and writerThreads must be non-zero");
    }
    if (o.maxParcelMemoryMB == 0) {
        throw std::invalid_argument("CacheLibSecondaryCache: maxParcelMemoryMB must be non-zero");
    }
    (void)ParseLogLevel(o.cachelibLogLevel);
}

navy::NavyConfig MakeNavyConfig(const SecondaryCacheOptions& options, const std::string& filePath) {
    const auto& o = options.cachelib;
    navy::NavyConfig config;
    config.setSimpleFile(filePath, options.capacity, /*truncateFile*/ true);
    config.setBlockSize(o.blockSize);
    config.setMaxParcelMemoryMB(o.maxParcelMemoryMB);
    config.setReaderAndWriterThreads(o.readerThreads, o.writerThreads);
    if (o.admissionWriteRateBytesPerSec) {
        config.enableDynamicRandomAdmPolicy().setAdmWriteRate(*o.admissionWriteRateBytesPerSec);
    }

    auto& blockCache = config.blockCache();
    blockCache.setRegionSize(o.regionSizeBytes);
    blockCache.setCleanRegions(o.cleanRegions);
    blockCache.setDataChecksum(o.dataChecksum);
    if (o.evictionPolicy == CacheLibSecondaryCacheOptions::EvictionPolicy::Fifo) {
        blockCache.enableFifo();
    }
    return config;
}

std::string Describe(const navy::NavyConfig& config) {
    std::ostringstream out;
    for (const auto& [name, value] : config.serialize()) {
        out << "    " << name << ": " << value << "\n";
    }
    return out.str();
}
} // namespace

class CacheLibSecondaryCache::Impl {
  public:
    explicit Impl(const SecondaryCacheOptions& options) : m_logger(options.logger) {
        Validate(options);
        const auto& o = options.cachelib;

        folly::LoggerDB::get().setLevel("cachelib", ParseLogLevel(o.cachelibLogLevel));

        std::filesystem::create_directories(options.cacheDir);
        const auto filePath = (options.cacheDir / o.fileName).string();
        // Contents never survive a restart, so start from an empty file.
        std::error_code ec;
        std::filesystem::remove(filePath, ec);

        Open(MakeNavyConfig(options, filePath));

        m_maxEntrySize = o.regionSizeBytes;
        BOOST_LOG_SEV(*m_logger, info) << "CacheLibSecondaryCache: initialized file='" << filePath
                                       << "', capacity=" << options.capacity
                                       << " bytes, usable=" << m_navy->getUsableSize() << " bytes";
    }

    ~Impl() {
        if (m_navy) {
            m_navy->drain();
        }
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) = delete;
    Impl& operator=(Impl&&) = delete;

    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper) noexcept {
        try {
            if (!helper || !helper->IsSecondaryCacheCompatible()) {
                return rocksdb::Status::OK();
            }
            const size_t dataSize = helper->size_cb(obj);
            if (dataSize == 0 || !Admissible(key, dataSize)) {
                return rocksdb::Status::OK();
            }
            auto entry = AllocateEntry(key, dataSize, rocksdb::kNoCompression, rocksdb::CacheTier::kVolatileTier);
            auto s = helper->saveto_cb(obj, 0, dataSize, entry.payload);
            if (!s.ok()) {
                return s;
            }
            Schedule(std::move(entry));
            return rocksdb::Status::OK();
        } catch (...) {
            return CurrentExceptionToStatus();
        }
    }

    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, rocksdb::CompressionType type,
                                rocksdb::CacheTier source) noexcept {
        try {
            if (saved.empty() || !Admissible(key, saved.size())) {
                return rocksdb::Status::OK();
            }
            auto entry = AllocateEntry(key, saved.size(), type, source);
            std::memcpy(entry.payload, saved.data(), saved.size());
            Schedule(std::move(entry));
            return rocksdb::Status::OK();
        } catch (...) {
            return CurrentExceptionToStatus();
        }
    }

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* createContext, bool wait, bool& keptInSecCache) noexcept {
        // Entries are never removed on a hit, so RocksDB must not re-insert
        // them into this cache when they are evicted from the primary again.
        keptInSecCache = false;
        try {
            if (!helper || !helper->IsSecondaryCacheCompatible() || key.size() > kMaxKeySize) {
                return nullptr;
            }
            if (!wait) {
                auto state = std::make_shared<AsyncLookupState>();
                state->key.assign(key.data(), key.size());
                auto handle = std::make_unique<CacheLibResultHandle>(state, helper, createContext, m_navy.get());
                m_navy->lookupAsync(navy::makeHK(state->key.data(), state->key.size()),
                                    [state](navy::Status status, facebook::cachelib::HashedKey, navy::Buffer value) {
                                        state->status = status;
                                        state->value = std::move(value);
                                        state->done.post();
                                    });
                keptInSecCache = true;
                return handle;
            }

            const auto hk = ToHashedKey(key);
            navy::Buffer value;
            if (m_navy->lookup(hk, value) != navy::Status::Ok) {
                return nullptr;
            }
            CreatedObject created;
            auto s = CreateObject(value.view(), helper, createContext, created);
            if (!s.ok()) {
                BOOST_LOG_SEV(*m_logger, warning)
                    << "CacheLibSecondaryCache: dropping entry that failed to deserialize: " << s.ToString();
                m_navy->remove(hk);
                return nullptr;
            }
            keptInSecCache = true;
            return std::make_unique<CacheLibResultHandle>(created.value, created.charge);
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << "CacheLibSecondaryCache: lookup failed: " << CurrentExceptionToStatus().ToString();
            return nullptr;
        }
    }

    void Erase(const rocksdb::Slice& key) noexcept {
        try {
            if (key.size() <= kMaxKeySize) {
                m_navy->remove(ToHashedKey(key));
            }
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << "CacheLibSecondaryCache: erase failed: " << CurrentExceptionToStatus().ToString();
        }
    }

    size_t GetCapacity() const noexcept { return m_navy->getUsableSize(); }

    size_t GetUsage() const {
        double used = 0;
        m_navy->getCounters(navy::CounterVisitor{[&used](folly::StringPiece name, double value) {
            if (name == "navy_bc_used_size_bytes") {
                used = value;
            }
        }});
        return static_cast<size_t>(used);
    }

    void VisitCounters(const std::function<void(std::string_view, double)>& visitor) const {
        m_navy->getCounters(navy::CounterVisitor{[&visitor](folly::StringPiece name, double value) {
            visitor(std::string_view(name.data(), name.size()), value);
        }});
    }

    void Drain() noexcept {
        try {
            m_navy->drain();
        } catch (...) {
        }
    }

    const std::string& PrintableOptions() const noexcept { return m_printable; }

  private:
    struct Entry {
        std::unique_ptr<char[]> buffer;
        size_t keySize = 0;
        size_t valueSize = 0;
        char* payload = nullptr;
    };

    void Open(const navy::NavyConfig& config) {
        m_navy = facebook::cachelib::createNavyCache(config, {}, {}, /*truncate*/ true, nullptr,
                                                      /*itemDestructorEnabled*/ false);
        m_printable = Describe(config);
    }

    bool Admissible(const rocksdb::Slice& key, size_t dataSize) {
        if (key.size() > kMaxKeySize || dataSize + kEntryHeaderSize + key.size() > m_maxEntrySize) {
            return false;
        }
        // The primary evicts blocks that were promoted from this cache; they
        // are still here, so skip the SSD write.
        return !m_navy->couldExist(ToHashedKey(key));
    }

    static Entry AllocateEntry(const rocksdb::Slice& key, size_t dataSize, rocksdb::CompressionType type,
                               rocksdb::CacheTier source) {
        Entry entry;
        entry.keySize = key.size();
        entry.valueSize = kEntryHeaderSize + dataSize;
        entry.buffer = std::make_unique_for_overwrite<char[]>(entry.keySize + entry.valueSize);
        char* p = entry.buffer.get();
        std::memcpy(p, key.data(), key.size());
        p[key.size()] = static_cast<char>(type);
        p[key.size() + 1] = static_cast<char>(source);
        entry.payload = p + key.size() + kEntryHeaderSize;
        return entry;
    }

    // Queues the write. Navy keeps views of the key and value, so the buffer
    // is owned by the completion callback. Rejections (admission policy,
    // parcel memory limit) silently drop the entry.
    void Schedule(Entry entry) {
        const char* base = entry.buffer.get();
        const auto hk = navy::makeHK(base, entry.keySize);
        const navy::BufferView value(entry.valueSize, reinterpret_cast<const uint8_t*>(base + entry.keySize));
        m_navy->insertAsync(hk, value,
                            [buffer = std::move(entry.buffer)](navy::Status, facebook::cachelib::HashedKey) mutable { buffer.reset(); });
    }

    std::shared_ptr<SecondaryCacheOptions::Logger> m_logger;
    std::unique_ptr<navy::AbstractCache> m_navy;
    size_t m_maxEntrySize = 0;
    std::string m_printable;
};

CacheLibSecondaryCache::CacheLibSecondaryCache(const SecondaryCacheOptions& options)
    : m_impl(std::make_unique<Impl>(options)) {}

CacheLibSecondaryCache::~CacheLibSecondaryCache() = default;

const char* CacheLibSecondaryCache::Name() const noexcept { return "CacheLibSecondaryCache"; }

rocksdb::Status CacheLibSecondaryCache::Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                                               const rocksdb::Cache::CacheItemHelper* helper,
                                               bool /*forceInsert*/) noexcept {
    return m_impl->Insert(key, obj, helper);
}

rocksdb::Status CacheLibSecondaryCache::InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved,
                                                    rocksdb::CompressionType type, rocksdb::CacheTier source) noexcept {
    return m_impl->InsertSaved(key, saved, type, source);
}

std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
CacheLibSecondaryCache::Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
                               rocksdb::Cache::CreateContext* create_context, bool wait, bool /*advise_erase*/,
                               rocksdb::Statistics* /*stats*/, bool& kept_in_sec_cache) noexcept {
    return m_impl->Lookup(key, helper, create_context, wait, kept_in_sec_cache);
}

void CacheLibSecondaryCache::Erase(const rocksdb::Slice& key) noexcept { m_impl->Erase(key); }

void CacheLibSecondaryCache::WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept {
    for (auto* handle : handles) {
        if (handle) {
            handle->Wait();
        }
    }
}

rocksdb::Status CacheLibSecondaryCache::SetCapacity(size_t /*capacity*/) noexcept {
    return rocksdb::Status::NotSupported("CacheLibSecondaryCache: capacity is fixed at construction");
}

rocksdb::Status CacheLibSecondaryCache::GetCapacity(size_t& capacity) noexcept {
    capacity = m_impl->GetCapacity();
    return rocksdb::Status::OK();
}

rocksdb::Status CacheLibSecondaryCache::Deflate(size_t /*decrease*/) noexcept {
    return rocksdb::Status::NotSupported("CacheLibSecondaryCache: capacity is fixed at construction");
}

rocksdb::Status CacheLibSecondaryCache::Inflate(size_t /*increase*/) noexcept {
    return rocksdb::Status::NotSupported("CacheLibSecondaryCache: capacity is fixed at construction");
}

std::string CacheLibSecondaryCache::GetPrintableOptions() const { return m_impl->PrintableOptions(); }

rocksdb::Status CacheLibSecondaryCache::GetUsage(size_t& usage) const noexcept {
    try {
        usage = m_impl->GetUsage();
        return rocksdb::Status::OK();
    } catch (...) {
        return CurrentExceptionToStatus();
    }
}

void CacheLibSecondaryCache::VisitCounters(
    const std::function<void(std::string_view name, double value)>& visitor) const {
    m_impl->VisitCounters(visitor);
}

void CacheLibSecondaryCache::Drain() noexcept { m_impl->Drain(); }
} // namespace AVEVA::RocksDB::Plugin::Core
