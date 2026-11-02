// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"

#include "LruFileIndex.hpp"
#include "ResultHandle.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>

#include <boost/algorithm/hex.hpp>
#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <boost/container/small_vector.hpp>

#include <cstdint>
#include <cstring>
#include <iterator>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core {
using namespace boost::log::trivial;

using Logger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;

/// <summary>Filesystem path helpers and best-effort file deletion.</summary>
struct FileUtil {
    /// <summary>
    /// Renames origPath to graveyardPath then deletes the graveyard file.
    /// Both operations are best-effort; a failed rename falls back to deleting the file directly.
    /// See RemoveEntryLocked for the TOCTOU trade-off: a concurrent insert for the
    /// same key may have replaced origPath between the in-memory removal (under lock)
    /// and this rename.  In that case the rename moves the new file to the graveyard,
    /// causing one cache miss on its next Lookup; Lookup's corruption-cleanup code
    /// then removes the phantom index entry.  This is acceptable for a best-effort cache.
    /// </summary>
    static void CommitEviction(Filesystem& fs, Logger& logger,
                               const std::optional<PendingEviction>& eviction) noexcept {
        if (eviction) {
            CommitPair(fs, logger, *eviction);
        }
    }

    /// <summary>Commits all rename+delete pairs produced by EvictUntilSizeLocked.</summary>
    static void CommitEvictions(Filesystem& fs, Logger& logger, const LruFileIndex::EvictList& evictions) noexcept {
        for (const auto& eviction : evictions) {
            CommitPair(fs, logger, eviction);
        }
    }

  private:
    // Path construction allocates; a failure leaves the file behind, which the cache tolerates (see above).
    static void CommitPair(Filesystem& fs, Logger& logger, const PendingEviction& p) noexcept {
        try {
            const std::filesystem::path graveyard{p.GraveyardPath.Value()};
            if (fs.RenameFile(RenameSource{std::filesystem::path{p.OriginalPath.Value()}},
                              RenameDestination{graveyard}) == FilesystemResult::Succeeded) {
                (void)fs.DeleteFile(graveyard);
                return;
            }
            // On Windows the rename fails while a reader still has the file open. The entry has already left the
            // index, so a file left behind would stay on disk uncounted; delete it directly instead.
            if (fs.DeleteFile(std::filesystem::path{p.OriginalPath.Value()}) == FilesystemResult::Failed) {
                BOOST_LOG_SEV(logger, warning)
                    << "Could not evict '" << p.OriginalPath.Value() << "'; the file was left on disk";
            }
        } catch (const std::exception& e) {
            try {
                BOOST_LOG_SEV(logger, warning)
                    << "Could not evict '" << p.OriginalPath.Value() << "': " << e.what() << "; the file may be left";
            } catch (...) {
                // Logging is best effort: this runs from cleanup paths that must not throw.
            }
        }
    }
};

/// <summary>Exception-to-rocksdb::Status conversion.</summary>
struct StatusUtil {
    /// <summary>
    /// Converts the currently-active exception to a rocksdb::Status.  Must only be called
    /// from within a catch block.
    /// </summary>
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

class FileBasedCompressedSecondaryCache::Impl {
    std::filesystem::path m_cacheDir;
    std::shared_ptr<Filesystem> m_fs;
    LruFileIndex m_lruIndex;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    enum class KeyLength : std::uint8_t { Fits, TooLong };

    /// <summary>
    /// Result of attempting to read a cache entry file.
    /// When status is Ok, contents holds the raw file bytes.
    /// The pin prevents LRU eviction while the caller processes the data.
    /// </summary>
    struct ReadEntryResult {
        enum class Status { Miss, Corrupt, Ok };
        Status status;
        std::string contents;
        std::optional<LruFileIndex::ScopedPin> pin;
    };

  public:
    explicit Impl(std::filesystem::path cacheDir, std::shared_ptr<Filesystem> fs, ByteCount capacity,
                  std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
        : m_cacheDir(std::move(cacheDir)), m_fs(std::move(fs)), m_lruIndex(m_cacheDir.string(), capacity),
          m_logger(std::move(logger)) {
        if (!m_logger) {
            throw std::invalid_argument("FileBasedCompressedSecondaryCache: logger cannot be null");
        }

        (void)m_fs->DeleteDir(m_cacheDir);
        if (m_fs->CreateDir(m_cacheDir) == FilesystemResult::Failed) {
            BOOST_LOG_SEV(*m_logger, error)
                << "FileBasedCompressedSecondaryCache: failed to create dir '" << m_cacheDir.string() << "'";
            throw std::runtime_error("FileBasedCompressedSecondaryCache: failed to create cache directory");
        }

        BOOST_LOG_SEV(*m_logger, info) << "FileBasedCompressedSecondaryCache: initialized dir='" << m_cacheDir.string()
                                       << "', capacity=" << capacity.Value() << " bytes";
    }

    const char* Name() const noexcept { return "FileBasedCompressedSecondaryCache"; }

    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper,
                           const LruFileIndex::InsertMode mode) noexcept {
        try {
            if (!helper || !helper->IsSecondaryCacheCompatible()) {
                return rocksdb::Status::OK();
            }

            if (ClassifyKey(key) == KeyLength::TooLong) {
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
            }

            const size_t dataSize = helper->size_cb(obj);
            if (dataSize == 0) {
                return rocksdb::Status::OK();
            }

            // One buffer in the on-disk layout: [type byte][payload]; saveto_cb writes straight into it.
            boost::container::small_vector<char, 1 + 4096> buf(dataSize + kFileHeaderSize.Value(),
                                                               boost::container::default_init);
            buf[0] = static_cast<char>(static_cast<uint8_t>(rocksdb::CompressionType::kNoCompression));
            auto s = helper->saveto_cb(obj, 0, dataSize, buf.data() + kFileHeaderSize.Value());
            if (!s.ok()) {
                return s;
            }

            return WriteEntry(key, std::span<const char>{buf.data(), buf.size()}, mode);
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, rocksdb::CompressionType type,
                                rocksdb::CacheTier /*source*/) noexcept {
        try {
            if (saved.size() == 0) {
                return rocksdb::Status::OK();
            }

            if (ClassifyKey(key) == KeyLength::TooLong) {
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
            }

            boost::container::small_vector<char, 1 + 4096> buf(saved.size() + kFileHeaderSize.Value(),
                                                               boost::container::default_init);
            buf[0] = static_cast<char>(static_cast<uint8_t>(type));
            std::memcpy(buf.data() + kFileHeaderSize.Value(), saved.data(), saved.size());
            return WriteEntry(key, std::span<const char>{buf.data(), buf.size()}, LruFileIndex::InsertMode::Force);
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    /// <summary>Whether the entry stays in the cache after a lookup that found it.</summary>
    enum class Retention : std::uint8_t {
        Dropped,
        Kept,
    };

    /// <summary>Whether the caller asked for a found entry to be erased as it is returned.</summary>
    enum class EraseAdvice : std::uint8_t {
        Keep,
        Erase,
    };

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle> Lookup(const rocksdb::Slice& key,
                                                                const rocksdb::Cache::CacheItemHelper* cacheItemHelper,
                                                                rocksdb::Cache::CreateContext* createContext,
                                                                const EraseAdvice advice, rocksdb::Statistics* stats,
                                                                Retention& retention) noexcept {
        try {
            retention = Retention::Dropped;
            if (!cacheItemHelper || !cacheItemHelper->IsSecondaryCacheCompatible()) {
                return nullptr;
            }

            if (ClassifyKey(key) == KeyLength::TooLong) {
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

            std::optional<PendingEviction> deferredEviction;
            if (advice == EraseAdvice::Erase) {
                deferredEviction = m_lruIndex.Remove(filename);
                if (!deferredEviction) {
                    return nullptr; // entry disappeared between map and this stage.
                }
            } else {
                if (m_lruIndex.Touch(filename) == LruFileIndex::TouchResult::NotFound) {
                    return nullptr; // entry disappeared between map and this stage.
                }
            }

            // Defer eviction I/O until after the mapped view is released to avoid
            // rename/delete failures on Windows while the file is still mapped.
            auto evictionCleanup = boost::scope::make_scope_exit(
                [&] noexcept { FileUtil::CommitEviction(*m_fs, *m_logger, deferredEviction); });

            // On any validation failure, remove the corrupt entry from the index.
            // When erase was advised the entry is already gone, so no cleanup needed.
            enum class Validation : std::uint8_t { Pending, Valid };
            Validation validation = Validation::Pending;
            auto corruptionCleanup = boost::scope::make_scope_exit([&] noexcept {
                if (validation == Validation::Pending && advice == EraseAdvice::Keep)
                    CleanupCorruptEntry(filename);
            });

            // File format: [1-byte compression type][payload bytes]
            const auto& contents = readResult.contents;
            if (contents.empty()) {
                BOOST_LOG_SEV(*m_logger, warning)
                    << "FileBasedCompressedSecondaryCache: file empty for indexed entry '" << filename << "'";
                return nullptr;
            }

            const auto compressionType = static_cast<rocksdb::CompressionType>(static_cast<uint8_t>(contents[0]));
            const rocksdb::Slice dataSlice{contents.data() + 1, contents.size() - 1};

            rocksdb::Cache::ObjectPtr outObj = nullptr;
            size_t outCharge = 0;
            auto objCleanup = boost::scope::make_scope_exit([&] noexcept {
                if (outObj)
                    cacheItemHelper->del_cb(outObj, /*allocator=*/nullptr);
            });

            auto s = cacheItemHelper->create_cb(dataSlice, compressionType, rocksdb::CacheTier::kNonVolatileBlockTier,
                                                createContext,
                                                /*allocator=*/nullptr, &outObj, &outCharge);
            if (!s.ok() || outObj == nullptr) {
                return nullptr;
            }

            RecordHitStats(stats, cacheItemHelper->role);

            validation = Validation::Valid;
            retention = advice == EraseAdvice::Keep ? Retention::Kept : Retention::Dropped;
            auto result = std::make_unique<ResultHandle>(outObj, outCharge);
            objCleanup.set_active(false);
            return result;
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
            return nullptr;
        }
    }

    void Erase(const rocksdb::Slice& key) noexcept {
        try {
            if (ClassifyKey(key) == KeyLength::TooLong) {
                return;
            }

            const auto filename = KeyToFilename(key);
            FileUtil::CommitEviction(*m_fs, *m_logger, m_lruIndex.Remove(filename));
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
        }
    }

    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> /*handles*/) noexcept {}

    rocksdb::Status SetCapacity(const ByteCount capacity) noexcept {
        try {
            FileUtil::CommitEvictions(*m_fs, *m_logger, m_lruIndex.SetCapacity(capacity));
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    ByteCount GetCapacity() const noexcept { return m_lruIndex.GetCapacity(); }

    rocksdb::Status Deflate(const ByteCount decrease) noexcept {
        try {
            FileUtil::CommitEvictions(*m_fs, *m_logger, m_lruIndex.Deflate(decrease));
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    rocksdb::Status Inflate(const ByteCount increase) noexcept {
        try {
            m_lruIndex.Inflate(increase);
            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    ByteCount GetUsage() const noexcept { return m_lruIndex.GetUsage(); }

  private:
    /// <summary>
    /// Hex-encodes <paramref name="key"/> and returns the result.
    /// Returns an empty string when the key is too long to encode inline.
    /// </summary>
    [[nodiscard]] static boost::static_string<LruFileIndex::kMaxFilenameLen>
    KeyToFilename(const rocksdb::Slice& key) noexcept {
        if (ClassifyKey(key) == KeyLength::TooLong) {
            return {};
        }

        boost::static_string<LruFileIndex::kMaxFilenameLen> result;
        boost::algorithm::hex_lower(key.data(), key.data() + key.size(), std::back_inserter(result));
        return result;
    }

    /// <summary>
    /// Classifies whether the hex-encoded key would exceed the inline filename buffer.
    /// Each input byte is encoded as two hex characters; uses an overflow-safe comparison.
    /// </summary>
    [[nodiscard]] static KeyLength ClassifyKey(const rocksdb::Slice& key) noexcept {
        return key.size() > LruFileIndex::kMaxFilenameLen / 2 ? KeyLength::TooLong : KeyLength::Fits;
    }

    /// <summary>
    /// Writes bytes to disk and updates the in-memory index.
    /// </summary>
    /// <param name="key">The key identifying the cache entry to write.</param>
    /// <param name="stored">Complete file image: the 1-byte compression type followed by the payload.</param>
    /// <param name="mode">With IfRoom, the write is skipped rather than evicting an existing entry to make
    /// room.</param>
    rocksdb::Status WriteEntry(const rocksdb::Slice& key, const std::span<const char> stored,
                               const LruFileIndex::InsertMode mode) noexcept {
        try {
            if (ClassifyKey(key) == KeyLength::TooLong) {
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
            }

            const auto filename = KeyToFilename(key);
            const ByteCount storedSize{stored.size()};

            // Phase 1: lock, gate capacity, pin existing entry, schedule evictions.
            //          Commit I/O (rename + delete) only after releasing the lock.
            auto reserved = m_lruIndex.ReserveCapacity(filename, storedSize, mode);
            if (!reserved)
                return rocksdb::Status::OK(); // admission control: over capacity, skip write
            FileUtil::CommitEvictions(*m_fs, *m_logger, *reserved);

            // Phase 2: write the file — no lock held.
            if (auto s = WriteToDisk(filename, stored); !s.ok())
                return s;

            // Phase 3: lock, register the new entry, correct concurrent overshoot.
            //          Commit I/O (rename + delete) only after releasing the lock.
            const auto evictList = m_lruIndex.RegisterEntry(filename, storedSize);
            FileUtil::CommitEvictions(*m_fs, *m_logger, evictList);

            return rocksdb::Status::OK();
        } catch (...) {
            const auto s = StatusUtil::CurrentExceptionToStatus();
            BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
            return s;
        }
    }

    /// <summary>
    /// Phase 2 of WriteEntry — called with no lock held.
    /// Writes the prepared file image ([1-byte compression type][payload bytes]) in a single
    /// atomic write call.
    /// </summary>
    [[nodiscard]] rocksdb::Status WriteToDisk(const std::string_view filename, const std::span<const char> stored) {
        const std::string pathStr = m_lruIndex.MakePath(filename);

        if (m_fs->WriteFileAtomic(pathStr, stored) == FilesystemResult::Failed) {
            BOOST_LOG_SEV(*m_logger, warning)
                << "FileBasedCompressedSecondaryCache: failed to write cache entry '" << pathStr << "'";
            return rocksdb::Status::IOError("Failed to write cache entry file", pathStr);
        }

        return rocksdb::Status::OK();
    }

    /// <summary>
    /// Phase 1 of Lookup — pins the entry to prevent eviction during I/O, then reads
    /// the file into memory.  The three outcomes are named explicitly in
    /// ReadEntryResult::Status.
    /// </summary>
    [[nodiscard]] ReadEntryResult ReadEntryForLookup(const std::string_view filename, const std::string& pathStr) {
        auto pin = m_lruIndex.TryPin(filename);
        if (!pin) {
            return {ReadEntryResult::Status::Miss};
        }

        auto contents = m_fs->ReadFileContents(pathStr);
        if (!contents) {
            BOOST_LOG_SEV(*m_logger, warning)
                << "FileBasedCompressedSecondaryCache: file missing for indexed entry '" << filename << "'";
            return {ReadEntryResult::Status::Corrupt};
        }

        return {ReadEntryResult::Status::Ok, std::move(*contents), std::move(pin)};
    }

    /// <summary>Removes a corrupt or missing entry from the in-memory index and commits the eviction.
    /// Best-effort; never throws.</summary>
    void CleanupCorruptEntry(std::string_view filename) noexcept {
        try {
            FileUtil::CommitEviction(*m_fs, *m_logger, m_lruIndex.Remove(filename));
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error)
                << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
        }
    }

    /// <summary>Records a secondary cache hit to RocksDB's statistics subsystem.</summary>
    static void RecordHitStats(rocksdb::Statistics* stats, const rocksdb::CacheEntryRole role) noexcept {
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
};

FileBasedCompressedSecondaryCache::FileBasedCompressedSecondaryCache(
    std::filesystem::path cacheDir, std::shared_ptr<Filesystem> fs, const ByteCount capacity,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_impl(std::make_unique<Impl>(std::move(cacheDir), std::move(fs), capacity, std::move(logger))) {}

AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache::~FileBasedCompressedSecondaryCache() {}

const char* FileBasedCompressedSecondaryCache::Name() const noexcept { return m_impl->Name(); }

// The RocksDB overrides below keep RocksDB's bool/size_t signatures; each converts to the strong types straight away.
rocksdb::Status FileBasedCompressedSecondaryCache::Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                                                          const rocksdb::Cache::CacheItemHelper* helper,
                                                          const bool forceInsert) noexcept {
    return m_impl->Insert(key, obj, helper,
                          forceInsert ? LruFileIndex::InsertMode::Force : LruFileIndex::InsertMode::IfRoom);
}

rocksdb::Status FileBasedCompressedSecondaryCache::InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved,
                                                               const rocksdb::CompressionType type,
                                                               const rocksdb::CacheTier source) noexcept {
    return m_impl->InsertSaved(key, saved, type, source);
}

std::unique_ptr<rocksdb::SecondaryCacheResultHandle> FileBasedCompressedSecondaryCache::Lookup(
    const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* cacheItemHelper,
    rocksdb::Cache::CreateContext* createContext, bool /*wait*/, const bool adviseErase, rocksdb::Statistics* stats,
    bool& keptInSecondaryCache) noexcept {
    auto retention = Impl::Retention::Dropped;
    auto result = m_impl->Lookup(key, cacheItemHelper, createContext,
                                 adviseErase ? Impl::EraseAdvice::Erase : Impl::EraseAdvice::Keep, stats, retention);
    keptInSecondaryCache = retention == Impl::Retention::Kept;
    return result;
}

void FileBasedCompressedSecondaryCache::Erase(const rocksdb::Slice& key) noexcept { m_impl->Erase(key); }

void FileBasedCompressedSecondaryCache::WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept {
    m_impl->WaitAll(std::move(handles));
}

rocksdb::Status FileBasedCompressedSecondaryCache::SetCapacity(const size_t capacity) noexcept {
    return m_impl->SetCapacity(ByteCount{capacity});
}

rocksdb::Status FileBasedCompressedSecondaryCache::GetCapacity(size_t& capacity) noexcept {
    capacity = static_cast<size_t>(m_impl->GetCapacity().Value());
    return rocksdb::Status::OK();
}

ByteCount FileBasedCompressedSecondaryCache::GetUsage() const noexcept { return m_impl->GetUsage(); }

rocksdb::Status FileBasedCompressedSecondaryCache::Deflate(const size_t decrease) noexcept {
    return m_impl->Deflate(ByteCount{decrease});
}

rocksdb::Status FileBasedCompressedSecondaryCache::Inflate(const size_t increase) noexcept {
    return m_impl->Inflate(ByteCount{increase});
}

} // namespace AVEVA::RocksDB::Plugin::Core
