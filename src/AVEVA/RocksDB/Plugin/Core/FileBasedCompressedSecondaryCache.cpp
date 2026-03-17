// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"

#include "LruFileIndex.hpp"
#include "MapEntryResult.hpp"
#include "ParsedHeader.hpp"
#include "FileFormat.hpp"
#include "ResultHandle.hpp"
#include "CrcUtil.hpp"
#include "ZstdCodec.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>

#include <boost/algorithm/hex.hpp>
#include <boost/endian/buffers.hpp>
#include <boost/scope/scope_exit.hpp>
#include <boost/log/trivial.hpp>

#include <boost/container/small_vector.hpp>

#include <cstring>
#include <iterator>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <ranges>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core
{
    using namespace boost::log::trivial;
    static_assert(sizeof(FileFormat::Header) == FileBasedCompressedSecondaryCache::kFileHeaderSize,
        "FileFormat::Header size must match the public kFileHeaderSize constant");


    /// <summary>Filesystem path helpers and best-effort file deletion.</summary>
    struct FileUtil
    {
        /// <summary>
        /// Renames origPath to graveyardPath then deletes the graveyard file.
        /// Both operations are best-effort; errors are silently ignored.
        /// See RemoveEntryLocked for the TOCTOU trade-off: a concurrent insert for the
        /// same key may have replaced origPath between the in-memory removal (under lock)
        /// and this rename.  In that case the rename moves the new file to the graveyard,
        /// causing one cache miss on its next Lookup; Lookup's corruption-cleanup code
        /// then removes the phantom index entry.  This is acceptable for a best-effort cache.
        /// </summary>
        static void CommitEviction(Filesystem& fs, const std::pair<std::string, std::string>& p) noexcept
        {
            if (p.first.empty()) return;
            if (fs.RenameFile(p.first, p.second))
            {
                fs.DeleteFile(p.second);
            }
        }

        /// <summary>Commits all rename+delete pairs produced by EvictUntilSizeLocked.</summary>
        static void CommitEvictions(Filesystem& fs, const std::vector<std::pair<std::string, std::string>>& pairs) noexcept
        {
            for (const auto& p : pairs)
            {
                CommitEviction(fs, p);
            }
        }
    };

    /// <summary>Exception-to-rocksdb::Status conversion.</summary>
    struct StatusUtil
    {
        /// <summary>
        /// Converts the currently-active exception to a rocksdb::Status.  Must only be called
        /// from within a catch block.
        /// </summary>
        static rocksdb::Status CurrentExceptionToStatus() noexcept
        {
            try { throw; }
            catch (const std::bad_alloc&) { return rocksdb::Status::MemoryLimit("out of memory"); }
            catch (const std::exception& e) { return rocksdb::Status::Aborted(e.what()); }
            catch (...) { return rocksdb::Status::Aborted("unknown exception in secondary cache"); }
        }
    };

    class FileBasedCompressedSecondaryCache::Impl
    {
        std::filesystem::path m_cacheDir;
        std::shared_ptr<Filesystem> m_fs;
        int m_zstdLevel;
        LruFileIndex m_lruIndex;
        std::shared_ptr<boost::log::sources::severity_logger_mt<
            boost::log::trivial::severity_level>> m_logger;

    public:
        explicit Impl(std::filesystem::path cacheDir,
            std::shared_ptr<Filesystem> fs,
            size_t capacity,
            int zstdLevel,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
            : m_cacheDir(std::move(cacheDir)),
            m_fs(std::move(fs)), m_zstdLevel(zstdLevel),
            m_lruIndex(m_cacheDir.string(), capacity),
            m_logger(std::move(logger))
        {
            m_fs->DeleteDir(m_cacheDir);
            m_fs->CreateDir(m_cacheDir);

            // Pre-create all 256 shard subdirectories so WriteEntry never calls
            // CreateDir on the hot write path.
            constexpr std::string_view kHex = "0123456789abcdef";
            for (const auto i : std::views::iota(0u, 256u))
            {
                std::string shard;
                shard.reserve(2);
                shard.push_back(kHex.at(i >> 4));
                shard.push_back(kHex.at(i & 0xf));
                m_fs->CreateDir(m_cacheDir / shard);
            }

            BOOST_LOG_SEV(*m_logger, info)
                << "FileBasedCompressedSecondaryCache: initialized dir='" << m_cacheDir.string()
                << "', capacity=" << capacity << " bytes, zstd_level=" << zstdLevel;
        }

        const char* Name() const noexcept
        {
            return "FileBasedCompressedSecondaryCache";
        }

        rocksdb::Status Insert(const rocksdb::Slice& key,
            rocksdb::Cache::ObjectPtr obj,
            const rocksdb::Cache::CacheItemHelper* helper,
            const bool forceInsert) noexcept
        {
            try
            {
                if (!helper || !helper->IsSecondaryCacheCompatible())
                {
                    return rocksdb::Status::OK();
                }

                if (IsKeyTooLong(key))
                {
                    return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
                }

                const size_t dataSize = helper->size_cb(obj);
                if (dataSize == 0)
                {
                    return rocksdb::Status::OK();
                }

                boost::container::small_vector<char, 4096> buf(dataSize);
                auto s = helper->saveto_cb(obj, 0, dataSize, buf.data());
                if (!s.ok())
                {
                    return s;
                }

                const auto compressed = ZstdCodec::MaybeCompress(buf.data(), dataSize, m_zstdLevel,
                    FileFormat::kMinCompressibleSize, FileFormat::zstdCompression);
                return WriteEntry(key, compressed.type, compressed.data, compressed.size, forceInsert);
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        rocksdb::Status InsertSaved(const rocksdb::Slice& key,
            const rocksdb::Slice& saved,
            rocksdb::CompressionType type,
            rocksdb::CacheTier /*source*/) noexcept
        {
            try
            {
                if (saved.size() == 0)
                {
                    return rocksdb::Status::OK();
                }

                if (IsKeyTooLong(key))
                {
                    return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
                }

                if (type == rocksdb::CompressionType::kNoCompression)
                {
                    const auto compressed = ZstdCodec::MaybeCompress(saved.data(), saved.size(), m_zstdLevel,
                        FileFormat::kMinCompressibleSize, FileFormat::zstdCompression);
                    return WriteEntry(key, compressed.type, compressed.data, compressed.size);
                }

                return WriteEntry(key, type, saved.data(), saved.size());
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        std::unique_ptr<rocksdb::SecondaryCacheResultHandle> Lookup(
            const rocksdb::Slice& key,
            const rocksdb::Cache::CacheItemHelper* cacheItemHelper,
            rocksdb::Cache::CreateContext* createContext,
            bool /*wait*/,
            bool adviseErase,
            rocksdb::Statistics* stats,
            bool& keptInSecondaryCache) noexcept
        {
            try
            {
                keptInSecondaryCache = false;
                if (!cacheItemHelper || !cacheItemHelper->IsSecondaryCacheCompatible())
                {
                    return nullptr;
                }

                if (IsKeyTooLong(key))
                {
                    return nullptr;
                }

                const auto filename = KeyToFilename(key);
                const std::string pathStr = m_lruIndex.MakePath(filename);

                auto mapResult = MapEntryForRead(filename, pathStr);
                if (mapResult.status == MapEntryResult::Status::Miss)
                {
                    return nullptr;
                }
                else if (mapResult.status == MapEntryResult::Status::Corrupt)
                {
                    CleanupCorruptEntry(filename);
                    return nullptr;
                }

                if (adviseErase)
                {
                    const auto evictionPair = m_lruIndex.Remove(filename);
                    if (evictionPair.first.empty())
                    {
                        return nullptr; // entry disappeared between map and this stage.
                    }

                    FileUtil::CommitEviction(*m_fs, evictionPair);
                }
                else
                {
                    if (!m_lruIndex.Touch(filename))
                    {
                        return nullptr; // entry disappeared between map and this stage.
                    }
                }

                // On any validation failure, remove the corrupt entry from the index.
                // When advise_erase was set the entry is already gone, so no cleanup needed.
                bool entryIsValid = false;
                auto corruptionCleanup = boost::scope::make_scope_exit([&] noexcept {
                    if (!entryIsValid && !adviseErase)
                        CleanupCorruptEntry(filename);
                    });

                const auto parsedHeader = ValidateHeader(mapResult.view->Data(), mapResult.view->Size());
                if (!parsedHeader)
                {
                    BOOST_LOG_SEV(*m_logger, warning)
                        << "FileBasedCompressedSecondaryCache: header validation failed for '" << filename << "'";
                    return nullptr;
                }

                const auto& [compressionType, payload] = *parsedHeader;

                std::string decompressed;
                rocksdb::Slice dataSlice;
                rocksdb::CompressionType effectiveType;
                if (static_cast<uint8_t>(compressionType) == FileFormat::zstdCompression)
                {
                    try
                    {
                        decompressed = ZstdCodec::Decompress(payload.data(), payload.size());
                    }
                    catch (const std::runtime_error&)
                    {
                        BOOST_LOG_SEV(*m_logger, warning)
                            << "FileBasedCompressedSecondaryCache: decompression failed for '" << filename << "'";
                        return nullptr;
                    }

                    dataSlice = { decompressed.data(), decompressed.size() };
                    effectiveType = rocksdb::CompressionType::kNoCompression;
                }
                else
                {
                    dataSlice = { payload.data(), payload.size() };
                    effectiveType = compressionType;
                }

                rocksdb::Cache::ObjectPtr outObj = nullptr;
                size_t outCharge = 0;
                auto s = cacheItemHelper->create_cb(dataSlice,
                    effectiveType,
                    rocksdb::CacheTier::kNonVolatileBlockTier,
                    createContext,
                    /*allocator=*/nullptr,
                    &outObj,
                    &outCharge);
                if (!s.ok() || outObj == nullptr)
                {
                    return nullptr;
                }

                auto objCleanup = boost::scope::make_scope_exit([&] noexcept {
                    if (outObj) cacheItemHelper->del_cb(outObj, /*allocator=*/nullptr);
                    });

                RecordHitStats(stats, cacheItemHelper->role);

                entryIsValid = true;
                keptInSecondaryCache = !adviseErase;
                auto result = std::make_unique<ResultHandle>(outObj, outCharge);
                objCleanup.set_active(false);
                return result;
            }
            catch (...)
            {
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
                return nullptr;
            }
        }

        bool SupportForceErase() const noexcept { return true; }
        void Erase(const rocksdb::Slice& key) noexcept
        {
            try
            {
                if (IsKeyTooLong(key))
                {
                    return;
                }

                const auto filename = KeyToFilename(key);
                FileUtil::CommitEviction(*m_fs, m_lruIndex.Remove(filename));
            }
            catch (...)
            {
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << StatusUtil::CurrentExceptionToStatus().ToString();
            }
        }

        void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> /*handles*/) noexcept
        {
        }

        rocksdb::Status SetCapacity(size_t capacity) noexcept
        {
            try
            {
                FileUtil::CommitEvictions(*m_fs, m_lruIndex.SetCapacity(capacity));
                return rocksdb::Status::OK();
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        rocksdb::Status GetCapacity(size_t& capacity) noexcept
        {
            try
            {
                capacity = m_lruIndex.GetCapacity();
                return rocksdb::Status::OK();
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        rocksdb::Status Deflate(const size_t decrease) noexcept
        {
            try
            {
                FileUtil::CommitEvictions(*m_fs, m_lruIndex.Deflate(decrease));
                return rocksdb::Status::OK();
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        rocksdb::Status Inflate(const size_t increase) noexcept
        {
            try
            {
                m_lruIndex.Inflate(increase);
                return rocksdb::Status::OK();
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        rocksdb::Status GetUsage(size_t& usage) const
        {
            try
            {
                usage = m_lruIndex.GetUsage();
                return rocksdb::Status::OK();
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        uint64_t GetEvictedCount() const noexcept
        {
            return m_lruIndex.GetEvictedCount();
        }
    private:
        /// <summary>
        /// Immediately-ready result handle returned by Lookup().
        /// Moved to its own header to reduce this header's size.
        /// </summary>

        /// <summary>
        /// Hex-encodes <paramref name="key"/> and returns the result.
        /// Returns an empty string when the key is too long to encode inline.
        /// </summary>
        [[nodiscard]] static boost::static_string<LruFileIndex::kMaxFilenameLen> KeyToFilename(
            const rocksdb::Slice& key) noexcept
        {
            if (IsKeyTooLong(key))
            {
                return {};
            }

            boost::static_string<LruFileIndex::kMaxFilenameLen> result;
            boost::algorithm::hex_lower(key.data(), key.data() + key.size(), std::back_inserter(result));
            return result;
        }

        /// <summary>
        /// Returns true when the hex-encoded key would exceed the inline filename buffer.
        /// Each input byte is encoded as two hex characters, so the hex-encoded length is
        /// twice the key length; the check uses <c>key.size() * 2</c> to reflect this.
        /// </summary>
        [[nodiscard]] static bool IsKeyTooLong(const rocksdb::Slice& key) noexcept
        {
            return key.size() * 2 > LruFileIndex::kMaxFilenameLen;
        }

        /// <summary>
        /// Writes bytes to disk and updates the in-memory index.
        /// </summary>
        /// <param name="key">The key identifying the cache entry to write.</param>
        /// <param name="type">The compression type of the data to store.</param>
        /// <param name="data">Pointer to the bytes to write to disk.</param>
        /// <param name="dataSize">Size in bytes of the data pointed to by <paramref name="data"/>.</param>
        /// <param name="force_insert">When false, the write is skipped rather than evicting an existing entry to make room.</param>
        rocksdb::Status WriteEntry(const rocksdb::Slice& key,
            const rocksdb::CompressionType type,
            const char* data,
            const size_t dataSize,
            const bool forceInsert = true) noexcept
        {
            try
            {
                if (IsKeyTooLong(key))
                {
                    return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
                }

                const auto filename = KeyToFilename(key);
                const size_t storedSize = dataSize + sizeof(FileFormat::Header);

                // Phase 1: lock, gate capacity, pin existing entry, schedule evictions.
                //          Commit I/O (rename + delete) only after releasing the lock.
                auto reserved = m_lruIndex.ReserveCapacity(filename, storedSize, forceInsert);
                if (!reserved)
                    return rocksdb::Status::OK(); // admission control: over capacity, skip write
                FileUtil::CommitEvictions(*m_fs, *reserved);

                // Phase 2: write the file — no lock held.
                if (auto s = WriteToDisk(filename, type, data, dataSize, storedSize); !s.ok())
                    return s;

                // Phase 3: lock, register the new entry, correct concurrent overshoot.
                //          Commit I/O (rename + delete) only after releasing the lock.
                const auto evictList = m_lruIndex.RegisterEntry(filename, storedSize);
                FileUtil::CommitEvictions(*m_fs, evictList);

                return rocksdb::Status::OK();
            }
            catch (...)
            {
                const auto s = StatusUtil::CurrentExceptionToStatus();
                BOOST_LOG_SEV(*m_logger, error) << Name() << "::" << __func__ << ": " << s.ToString();
                return s;
            }
        }

        /// <summary>
        /// Phase 2 of WriteEntry — called with no lock held.
        /// Builds the on-disk header, computes the CRC32C checksum, concatenates header
        /// and payload into a single buffer, and writes it atomically to pathStr.
        /// </summary>
        [[nodiscard]] rocksdb::Status WriteToDisk(const std::string_view filename,
            const rocksdb::CompressionType type,
            const char* data,
            const size_t dataSize,
            const size_t storedSize)
        {
            const std::string pathStr = m_lruIndex.MakePath(filename);
            FileFormat::Header header{};
            header.magic = FileFormat::magicFilePrefix;
            header.version = FileFormat::kFileVersion;
            header.compressionType = static_cast<uint8_t>(type);
            header.dataSize = static_cast<uint64_t>(dataSize);
            // Checksum covers compressionType + dataSize + payload so a bit-flip in any
            // header field is detected alongside payload corruption.
            header.checksum = CrcUtil::Compute(
                reinterpret_cast<const char*>(&header.compressionType),
                sizeof(header.compressionType) + sizeof(header.dataSize),
                data, dataSize);

            // Combine header and payload into a single buffer for one atomic write call.
            // small_vector keeps entries up to ~4 KiB on the stack.
            boost::container::small_vector<char, sizeof(FileFormat::Header) + 4096> writeBuf(storedSize);
            std::memcpy(writeBuf.data(), &header, sizeof(header));
            std::memcpy(writeBuf.data() + sizeof(header), data, dataSize);

            if (!m_fs->WriteFileAtomic(pathStr, writeBuf.data(), writeBuf.size()))
            {
                BOOST_LOG_SEV(*m_logger, warning)
                    << "FileBasedCompressedSecondaryCache: failed to write cache entry '" << pathStr << "'";
                return rocksdb::Status::IOError("Failed to write cache entry file", pathStr);
            }

            return rocksdb::Status::OK();
        }

        /// <summary>Phase 1 of Lookup — pins the entry to prevent eviction during I/O, then maps the file.
        /// The three outcomes are named explicitly in MapEntryResult::Status.</summary>
        // MapEntryResult and ParsedHeader have been moved to their own headers.
        [[nodiscard]] MapEntryResult MapEntryForRead(const std::string_view filename,
            const std::string& pathStr)
        {
            const auto pin = m_lruIndex.TryPin(filename);
            if (!pin)
            {
                return { MapEntryResult::Status::Miss };
            }

            auto view = m_fs->MapReadOnly(pathStr);
            if (!view)
            {
                BOOST_LOG_SEV(*m_logger, warning)
                    << "FileBasedCompressedSecondaryCache: file missing for indexed entry '" << filename << "'";
                return { MapEntryResult::Status::Corrupt };
            }

            return { MapEntryResult::Status::Ok, std::move(view) };
        }

        /// <summary>Removes a corrupt or missing entry from the in-memory index and commits the eviction.
        /// Best-effort; never throws.</summary>
        void CleanupCorruptEntry(std::string_view filename) noexcept
        {
            try
            {
                FileUtil::CommitEviction(*m_fs, m_lruIndex.Remove(filename));
            }
            catch (...)
            {
                BOOST_LOG_SEV(*m_logger, error) << Name()
                    << "::"
                    << __func__
                    << ": "
                    << StatusUtil::CurrentExceptionToStatus().ToString();
            }
        }

        /// <summary>Validates the on-disk header of a mapped cache entry.
        /// Returns std::nullopt on any mismatch (magic, version, bounds, or checksum).</summary>
        [[nodiscard]] std::optional<ParsedHeader> ValidateHeader(
            const char* mapped,
            const size_t mappedSize) noexcept
        {
            if (mappedSize < sizeof(FileFormat::Header))
            {
                return std::nullopt;
            }

            FileFormat::Header header;
            std::memcpy(&header, mapped, sizeof(header));
            if (header.magic.value() != FileFormat::magicFilePrefix)
            {
                return std::nullopt;
            }

            if (header.version.value() != FileFormat::kFileVersion)
            {
                return std::nullopt;
            }

            const auto dataSize = static_cast<size_t>(header.dataSize.value());
            if (dataSize + sizeof(FileFormat::Header) > mappedSize)
            {
                return std::nullopt;
            }

            const char* dataPtr = mapped + sizeof(FileFormat::Header);
            const auto compressionType = static_cast<rocksdb::CompressionType>(header.compressionType.value());
            if (CrcUtil::Compute(
                reinterpret_cast<const char*>(&header.compressionType),
                sizeof(header.compressionType) + sizeof(header.dataSize),
                dataPtr, dataSize) != header.checksum.value())
            {
                BOOST_LOG_SEV(*m_logger, error) << "Computed mismatched checksum. Cached file is corrupt";
                return std::nullopt;
            }

            return ParsedHeader{ compressionType, std::span<const char>(dataPtr, dataSize) };
        }

        /// <summary>Records a secondary cache hit to RocksDB's statistics subsystem.</summary>
        static void RecordHitStats(rocksdb::Statistics* stats,
            const rocksdb::CacheEntryRole role) noexcept
        {
            if (stats == nullptr)
            {
                return;
            }

            stats->recordTick(rocksdb::SECONDARY_CACHE_HITS);
            switch (role)
            {
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
        std::filesystem::path cacheDir,
        std::shared_ptr<Filesystem> fs,
        const size_t capacity,
        const int zstdLevel,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
        : m_impl(std::make_unique<Impl>(std::move(cacheDir),
            std::move(fs),
            capacity,
            zstdLevel,
            std::move(logger)))
    {
    }

    AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache::~FileBasedCompressedSecondaryCache()
    {
    }

    const char* FileBasedCompressedSecondaryCache::Name() const noexcept
    {
        return m_impl->Name();
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::Insert(
        const rocksdb::Slice& key,
        rocksdb::Cache::ObjectPtr obj,
        const rocksdb::Cache::CacheItemHelper* helper,
        const bool forceInsert) noexcept
    {
        return m_impl->Insert(key, obj, helper, forceInsert);
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::InsertSaved(
        const rocksdb::Slice& key,
        const rocksdb::Slice& saved,
        const rocksdb::CompressionType type,
        const rocksdb::CacheTier source) noexcept
    {
        return m_impl->InsertSaved(key, saved, type, source);
    }

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
        FileBasedCompressedSecondaryCache::Lookup(const rocksdb::Slice& key,
            const rocksdb::Cache::CacheItemHelper* cacheItemHelper,
            rocksdb::Cache::CreateContext* createContext,
            bool wait,
            bool adviseErase,
            rocksdb::Statistics* stats,
            bool& keptInSecondaryCache) noexcept
    {
        return m_impl->Lookup(key, cacheItemHelper, createContext, wait, adviseErase, stats, keptInSecondaryCache);
    }

    void FileBasedCompressedSecondaryCache::Erase(const rocksdb::Slice& key) noexcept
    {
        m_impl->Erase(key);
    }

    void FileBasedCompressedSecondaryCache::WaitAll(
        std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept
    {
        return m_impl->WaitAll(handles);
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::SetCapacity(const size_t capacity) noexcept
    {
        return m_impl->SetCapacity(capacity);
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::GetCapacity(size_t& capacity) noexcept
    {
        return m_impl->GetCapacity(capacity);
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::GetUsage(size_t& usage) const noexcept
    {
        return m_impl->GetUsage(usage);
    }

    uint64_t FileBasedCompressedSecondaryCache::GetEvictedCount() const noexcept
    {
        return m_impl->GetEvictedCount();
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::Deflate(const size_t decrease) noexcept
    {
        return m_impl->Deflate(decrease);
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::Inflate(const size_t increase) noexcept
    {
        return m_impl->Inflate(increase);
    }

}

