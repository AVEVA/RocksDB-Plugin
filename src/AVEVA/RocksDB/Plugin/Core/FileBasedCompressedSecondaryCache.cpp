// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>

#include <boost/algorithm/hex.hpp>
#include <boost/crc.hpp>
#include <boost/endian/buffers.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/scope/scope_exit.hpp>

// SSE4.2 CRC32C intrinsics (x64 only; boost::crc_32_type used as fallback).
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_AMD64))
#  include <intrin.h>
#elif defined(__SSE4_2__)
#  include <nmmintrin.h>
#endif

#include <zstd.h>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include <atomic>
#include <array>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <list>
#include <new>
#include <stdexcept>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>On-disk layout constants and binary header structure.</summary>
    struct FileFormat
    {
        /// <summary>Magic bytes written at the start of every cache entry file.</summary>
        static constexpr uint64_t magicFilePrefix = 0xFAB1C0DEBAD0C0DEULL;

        /// <summary>
        /// Sentinel stored in Header::compressionType when the payload was compressed by this
        /// cache using zstd.  Chosen from RocksDB's reserved custom-compression range
        /// (kFirstCustomCompression–kLastCustomCompression).
        /// </summary>
        static constexpr uint8_t zstdCompression = 0x80;

        /// <summary>
        /// Current on-disk format version.  The version byte in every file header must match this
        /// value; mismatches are treated as corrupt/stale entries.
        /// Version 2: checksum uses CRC32C (Castagnoli) rather than CRC32/ISO.
        /// </summary>
        static constexpr uint8_t kFileVersion = 2;

        /// <summary>
        /// Minimum payload size to attempt zstd compression.  Below this threshold zstd's
        /// per-frame overhead reliably produces output larger than the input, so the compression
        /// step is skipped entirely.
        /// </summary>
        static constexpr size_t kMinCompressibleSize = 64;

        /// <summary>
        /// On-disk header layout.  boost::endian align::no buffers have alignof==1, so the struct
        /// packs to exactly 22 bytes with no compiler padding.
        /// </summary>
        struct Header
        {
            boost::endian::little_uint64_buf_t magic;
            boost::endian::little_uint8_buf_t  version;
            boost::endian::little_uint8_buf_t  compressionType;
            boost::endian::little_uint64_buf_t dataSize;
            boost::endian::little_uint32_buf_t checksum;
        };
    };

    static_assert(
        FileFormat::zstdCompression >= static_cast<uint8_t>(rocksdb::kFirstCustomCompression) &&
        FileFormat::zstdCompression <= static_cast<uint8_t>(rocksdb::kLastCustomCompression),
        "FileFormat::zstdCompression must lie within RocksDB's custom compression range");
    static_assert(sizeof(FileFormat::Header) == sizeof(uint64_t) + 2 * sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t),
        "FileFormat::Header must be exactly 22 bytes with no padding");
    static_assert(sizeof(FileFormat::Header) == FileBasedCompressedSecondaryCache::kFileHeaderSize,
        "FileFormat::Header size must match the public kFileHeaderSize constant");

    /// <summary>Thread-local context management, compression, and decompression.</summary>
    struct ZstdCodec
    {
        struct CCtxDeleter { void operator()(ZSTD_CCtx* p) const noexcept { ZSTD_freeCCtx(p); } };
        struct DCtxDeleter { void operator()(ZSTD_DCtx* p) const noexcept { ZSTD_freeDCtx(p); } };

        /// <summary>Returns the thread-local compression context, reusing it to avoid per-call allocation overhead.</summary>
        static ZSTD_CCtx& GetCCtx()
        {
            thread_local std::unique_ptr<ZSTD_CCtx, CCtxDeleter> ctx{ ZSTD_createCCtx() };
            if (!ctx) throw std::runtime_error("zstd: failed to create CCtx");
            return *ctx;
        }

        /// <summary>Returns the thread-local decompression context, reusing it to avoid per-call allocation overhead.</summary>
        static ZSTD_DCtx& GetDCtx()
        {
            thread_local std::unique_ptr<ZSTD_DCtx, DCtxDeleter> ctx{ ZSTD_createDCtx() };
            if (!ctx) throw std::runtime_error("zstd: failed to create DCtx");
            return *ctx;
        }

        static std::string Compress(const char* data, size_t size, int level)
        {
            const size_t bound = ZSTD_compressBound(size);
            std::string out(bound, '\0');
            const size_t result = ZSTD_compressCCtx(&GetCCtx(), out.data(), bound, data, size, level);
            if (ZSTD_isError(result))
            {
                return {};
            }
            out.resize(result);
            return out;
        }

        static std::string Decompress(const char* data, size_t size)
        {
            const unsigned long long contentSize = ZSTD_getFrameContentSize(data, size);
            if (contentSize == ZSTD_CONTENTSIZE_ERROR || contentSize == ZSTD_CONTENTSIZE_UNKNOWN)
            {
                throw std::runtime_error("zstd: could not determine decompressed size");
            }
            std::string out(static_cast<size_t>(contentSize), '\0');
            const size_t written = ZSTD_decompressDCtx(&GetDCtx(), out.data(), out.size(), data, size);
            if (ZSTD_isError(written))
            {
                throw std::runtime_error(ZSTD_getErrorName(written));
            }
            out.resize(written);
            return out;
        }
    };

    /// <summary>
    /// Computes a 32-bit CRC32C (Castagnoli) checksum.
    /// On x64 the hardware path uses SSE4.2 instructions (_mm_crc32_u64/_u32/_u8) which are
    /// available on all x86-64 processors since Intel Nehalem / AMD Bulldozer (~2010).
    /// A software fallback using boost::crc_32_type is compiled for other architectures.
    /// </summary>
    struct CrcUtil
    {
        static uint32_t Compute(const char* data, size_t size) noexcept
        {
#if defined(_M_X64) || defined(_M_AMD64) || defined(__x86_64__)
            uint32_t crc = ~0u;
            const auto* p = reinterpret_cast<const uint8_t*>(data);
            while (size >= 8)
            {
                uint64_t v;
                std::memcpy(&v, p, 8);
                crc = static_cast<uint32_t>(_mm_crc32_u64(crc, v));
                p += 8; size -= 8;
            }
            if (size >= 4)
            {
                uint32_t v;
                std::memcpy(&v, p, 4);
                crc = _mm_crc32_u32(crc, v);
                p += 4; size -= 4;
            }
            while (size--)
                crc = _mm_crc32_u8(crc, *p++);
            return ~crc;
#else
            boost::crc_32_type crc;
            crc.process_bytes(data, size);
            return static_cast<uint32_t>(crc.checksum());
#endif
        }
    };

    /// <summary>Filesystem path helpers and best-effort file deletion.</summary>
    struct FileUtil
    {
        /// <summary>
        /// Returns the sharded path string for a cache entry file.  Entries are bucketed into one
        /// of 256 subdirectories (named by the first two hex digits of the filename) so that no
        /// single directory accumulates an unbounded number of entries, which degrades readdir
        /// performance on many filesystems.
        /// </summary>
        static std::string ShardedPathStr(
            const std::string& cacheDirStr, std::string_view filename)
        {
            constexpr char kSep = static_cast<char>(std::filesystem::path::preferred_separator);
            std::string result;
            if (filename.size() >= 2)
            {
                result.reserve(cacheDirStr.size() + 1 + 2 + 1 + filename.size());
                result += cacheDirStr;
                result += kSep;
                result.append(filename.data(), 2);
                result += kSep;
                result.append(filename.data(), filename.size());
            }
            else
            {
                result.reserve(cacheDirStr.size() + 1 + filename.size());
                result += cacheDirStr;
                result += kSep;
                result.append(filename.data(), filename.size());
            }
            return result;
        }

        /// <summary>Deletes a single path (no-op for empty paths).  Errors are silently ignored because this is a best-effort cache; the file may already be absent.</summary>
        static void DeleteFilePath(const std::string& path) noexcept
        {
            if (path.empty()) return;
            std::error_code ec;
            std::filesystem::remove(std::filesystem::path(path), ec);
        }

        /// <summary>Deletes every path in the vector; see DeleteFilePath for error semantics.</summary>
        static void DeleteFilePaths(const std::vector<std::string>& paths) noexcept
        {
            for (const auto& p : paths)
                DeleteFilePath(p);
        }

        /// <summary>
        /// Renames origPath to graveyardPath then deletes the graveyard file.
        /// Both operations are best-effort; errors are silently ignored.
        /// See RemoveEntryLocked for the TOCTOU trade-off: a concurrent insert for the
        /// same key may have replaced origPath between the in-memory removal (under lock)
        /// and this rename.  In that case the rename moves the new file to the graveyard,
        /// causing one cache miss on its next Lookup; Lookup's corruption-cleanup code
        /// then removes the phantom index entry.  This is acceptable for a best-effort cache.
        /// </summary>
        static void CommitEviction(const std::pair<std::string, std::string>& p) noexcept
        {
            if (p.first.empty()) return;
            std::error_code ec;
            std::filesystem::rename(
                std::filesystem::path(p.first),
                std::filesystem::path(p.second), ec);
            if (!ec)
                DeleteFilePath(p.second);
        }

        /// <summary>Commits all rename+delete pairs produced by EvictUntilSizeLocked.</summary>
        static void CommitEvictions(const std::vector<std::pair<std::string, std::string>>& pairs) noexcept
        {
            for (const auto& p : pairs)
                CommitEviction(p);
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

    FileBasedCompressedSecondaryCache::FileBasedCompressedSecondaryCache(
        std::filesystem::path cacheDir, size_t capacity, int zstdLevel)
        : m_cacheDir(std::move(cacheDir)), m_cacheDirStr(m_cacheDir.string()),
          m_capacity(capacity), m_zstdLevel(zstdLevel)
    {
        std::filesystem::remove_all(m_cacheDir);
        std::filesystem::create_directories(m_cacheDir);

        // Pre-create all 256 shard subdirectories so WriteEntry never calls
        // create_directories on the hot write path.
        const char* kHex = "0123456789abcdef";
        for (int i = 0; i < 256; ++i)
        {
            const char shard[3] = { kHex[i >> 4], kHex[i & 0xf], '\0' };
            std::filesystem::create_directories(m_cacheDir / shard);
        }
    }

    std::string_view FileBasedCompressedSecondaryCache::KeyToFilenameView(
        const rocksdb::Slice& key, std::array<char, Entry::kMaxFilenameLen>& buf) noexcept
    {
        const size_t len = key.size() * 2;
        if (len > buf.size())
            return {};
        boost::algorithm::hex_lower(key.data(), key.data() + key.size(), buf.data());
        return { buf.data(), len };
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::Insert(
        const rocksdb::Slice& key,
        rocksdb::Cache::ObjectPtr obj,
        const rocksdb::Cache::CacheItemHelper* helper,
        bool force_insert) noexcept
    {
        try
        {
            if (!helper || !helper->IsSecondaryCacheCompatible())
            {
                return rocksdb::Status::OK();
            }

            if (key.size() * 2 > Entry::kMaxFilenameLen)
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");

            const size_t dataSize = helper->size_cb(obj);
            if (dataSize == 0)
            {
                return rocksdb::Status::OK();
            }

            // small_vector keeps payloads up to 4 KiB on the stack, avoiding a heap
            // allocation for the common case of small index/filter/data blocks.
            boost::container::small_vector<char, 4096> buf(dataSize);
            auto s = helper->saveto_cb(obj, 0, dataSize, buf.data());
            if (!s.ok())
            {
                return s;
            }

            std::string compressed;
            if (dataSize >= FileFormat::kMinCompressibleSize)
                compressed = ZstdCodec::Compress(buf.data(), dataSize, m_zstdLevel);

            rocksdb::CompressionType compressionType = rocksdb::CompressionType::kNoCompression;
            const char* writeData = buf.data();
            size_t writeSize = dataSize;
            if (!compressed.empty() && compressed.size() < dataSize)
            {
                compressionType = static_cast<rocksdb::CompressionType>(FileFormat::zstdCompression);
                writeData = compressed.data();
                writeSize = compressed.size();
            }

            return WriteEntry(key, compressionType, writeData, writeSize, force_insert);
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }
    rocksdb::Status FileBasedCompressedSecondaryCache::InsertSaved(
        const rocksdb::Slice& key,
        const rocksdb::Slice& saved,
        rocksdb::CompressionType type,
        rocksdb::CacheTier /*source*/) noexcept
    {
        try
        {
            if (saved.size() == 0)
                return rocksdb::Status::OK();

            if (key.size() * 2 > Entry::kMaxFilenameLen)
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");

            if (type == rocksdb::CompressionType::kNoCompression)
            {
                std::string compressed = (saved.size() >= FileFormat::kMinCompressibleSize)
                    ? ZstdCodec::Compress(saved.data(), saved.size(), m_zstdLevel)
                    : std::string{};
                const bool useCompression = !compressed.empty() && compressed.size() < saved.size();
                return WriteEntry(key,
                    useCompression
                    ? static_cast<rocksdb::CompressionType>(FileFormat::zstdCompression)
                    : rocksdb::CompressionType::kNoCompression,
                    useCompression ? compressed.data() : saved.data(),
                    useCompression ? compressed.size() : saved.size());
            }
            return WriteEntry(key, type, saved.data(), saved.size());
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::WriteEntry(
        const rocksdb::Slice& key,
        rocksdb::CompressionType type,
        const char* data,
        size_t dataSize,
        bool force_insert) noexcept
    {
        try
        {
            if (key.size() * 2 > Entry::kMaxFilenameLen)
                return rocksdb::Status::InvalidArgument("cache key hex exceeds maximum inline buffer");
            std::array<char, Entry::kMaxFilenameLen> filenameBuf;
            const std::string_view filename = KeyToFilenameView(key, filenameBuf);
            const std::string pathStr = FileUtil::ShardedPathStr(m_cacheDirStr, filename);

            // Total bytes this entry will occupy on disk
            const size_t storedSize = dataSize + sizeof(FileFormat::Header);

            // Phase 1 (locked): capacity gate, MRU-protection of the existing entry,
            // and eviction.  The existing entry is intentionally kept in the index so
            // a write failure in Phase 2 leaves the old data accessible rather than
            // silently orphaning it on disk.  Phase 3 removes it after the rename
            // succeeds.  The lock is released before any disk I/O so concurrent
            // Lookup, Insert, and Erase calls are not blocked for the write duration.
            std::vector<std::pair<std::string, std::string>> evictPairs1;
            {
                std::lock_guard lock(m_mutex);

                auto indexIt = m_index.find(filename);
                const size_t existingSize = (indexIt != m_index.end()) ? (*indexIt)->size : 0;

                // When insertion is not forced, skip rather than evict a potentially
                // more valuable entry.  We check against the net change in usage
                // (new size minus whatever this key already occupies) so a same-key
                // update is never skipped even when the cache is exactly full.
                if (!force_insert && m_currentSize - existingSize + storedSize > m_capacity)
                    return rocksdb::Status::OK();

                // Move the existing entry to MRU so EvictUntilSizeLocked cannot
                // accidentally evict it, and so the temporary m_currentSize
                // adjustment below is not subject to double-counting.
                if (indexIt != m_index.end())
                    m_lruList.splice(m_lruList.begin(), m_lruList, *indexIt);

                // Temporarily discount the existing entry from m_currentSize so
                // eviction is sized against only the net new bytes required.
                // Restored immediately so m_currentSize stays consistent on failure.
                m_currentSize -= existingSize;
                if (m_currentSize + storedSize > m_capacity)
                    EvictUntilSizeLocked(m_capacity >= storedSize ? m_capacity - storedSize : 0, evictPairs1);
                m_currentSize += existingSize;
            }
            // Rename + delete evicted files outside the lock so concurrent readers are not blocked.
            FileUtil::CommitEvictions(evictPairs1);

            // Phase 2 (unlocked): write the entry atomically via a staging file.
            // Each call gets a unique staging path (via m_seq) so concurrent
            // writes for the same key cannot truncate each other's in-progress file.
            // Shard subdirectories are pre-created in the constructor.
            const std::string stagingPathStr =
                pathStr + "."
                    + std::to_string(m_seq.fetch_add(1, std::memory_order_relaxed)) + ".tmp";
            auto stagingCleanup = boost::scope::make_scope_exit([&] {
                std::error_code ec;
                std::filesystem::remove(std::filesystem::path(stagingPathStr), ec);
                });
            {
                std::ofstream ofs(stagingPathStr, std::ios::binary | std::ios::trunc);
                if (!ofs)
                {
                    return rocksdb::Status::IOError("Failed to open cache file for writing",
                        stagingPathStr);
                }
                // Disable the iostream internal copy buffer: our single write call goes
                // directly to the OS without an extra memcpy through the stream layer.
                ofs.rdbuf()->pubsetbuf(nullptr, 0);

                FileFormat::Header header{};
                header.magic = FileFormat::magicFilePrefix;
                header.version = FileFormat::kFileVersion;
                header.compressionType = static_cast<uint8_t>(type);
                header.dataSize = static_cast<uint64_t>(dataSize);
                header.checksum = CrcUtil::Compute(data, dataSize);

                // Combine header + payload into a single buffer for one write syscall.
                boost::container::small_vector<char, sizeof(FileFormat::Header) + 4096> writeBuf(storedSize);
                std::memcpy(writeBuf.data(), &header, sizeof(header));
                std::memcpy(writeBuf.data() + sizeof(header), data, dataSize);
                ofs.write(writeBuf.data(), static_cast<std::streamsize>(writeBuf.size()));
                if (!ofs)
                {
                    return rocksdb::Status::IOError("Failed to write cache entry data",
                        stagingPathStr);
                }
                ofs.close();
                if (!ofs)
                {
                    return rocksdb::Status::IOError("Failed to close cache entry staging file",
                        stagingPathStr);
                }
            }

            std::error_code ec;
            std::filesystem::rename(
                std::filesystem::path(stagingPathStr),
                std::filesystem::path(pathStr), ec);
            if (ec)
            {
                return rocksdb::Status::IOError("Failed to finalise cache entry file", pathStr);
            }
            stagingCleanup.set_active(false);

            // Phase 3 (locked): register the new entry at the MRU front.
            // Removes any existing entry for this key (the original entry preserved by
            // Phase 1, or a newer one written by a concurrent thread).  The file at
            // `path` was already atomically replaced by Phase 2's rename, so no file
            // deletion is needed for the displaced entry itself.
            // EvictUntilSizeLocked corrects any overshoot from concurrent writes for
            // different keys that each passed Phase 1's capacity check independently.
            std::vector<std::pair<std::string, std::string>> evictPairs3;
            {
                std::lock_guard lock(m_mutex);
                auto existingIt = m_index.find(filename);
                if (existingIt != m_index.end())
                {
                    m_currentSize -= (*existingIt)->size;
                    m_lruList.erase(*existingIt);
                    m_index.erase(existingIt);
                }
                Entry newEntry{};
                newEntry.filenameLen = static_cast<uint8_t>(filename.size());
                std::memcpy(newEntry.filenameBuf.data(), filename.data(), filename.size());
                newEntry.size = storedSize;
                m_lruList.push_front(std::move(newEntry));
                m_index.insert(m_lruList.begin());
                m_currentSize += storedSize;
                EvictUntilSizeLocked(m_capacity, evictPairs3);
            }
            FileUtil::CommitEvictions(evictPairs3);

            return rocksdb::Status::OK();
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
        FileBasedCompressedSecondaryCache::Lookup(
            const rocksdb::Slice& key,
            const rocksdb::Cache::CacheItemHelper* helper,
            rocksdb::Cache::CreateContext* create_context,
            bool /*wait*/,
            bool advise_erase,
            rocksdb::Statistics* stats,
            bool& kept_in_sec_cache) noexcept
    {
        try
        {
            kept_in_sec_cache = false;

            if (!helper || !helper->IsSecondaryCacheCompatible())
            {
                return nullptr;
            }

            if (key.size() * 2 > Entry::kMaxFilenameLen)
                return nullptr;
            std::array<char, Entry::kMaxFilenameLen> filenameBuf;
            const std::string_view filename = KeyToFilenameView(key, filenameBuf);
            const std::string pathStr = FileUtil::ShardedPathStr(m_cacheDirStr, filename);

            namespace bip = boost::interprocess;

            bip::mapped_region region;

            // Two-phase locking: Phase 1 uses shared_lock so concurrent Lookup calls
            // can map their files simultaneously.  Only the brief MRU splice in Phase 2
            // requires exclusive access.
            bool entryRemovedByAdvise = false;
            bool mappingFailed = false;

            // Phase 1 (shared_lock): index presence check + file mapping.
            {
                std::shared_lock<std::shared_mutex> rdLock(m_mutex);

                if (m_index.find(filename) == m_index.end())
                {
                    return nullptr;
                }

                // Map the file while holding the shared lock so the mapping is
                // established before any concurrent writer can evict and delete it.
                // file_mapping can be destroyed immediately after creating the region;
                // mapped_region retains its own OS-level view handle independently.
                try
                {
                    bip::file_mapping fm(pathStr.c_str(), bip::read_only);
                    region = bip::mapped_region(fm, bip::read_only);
                }
                catch (const bip::interprocess_exception&)
                {
                    mappingFailed = true;
                }
            } // shared_lock always released here

            // Remove corrupt/missing entry under exclusive lock before returning.
            if (mappingFailed)
            {
                std::pair<std::string, std::string> evictPair;
                {
                    std::lock_guard exLock(m_mutex);
                    auto reIt = m_index.find(filename);
                    if (reIt != m_index.end())
                        evictPair = RemoveEntryLocked(*reIt);
                }
                FileUtil::CommitEviction(evictPair);
                return nullptr;
            }

            // Phase 2 (unique_lock): MRU splice + optional erase.
            // Re-validate: a concurrent eviction may have removed the entry between
            // Phase 1 and here.  Treat as a miss rather than returning data from the
            // now-orphaned mapping.
            std::pair<std::string, std::string> advisedPair;
            {
                std::lock_guard exLock(m_mutex);

                auto indexIt = m_index.find(filename);
                if (indexIt == m_index.end())
                {
                    return nullptr;
                }

                // Splice the entry to the MRU front of the list in O(1).
                m_lruList.splice(m_lruList.begin(), m_lruList, *indexIt);

                if (advise_erase && SupportForceErase())
                {
                    advisedPair = RemoveEntryLocked(*indexIt);
                    entryRemovedByAdvise = true;
                }
            }
            FileUtil::CommitEviction(advisedPair);

            // Remove the index entry on any validation failure below
            // file is not repeatedly re-mapped and re-validated on future lookups.
            // Skip when advise_erase already removed the entry from the index.
            bool entryIsValid = false;
            auto corruptionCleanup = boost::scope::make_scope_exit([&] noexcept {
                if (!entryIsValid && !entryRemovedByAdvise)
                {
                    try
                    {
                        std::pair<std::string, std::string> evictPair;
                        {
                            std::lock_guard cleanupLock(m_mutex);
                            auto it = m_index.find(filename);
                            if (it != m_index.end())
                                evictPair = RemoveEntryLocked(*it);
                        }
                        FileUtil::CommitEviction(evictPair);
                    }
                    catch (...) {}
                }
                });

            const auto* mapped = static_cast<const char*>(region.get_address());
            const size_t mappedSize = region.get_size();

            // Validate header.
            if (mappedSize < sizeof(FileFormat::Header))
            {
                return nullptr;
            }

            FileFormat::Header header;
            std::memcpy(&header, mapped, sizeof(header));

            if (header.magic.value() != FileFormat::magicFilePrefix)
            {
                return nullptr;
            }

            if (header.version.value() != FileFormat::kFileVersion)
            {
                return nullptr;
            }

            const auto compressionType = static_cast<rocksdb::CompressionType>(header.compressionType.value());
            const size_t dataSize = static_cast<size_t>(header.dataSize.value());

            if (dataSize + sizeof(FileFormat::Header) > mappedSize)
            {
                return nullptr;
            }

            const char* dataPtr = mapped + sizeof(FileFormat::Header);

            if (CrcUtil::Compute(dataPtr, dataSize) != header.checksum.value())
            {
                return nullptr;
            }

            std::string decompressed;
            rocksdb::Slice dataSlice;
            rocksdb::CompressionType effectiveCompressionType;

            if (static_cast<uint8_t>(compressionType) == FileFormat::zstdCompression)
            {
                try
                {
                    decompressed = ZstdCodec::Decompress(dataPtr, dataSize);
                }
                catch (const std::runtime_error&)
                {
                    return nullptr;
                }
                dataSlice = rocksdb::Slice(decompressed.data(), decompressed.size());
                effectiveCompressionType = rocksdb::CompressionType::kNoCompression;
            }
            else
            {
                dataSlice = rocksdb::Slice(dataPtr, dataSize);
                effectiveCompressionType = compressionType;
            }

            rocksdb::Cache::ObjectPtr outObj = nullptr;
            size_t outCharge = 0;
            auto s = helper->create_cb(dataSlice, effectiveCompressionType,
                rocksdb::CacheTier::kNonVolatileBlockTier,
                create_context, /*allocator=*/nullptr,
                &outObj, &outCharge);
            if (!s.ok() || outObj == nullptr)
            {
                return nullptr;
            }

            // Guard outObj so it is freed if ResultHandle allocation (heap) throws.
            auto objCleanup = boost::scope::make_scope_exit([&] noexcept {
                if (outObj)
                    helper->del_cb(outObj, /*allocator=*/nullptr);
                });

            // Report the hit to RocksDB's statistics subsystem.
            if (stats)
            {
                stats->recordTick(rocksdb::SECONDARY_CACHE_HITS);
                switch (helper->role)
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

            entryIsValid = true;
            kept_in_sec_cache = !entryRemovedByAdvise;
            auto result = std::make_unique<ResultHandle>(outObj, outCharge);
            objCleanup.set_active(false); // ResultHandle now owns outObj
            return result;
        }
        catch (...)
        {
            return nullptr;
        }
    }

    void FileBasedCompressedSecondaryCache::Erase(const rocksdb::Slice& key) noexcept
    {
        try
        {
            if (key.size() * 2 > Entry::kMaxFilenameLen)
                return;
            std::array<char, Entry::kMaxFilenameLen> filenameBuf;
            const std::string_view filename = KeyToFilenameView(key, filenameBuf);
            std::pair<std::string, std::string> evictPair;
            {
                std::lock_guard lock(m_mutex);
                auto indexIt = m_index.find(filename);
                if (indexIt != m_index.end())
                    evictPair = RemoveEntryLocked(*indexIt);
            }
            FileUtil::CommitEviction(evictPair);
        }
        catch (...) {}
    }

    void FileBasedCompressedSecondaryCache::WaitAll(
        std::vector<rocksdb::SecondaryCacheResultHandle*> /*handles*/) noexcept
    {
        // All handles are immediately ready; nothing to wait on.
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::SetCapacity(size_t capacity) noexcept
    {
        try
        {
            std::vector<std::pair<std::string, std::string>> evictPairs;
            {
                std::lock_guard lock(m_mutex);
                m_capacity = capacity;
                EvictUntilSizeLocked(m_capacity, evictPairs);
            }
            FileUtil::CommitEvictions(evictPairs);
            return rocksdb::Status::OK();
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::GetCapacity(size_t& capacity) noexcept
    {
        try
        {
            std::shared_lock lock(m_mutex);
            capacity = m_capacity;
            return rocksdb::Status::OK();
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::GetUsage(size_t& usage) const noexcept
    {
        try
        {
            std::shared_lock lock(m_mutex);
            usage = m_currentSize;
            return rocksdb::Status::OK();
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::Deflate(size_t decrease) noexcept
    {
        try
        {
            std::vector<std::pair<std::string, std::string>> evictPairs;
            {
                std::lock_guard lock(m_mutex);
                m_capacity -= std::min(decrease, m_capacity);
                EvictUntilSizeLocked(m_capacity, evictPairs);
            }
            FileUtil::CommitEvictions(evictPairs);
            return rocksdb::Status::OK();
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    rocksdb::Status FileBasedCompressedSecondaryCache::Inflate(size_t increase) noexcept
    {
        try
        {
            std::lock_guard lock(m_mutex);
            // Saturating add: clamp to size_t max rather than wrapping silently.
            const size_t remaining = std::numeric_limits<size_t>::max() - m_capacity;
            m_capacity += (increase > remaining) ? remaining : increase;
            return rocksdb::Status::OK();
        }
        catch (...)
        {
            return StatusUtil::CurrentExceptionToStatus();
        }
    }

    void FileBasedCompressedSecondaryCache::EvictUntilSizeLocked(
        size_t targetSize, std::vector<std::pair<std::string, std::string>>& evictPairs)
    {
        while (m_currentSize > targetSize && !m_lruList.empty())
        {
            evictPairs.push_back(RemoveEntryLocked(std::prev(m_lruList.end())));
            m_evictedCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    std::pair<std::string, std::string>
        FileBasedCompressedSecondaryCache::RemoveEntryLocked(LruIterator it)
    {
        // Build the orig and graveyard path strings while under the lock so that the
        // unique graveyard name is reserved before any concurrent writer can claim the
        // same sequence number.  No filesystem operation is performed here; the caller
        // must call FileUtil::CommitEviction on the returned pair after releasing the
        // lock.  Deferring the rename outside the lock allows bulk evictions to release
        // the mutex sooner, at the cost of a narrow TOCTOU window where a concurrent
        // insert for the same key could have its file moved to the graveyard (causing
        // one cache miss; the phantom index entry is then cleaned up by Lookup).
        const std::string origPathStr = FileUtil::ShardedPathStr(m_cacheDirStr, it->FilenameView());
        const std::string graveyardPathStr =
            origPathStr + "." +
                std::to_string(m_seq.fetch_add(1, std::memory_order_relaxed)) + ".del";
        m_currentSize -= it->size;
        m_index.erase(it);
        m_lruList.erase(it);
        return { origPathStr, graveyardPathStr };
    }
}
