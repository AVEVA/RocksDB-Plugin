// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "ZstdCodec.hpp"

#include <zstd.h>

#include <memory>
#include <stdexcept>

namespace AVEVA::RocksDB::Plugin::Core
{
    namespace
    {
        struct CCtxDeleter { void operator()(ZSTD_CCtx* p) const noexcept { ZSTD_freeCCtx(p); } };
        struct DCtxDeleter { void operator()(ZSTD_DCtx* p) const noexcept { ZSTD_freeDCtx(p); } };

        /// <summary>Returns the thread-local compression context, reusing it to avoid per-call allocation overhead.</summary>
        ZSTD_CCtx& GetCCtx()
        {
            thread_local std::unique_ptr<ZSTD_CCtx, CCtxDeleter> ctx{ ZSTD_createCCtx() };
            if (!ctx) throw std::runtime_error("zstd: failed to create CCtx");
            return *ctx;
        }

        /// <summary>Returns the thread-local decompression context, reusing it to avoid per-call allocation overhead.</summary>
        ZSTD_DCtx& GetDCtx()
        {
            thread_local std::unique_ptr<ZSTD_DCtx, DCtxDeleter> ctx{ ZSTD_createDCtx() };
            if (!ctx) throw std::runtime_error("zstd: failed to create DCtx");
            return *ctx;
        }
    }

    std::string ZstdCodec::Compress(const char* data, size_t size, int level)
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

    std::string ZstdCodec::Decompress(const char* data, size_t size)
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

    ZstdCodec::CompressResult ZstdCodec::MaybeCompress(
        const char* data, size_t size, int zstdLevel,
        size_t minCompressibleSize, uint8_t zstdCompressionByte)
    {
        if (size >= minCompressibleSize)
        {
            std::string compressed = Compress(data, size, zstdLevel);
            if (!compressed.empty() && compressed.size() < size)
            {
                CompressResult result;
                result.type = static_cast<rocksdb::CompressionType>(zstdCompressionByte);
                result.storage = std::move(compressed);
                result.data = result.storage.data();
                result.size = result.storage.size();
                return result;
            }
        }
        return { rocksdb::CompressionType::kNoCompression, data, size, {} };
    }
}
