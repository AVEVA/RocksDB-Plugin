// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <rocksdb/advanced_options.h>

#include <cstdint>
#include <string>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>Thread-local context management, compression, and decompression.</summary>
    struct ZstdCodec
    {
        static std::string Compress(const char* data, size_t size, int level);
        static std::string Decompress(const char* data, size_t size);

        struct CompressResult
        {
            /// <summary>Effective compression type written to disk.</summary>
            rocksdb::CompressionType type;
            /// <summary>Points into <c>storage</c> when compression was applied, otherwise the original input pointer.</summary>
            const char* data;
            /// <summary>Size of <c>data</c> in bytes.</summary>
            size_t size;
            /// <summary>Non-empty only when compression was applied.</summary>
            std::string storage;
        };

        /// <param name="minCompressibleSize">Payloads smaller than this are stored uncompressed.</param>
        /// <param name="zstdCompressionByte">The <c>CompressionType</c> byte written to disk when zstd is applied.</param>
        static CompressResult MaybeCompress(const char* data, size_t size, int zstdLevel,
            size_t minCompressibleSize, uint8_t zstdCompressionByte);
    };
}
