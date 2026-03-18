// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <rocksdb/advanced_options.h>
#include <boost/endian/buffers.hpp>

#include <cstdint>
#include <cstddef>

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
        /// Version 3: checksum covers compressionType + dataSize + payload (previously payload only).
        /// </summary>
        static constexpr uint8_t kFileVersion = 3;

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
            boost::endian::little_uint8_buf_t version;
            boost::endian::little_uint8_buf_t compressionType;
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
}
