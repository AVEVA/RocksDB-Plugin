// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <rocksdb/advanced_options.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
inline constexpr uint32_t kRecordMagic = 0x31435641u; // 'AVC1'
inline constexpr uint8_t kFormatVersion = 1;
inline constexpr size_t kRecordAlign = 64;
inline constexpr size_t kDeviceBlock = 4096;

enum class RecordFlags : uint8_t {
    kNone = 0,
    kTombstone = 1u << 0,
};

#pragma pack(push, 1)
struct RecordHeader {
    uint32_t magic;
    uint8_t formatVersion;
    uint8_t compression;
    uint8_t sourceTier;
    uint8_t flags;
    uint64_t keyHash;
    uint32_t keyLen;
    uint32_t payloadLen;
    uint32_t payloadCrc;
    uint32_t headerCrc;
};
#pragma pack(pop)
static_assert(sizeof(RecordHeader) == 32);

enum class DecodeResult : uint8_t {
    kOk,
    kBadMagic,
    kBadVersion,
    kBadLength,
    kBadHeaderCrc,
    kKeyMismatch,
    kBadPayloadCrc,
};

struct DecodedRecordView {
    std::span<const std::byte> payload;
    rocksdb::CompressionType compressionType{rocksdb::CompressionType::kNoCompression};
    rocksdb::CacheTier sourceTier{rocksdb::CacheTier::kVolatileTier};
    RecordFlags flags{RecordFlags::kNone};
    uint64_t keyHash{0};
};

[[nodiscard]] constexpr size_t RecordSize(const size_t keyLen, const size_t payloadLen) noexcept {
    const size_t rawSize = sizeof(RecordHeader) + keyLen + payloadLen;
    return (rawSize + kRecordAlign - 1) & ~(kRecordAlign - 1);
}

[[nodiscard]] uint32_t ComputeCrc32c(std::span<const std::byte> bytes, uint32_t seed = 0) noexcept;

/// <summary>Writes a record and zero-fills any alignment pad.</summary>
void EncodeRecord(std::span<std::byte> dst, std::span<const std::byte> key, std::span<const std::byte> payload,
                  rocksdb::CompressionType type, rocksdb::CacheTier tier, uint64_t keyHash,
                  RecordFlags flags = RecordFlags::kNone) noexcept;

/// <summary>Reads the packed header without validating checksums.</summary>
[[nodiscard]] bool TryReadHeader(std::span<const std::byte> src, RecordHeader& headerOut) noexcept;

/// <summary>Returns the aligned record size implied by a header.</summary>
[[nodiscard]] size_t RecordSize(const RecordHeader& header) noexcept;

/// <summary>
/// Validates a record using the required order:
/// magic/version -> bounds -> header CRC -> key comparison -> payload CRC.
/// </summary>
[[nodiscard]] DecodeResult DecodeRecord(std::span<const std::byte> src, std::span<const std::byte> expectedKey,
                                        DecodedRecordView& decodedOut) noexcept;
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
