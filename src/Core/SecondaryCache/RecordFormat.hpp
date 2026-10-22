// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <rocksdb/advanced_options.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
/// First header field of every record ('AVC1' in little-endian byte order).
inline constexpr uint32_t kRecordMagic = 0x31435641u;
/// On-disk layout version; records with another value decode as kBadVersion.
inline constexpr uint8_t kFormatVersion = 1;
/// Records start and end on this boundary; Location stores offsets and lengths in these units.
inline constexpr size_t kRecordAlign = 64;
/// Typical device page size. Unused by the current buffered-I/O implementation.
inline constexpr size_t kDeviceBlock = 4096;

/// Per-record flag bits stored in the header; none are defined in format version 1.
enum class RecordFlags : uint8_t {
    kNone = 0,
};

#pragma pack(push, 1)
/// <summary>
/// Packed 32-byte record header in host byte order (the cache is never shared across machines or restarts). It is
/// followed by the key bytes, the payload bytes and zero padding up to kRecordAlign.
/// </summary>
struct RecordHeader {
    /// kRecordMagic.
    uint32_t magic;
    /// kFormatVersion.
    uint8_t formatVersion;
    /// rocksdb::CompressionType of the payload.
    uint8_t compression;
    /// rocksdb::CacheTier the payload came from.
    uint8_t sourceTier;
    /// RecordFlags bits.
    uint8_t flags;
    /// Key fingerprint used by the index; covered by headerCrc.
    uint64_t keyHash;
    /// Key bytes following the header.
    uint32_t keyLen;
    /// Payload bytes following the key.
    uint32_t payloadLen;
    /// CRC-32C of the payload bytes.
    uint32_t payloadCrc;
    /// CRC-32C of all header bytes before this field, continued over the key bytes.
    uint32_t headerCrc;
};
#pragma pack(pop)
static_assert(sizeof(RecordHeader) == 32);

/// Outcome of DecodeRecord. Failures are listed in check order, except that a buffer shorter than a header is
/// reported as kBadLength before any other check.
enum class DecodeResult : uint8_t {
    /// Structural/CRC checks passed and the full key matches; see DecodeRecord for unchecked fields.
    kOk,
    /// Bytes do not start with kRecordMagic.
    kBadMagic,
    /// Unknown format version.
    kBadVersion,
    /// Buffer is shorter than a header or differs from the size implied by the header.
    kBadLength,
    /// Header or key bytes are corrupt.
    kBadHeaderCrc,
    /// Header/key CRC passed but the key differs; payload CRC has not been checked.
    kKeyMismatch,
    /// Payload bytes are corrupt.
    kBadPayloadCrc,
};

/// <summary>
/// Fields of a successfully decoded record. payload aliases the source buffer and is valid only while that buffer
/// is alive and unmodified.
/// </summary>
struct DecodedRecordView {
    /// Payload bytes inside the source buffer.
    std::span<const std::byte> payload;
    /// Header compression; passed to create_cb.
    rocksdb::CompressionType compressionType{rocksdb::CompressionType::kNoCompression};
    /// Header source tier; passed to create_cb.
    rocksdb::CacheTier sourceTier{rocksdb::CacheTier::kVolatileTier};
    /// Header flag bits.
    RecordFlags flags{RecordFlags::kNone};
    /// Stored fingerprint; DecodeRecord compares the full key instead.
    uint64_t keyHash{0};
};

/// <summary>Computes the encoded size: header + key + payload, rounded up to kRecordAlign.</summary>
/// <param name="keyLen">Key length in bytes.</param>
/// <param name="payloadLen">Payload length in bytes.</param>
/// <returns>Encoded size in bytes, or 0 if a length exceeds UINT32_MAX or the total overflows size_t.</returns>
[[nodiscard]] constexpr size_t RecordSize(const size_t keyLen, const size_t payloadLen) noexcept {
    constexpr auto maximum = std::numeric_limits<size_t>::max();
    if (keyLen > std::numeric_limits<uint32_t>::max() || payloadLen > std::numeric_limits<uint32_t>::max() ||
        keyLen > maximum - sizeof(RecordHeader) - (kRecordAlign - 1) ||
        payloadLen > maximum - sizeof(RecordHeader) - (kRecordAlign - 1) - keyLen) {
        return 0;
    }
    const size_t rawSize = sizeof(RecordHeader) + keyLen + payloadLen;
    return (rawSize + kRecordAlign - 1) & ~(kRecordAlign - 1);
}

/// <summary>Computes CRC-32C (Castagnoli) with a bytewise table.</summary>
/// <param name="bytes">Data to checksum.</param>
/// <param name="seed">CRC of the preceding bytes to continue a running checksum, or 0 to start a new one.</param>
/// <returns>The CRC-32C of the preceding bytes followed by bytes.</returns>
[[nodiscard]] uint32_t ComputeCrc32c(std::span<const std::byte> bytes, uint32_t seed = 0) noexcept;

/// <summary>Writes a complete record, including both CRCs, and zero-fills the alignment pad.</summary>
/// <param name="dst">[out] Destination; its size must equal RecordSize(key.size(), payload.size()).</param>
/// <param name="key">Full cache key; must not overlap dst.</param>
/// <param name="payload">Serialized value; must not overlap dst.</param>
/// <param name="type">Compression of payload.</param>
/// <param name="tier">Tier payload came from.</param>
/// <param name="keyHash">Key fingerprint to store; callers pass the same hash they index by.</param>
/// <param name="flags">Header flag bits.</param>
/// <returns>False, leaving dst untouched, if the size is unrepresentable or dst has the wrong size.</returns>
[[nodiscard]] bool EncodeRecord(std::span<std::byte> dst, std::span<const std::byte> key,
                                std::span<const std::byte> payload, rocksdb::CompressionType type,
                                rocksdb::CacheTier tier, uint64_t keyHash,
                                RecordFlags flags = RecordFlags::kNone) noexcept;

/// <summary>Copies the packed header if src is long enough; does not validate header fields.</summary>
/// <param name="src">Record bytes.</param>
/// <param name="headerOut">[out] Receives the header on success; unchanged on failure.</param>
/// <returns>False if src is shorter than a header.</returns>
[[nodiscard]] bool TryReadHeader(std::span<const std::byte> src, RecordHeader& headerOut) noexcept;

/// <param name="header">Header to measure; not validated.</param>
/// <returns>The aligned record size implied by the header's lengths, or 0 if unrepresentable.</returns>
[[nodiscard]] size_t RecordSize(const RecordHeader& header) noexcept;

/// <summary>
/// Validates a record using the required order:
/// magic/version -> bounds -> header CRC -> key comparison -> payload CRC.
/// </summary>
/// <param name="src">Exactly one encoded record; its size must match the aligned size in its header.</param>
/// <param name="expectedKey">Key the caller looked up; compared in full.</param>
/// <param name="decodedOut">[out] Filled only on kOk; its payload aliases src.</param>
/// <returns>kOk, or the first check that failed.</returns>
/// <remarks>
/// Does not validate padding, compression/tier/flag enum values, or whether keyHash is the expected fingerprint.
/// Full-key comparison, rather than keyHash, establishes the key match.
/// </remarks>
[[nodiscard]] DecodeResult DecodeRecord(std::span<const std::byte> src, std::span<const std::byte> expectedKey,
                                        DecodedRecordView& decodedOut) noexcept;
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
