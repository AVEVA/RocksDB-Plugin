// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "RecordFormat.hpp"

#include <algorithm>
#include <array>
#include <cstring>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
namespace {
constexpr uint32_t kCrc32cPolynomial = 0x82f63b78u;

constexpr std::array<uint32_t, 256> BuildCrc32cTable() noexcept {
    std::array<uint32_t, 256> table{};
    for (size_t i = 0; i < table.size(); ++i) {
        uint32_t crc = static_cast<uint32_t>(i);
        for (size_t bit = 0; bit < 8; ++bit) {
            crc = (crc & 1u) != 0u ? (crc >> 1u) ^ kCrc32cPolynomial : (crc >> 1u);
        }
        table[i] = crc;
    }
    return table;
}

constexpr std::array<uint32_t, 256> kCrc32cTable = BuildCrc32cTable();
constexpr size_t kHeaderCrcPrefixBytes = offsetof(RecordHeader, headerCrc);
} // namespace

uint32_t ComputeCrc32c(const std::span<const std::byte> bytes, uint32_t seed) noexcept {
    uint32_t crc = ~seed;
    for (const std::byte value : bytes) {
        const auto index = static_cast<uint8_t>(crc ^ static_cast<uint8_t>(value));
        crc = (crc >> 8u) ^ kCrc32cTable[index];
    }
    return ~crc;
}

void EncodeRecord(const std::span<std::byte> dst, const std::span<const std::byte> key,
                  const std::span<const std::byte> payload, const rocksdb::CompressionType type,
                  const rocksdb::CacheTier tier, const uint64_t keyHash, const RecordFlags flags) noexcept {
    const size_t requiredSize = RecordSize(key.size(), payload.size());
    if (dst.size() < requiredSize) {
        return;
    }

    std::fill(dst.begin(), dst.begin() + static_cast<std::ptrdiff_t>(requiredSize), std::byte{0});

    RecordHeader header{};
    header.magic = kRecordMagic;
    header.formatVersion = kFormatVersion;
    header.compression = static_cast<uint8_t>(type);
    header.sourceTier = static_cast<uint8_t>(tier);
    header.flags = static_cast<uint8_t>(flags);
    header.keyHash = keyHash;
    header.keyLen = static_cast<uint32_t>(key.size());
    header.payloadLen = static_cast<uint32_t>(payload.size());
    header.payloadCrc = ComputeCrc32c(payload);

    std::memcpy(dst.data(), &header, sizeof(header));
    std::memcpy(dst.data() + static_cast<std::ptrdiff_t>(sizeof(RecordHeader)), key.data(), key.size());
    std::memcpy(dst.data() + static_cast<std::ptrdiff_t>(sizeof(RecordHeader) + key.size()), payload.data(),
                payload.size());

    header.headerCrc = ComputeCrc32c(
        std::span<const std::byte>(dst.data(), dst.data() + static_cast<std::ptrdiff_t>(kHeaderCrcPrefixBytes)));
    header.headerCrc = ComputeCrc32c(
        std::span<const std::byte>(dst.data() + static_cast<std::ptrdiff_t>(sizeof(RecordHeader)),
                                   dst.data() + static_cast<std::ptrdiff_t>(sizeof(RecordHeader) + key.size())),
        header.headerCrc);
    std::memcpy(dst.data(), &header, sizeof(header));
}

bool TryReadHeader(const std::span<const std::byte> src, RecordHeader& headerOut) noexcept {
    if (src.size() < sizeof(RecordHeader)) {
        return false;
    }
    std::memcpy(&headerOut, src.data(), sizeof(RecordHeader));
    return true;
}

size_t RecordSize(const RecordHeader& header) noexcept { return RecordSize(header.keyLen, header.payloadLen); }

DecodeResult DecodeRecord(const std::span<const std::byte> src, const std::span<const std::byte> expectedKey,
                          DecodedRecordView& decodedOut) noexcept {
    RecordHeader header{};
    if (!TryReadHeader(src, header)) {
        return DecodeResult::kBadLength;
    }

    if (header.magic != kRecordMagic) {
        return DecodeResult::kBadMagic;
    }
    if (header.formatVersion != kFormatVersion) {
        return DecodeResult::kBadVersion;
    }

    const size_t alignedSize = RecordSize(header);
    const size_t rawSize = sizeof(RecordHeader) + static_cast<size_t>(header.keyLen) + static_cast<size_t>(header.payloadLen);
    if (rawSize < sizeof(RecordHeader) || alignedSize > src.size()) {
        return DecodeResult::kBadLength;
    }

    const auto headerPrefix =
        std::span<const std::byte>(src.data(), src.data() + static_cast<std::ptrdiff_t>(kHeaderCrcPrefixBytes));
    const auto keyBytes =
        std::span<const std::byte>(src.data() + static_cast<std::ptrdiff_t>(sizeof(RecordHeader)), header.keyLen);

    uint32_t headerCrc = ComputeCrc32c(headerPrefix);
    headerCrc = ComputeCrc32c(keyBytes, headerCrc);
    if (headerCrc != header.headerCrc) {
        return DecodeResult::kBadHeaderCrc;
    }

    if (keyBytes.size() != expectedKey.size() ||
        !std::equal(keyBytes.begin(), keyBytes.end(), expectedKey.begin(), expectedKey.end())) {
        return DecodeResult::kKeyMismatch;
    }

    const auto payloadBytes = std::span<const std::byte>(
        src.data() + static_cast<std::ptrdiff_t>(sizeof(RecordHeader) + header.keyLen), header.payloadLen);
    if (ComputeCrc32c(payloadBytes) != header.payloadCrc) {
        return DecodeResult::kBadPayloadCrc;
    }

    decodedOut.payload = payloadBytes;
    decodedOut.compressionType = static_cast<rocksdb::CompressionType>(header.compression);
    decodedOut.sourceTier = static_cast<rocksdb::CacheTier>(header.sourceTier);
    decodedOut.flags = static_cast<RecordFlags>(header.flags);
    decodedOut.keyHash = header.keyHash;
    return DecodeResult::kOk;
}
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
