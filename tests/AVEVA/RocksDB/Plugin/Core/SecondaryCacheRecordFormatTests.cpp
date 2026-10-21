// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "SecondaryCache/RecordFormat.hpp"

#include <gtest/gtest.h>

#include <array>
#include <vector>

namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;

namespace {
std::vector<std::byte> ToBytes(const std::string& value) {
    return std::vector<std::byte>(reinterpret_cast<const std::byte*>(value.data()),
                                  reinterpret_cast<const std::byte*>(value.data() + value.size()));
}
} // namespace

TEST(SecondaryCacheRecordFormatTests, EncodeDecodeRoundTrips) {
    const auto key = ToBytes("record-key");
    const auto payload = ToBytes("record-payload");
    std::vector<std::byte> buffer(Secondary::RecordSize(key.size(), payload.size()));
    ASSERT_TRUE(Secondary::EncodeRecord(buffer, key, payload, rocksdb::CompressionType::kZSTD,
                                        rocksdb::CacheTier::kNonVolatileBlockTier, 42));

    Secondary::DecodedRecordView decoded{};
    const auto result = Secondary::DecodeRecord(buffer, key, decoded);
    ASSERT_EQ(result, Secondary::DecodeResult::kOk);
    EXPECT_EQ(decoded.compressionType, rocksdb::CompressionType::kZSTD);
    EXPECT_EQ(decoded.sourceTier, rocksdb::CacheTier::kNonVolatileBlockTier);
    EXPECT_EQ(decoded.keyHash, 42u);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(decoded.payload.data()), decoded.payload.size()),
              "record-payload");
}

TEST(SecondaryCacheRecordFormatTests, CorruptionIsDetectedInValidationOrder) {
    const auto key = ToBytes("abc");
    const auto payload = ToBytes("payload");
    std::vector<std::byte> buffer(Secondary::RecordSize(key.size(), payload.size()));
    ASSERT_TRUE(Secondary::EncodeRecord(buffer, key, payload, rocksdb::CompressionType::kNoCompression,
                                        rocksdb::CacheTier::kVolatileTier, 7));

    auto mutated = buffer;
    mutated[0] = std::byte{0};
    Secondary::DecodedRecordView decoded{};
    EXPECT_EQ(Secondary::DecodeRecord(mutated, key, decoded), Secondary::DecodeResult::kBadMagic);

    mutated = buffer;
    auto* header = reinterpret_cast<Secondary::RecordHeader*>(mutated.data());
    header->formatVersion = 9;
    EXPECT_EQ(Secondary::DecodeRecord(mutated, key, decoded), Secondary::DecodeResult::kBadVersion);

    mutated = buffer;
    header = reinterpret_cast<Secondary::RecordHeader*>(mutated.data());
    header->keyLen = 999999;
    EXPECT_EQ(Secondary::DecodeRecord(mutated, key, decoded), Secondary::DecodeResult::kBadLength);

    mutated = buffer;
    mutated[sizeof(Secondary::RecordHeader)] ^= std::byte{0x1};
    EXPECT_EQ(Secondary::DecodeRecord(mutated, key, decoded), Secondary::DecodeResult::kBadHeaderCrc);

    mutated = buffer;
    EXPECT_EQ(Secondary::DecodeRecord(mutated, ToBytes("abd"), decoded), Secondary::DecodeResult::kKeyMismatch);

    mutated = buffer;
    mutated[sizeof(Secondary::RecordHeader) + key.size()] ^= std::byte{0x1};
    EXPECT_EQ(Secondary::DecodeRecord(mutated, key, decoded), Secondary::DecodeResult::kBadPayloadCrc);
}

TEST(SecondaryCacheRecordFormatTests, SizeArithmeticRejectsOverflowAndEncoderRejectsWrongBufferSize) {
    EXPECT_EQ(Secondary::RecordSize(SIZE_MAX, 1), 0u);
    EXPECT_EQ(Secondary::RecordSize(1, SIZE_MAX), 0u);
    EXPECT_EQ(Secondary::RecordSize(0, 0), Secondary::kRecordAlign);
    const auto key = ToBytes("key");
    const auto payload = ToBytes("value");
    std::vector<std::byte> small(16);
    EXPECT_FALSE(
        Secondary::EncodeRecord(small, key, payload, rocksdb::kNoCompression, rocksdb::CacheTier::kVolatileTier, 0));
}

TEST(SecondaryCacheRecordFormatTests, EmptyKeyAndPayloadPermitNullSpans) {
    std::array<std::byte, Secondary::kRecordAlign> encoded{};
    ASSERT_TRUE(
        Secondary::EncodeRecord(encoded, {}, {}, rocksdb::kNoCompression, rocksdb::CacheTier::kVolatileTier, 0));
    Secondary::DecodedRecordView decoded;
    ASSERT_EQ(Secondary::DecodeRecord(encoded, {}, decoded), Secondary::DecodeResult::kOk);
    EXPECT_TRUE(decoded.payload.empty());
}
