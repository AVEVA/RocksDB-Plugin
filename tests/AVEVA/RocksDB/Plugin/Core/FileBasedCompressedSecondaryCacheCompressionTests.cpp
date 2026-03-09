// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

// --------------------------------------------------------------------------
// Insert compresses compressible data — the on-disk file must be smaller
// than the uncompressed payload plus the 21-byte file header
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertCompressesCompressibleData)
{
    const std::string keyStr = "compress_size_key";
    // 1 KiB of identical bytes — highly compressible.
    const std::string raw(1024, 'A');
    TestPayload payload{raw};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // 8 magic + 1 version + 1 compressionType + 8 dataSize + 4 checksum = 22 bytes
    constexpr std::uintmax_t kFileHeaderSize = 8 + 1 + 1 + 8 + 4;
    const std::uintmax_t fileSize = std::filesystem::file_size(filePath);

    EXPECT_LT(fileSize, kFileHeaderSize + raw.size())
        << "Expected compressed file (" << fileSize << " B) to be smaller than "
        << "header + uncompressed payload (" << kFileHeaderSize + raw.size() << " B)";

    // Round-trip must still produce the original data.
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}

// --------------------------------------------------------------------------
// InsertSaved with kNoCompression compresses compressible data — on-disk file
// must be smaller than header + raw payload size
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSavedWithNoCompressionCompressesCompressibleData)
{
    const std::string keyStr = "insertsaved_compress_key";
    // 1 KiB of identical bytes — highly compressible.
    const std::string raw(1024, 'B');
    const rocksdb::Slice saved(raw);

    auto s = m_cache->InsertSaved(MakeKey(keyStr), saved,
                                   rocksdb::CompressionType::kNoCompression,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // 8 magic + 1 version + 1 compressionType + 8 dataSize + 4 checksum = 22 bytes
    constexpr std::uintmax_t kFileHeaderSize = 8 + 1 + 1 + 8 + 4;
    const std::uintmax_t fileSize = std::filesystem::file_size(filePath);

    EXPECT_LT(fileSize, kFileHeaderSize + raw.size())
        << "InsertSaved with kNoCompression should compress compressible data; "
        << "file (" << fileSize << " B) must be smaller than header + raw payload ("
        << kFileHeaderSize + raw.size() << " B)";

    // Round-trip must still produce the original data.
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}

// --------------------------------------------------------------------------
// A non-default zstd level is accepted and produces a correct round-trip
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CustomZstdLevel_RoundTrips)
{
    // Use level 9 (high compression) instead of the default level 1.
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(
        m_cacheDir,
        m_fs,
        FileBasedCompressedSecondaryCache::kDefaultCapacity,
        /*zstdLevel=*/9);

    // 1 KiB of repeated bytes — highly compressible, exercises the compression path.
    const std::string raw(1024, 'X');
    TestPayload payload{raw};

    ASSERT_TRUE(m_cache->Insert(MakeKey("zstd_level9_key"), &payload, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("zstd_level9_key"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(kept);

    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}
