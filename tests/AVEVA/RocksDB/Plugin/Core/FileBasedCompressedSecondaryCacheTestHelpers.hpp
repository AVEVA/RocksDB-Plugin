// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "Mocks/FilesystemMock.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>
#include <rocksdb/status.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <boost/algorithm/hex.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/core.hpp>

#include <array>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache;
using AVEVA::RocksDB::Plugin::Core::Mocks::FilesystemMock;
using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace
{
    // Returns a severity_logger_mt that discards all log records.
    // Use this wherever a non-null logger is required but log output is unwanted.
    inline std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>
    MakeNullLogger()
    {
        return std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    }

    // A simple heap-allocated payload object used by the test helper callbacks.
    struct TestPayload
    {
        std::string data;
    };

    size_t TestSizeCb(rocksdb::Cache::ObjectPtr obj)
    {
        return static_cast<TestPayload*>(obj)->data.size();
    }

    rocksdb::Status TestSaveToCb(rocksdb::Cache::ObjectPtr obj,
                                  size_t from_offset,
                                  size_t length,
                                  char* out_buf)
    {
        const auto* payload = static_cast<TestPayload*>(obj);
        if (from_offset + length > payload->data.size())
            return rocksdb::Status::InvalidArgument("out of range");
        std::memcpy(out_buf, payload->data.data() + from_offset, length);
        return rocksdb::Status::OK();
    }

    rocksdb::Status TestCreateCb(const rocksdb::Slice& data,
                                  rocksdb::CompressionType /*type*/,
                                  rocksdb::CacheTier /*source*/,
                                  rocksdb::Cache::CreateContext* /*ctx*/,
                                  rocksdb::MemoryAllocator* /*alloc*/,
                                  rocksdb::Cache::ObjectPtr* out_obj,
                                  size_t* out_charge)
    {
        auto* payload = new TestPayload{std::string(data.data(), data.size())};
        *out_obj = payload;
        *out_charge = payload->data.size();
        return rocksdb::Status::OK();
    }

    void TestDeleteCb(rocksdb::Cache::ObjectPtr obj, rocksdb::MemoryAllocator*)
    {
        delete static_cast<TestPayload*>(obj);
    }

    // Returns a temporary directory that is removed when the test fixture tears down.
    std::filesystem::path MakeTempDir(const std::string& suffix)
    {
        auto base = std::filesystem::temp_directory_path() /
                    ("aveva_sec_cache_test_" + suffix);
        std::filesystem::remove_all(base);
        std::filesystem::create_directories(base);
        return base;
    }

    // Counts .del graveyard files remaining under cacheDir.
    // RemoveEntryLocked renames evicted files to .del under the lock and the
    // caller deletes them after releasing it.  By the time the mutating
    // operation (Insert, Erase, SetCapacity, …) returns, this must be zero.
    [[maybe_unused]] size_t CountGraveyardFiles(const std::filesystem::path& cacheDir)
    {
        size_t count = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(cacheDir))
            if (entry.is_regular_file() && entry.path().extension() == ".del")
                ++count;
        return count;
    }

    // Records the CompressionType argument received by the last call to
    // CapturingCreateCb.  Reset to kNoCompression before each use.
    [[maybe_unused]] rocksdb::CompressionType g_capturedCompressionType =
        rocksdb::CompressionType::kNoCompression;

    [[maybe_unused]] rocksdb::Status CapturingCreateCb(
        const rocksdb::Slice& data,
        rocksdb::CompressionType type,
        rocksdb::CacheTier /*source*/,
        rocksdb::Cache::CreateContext* /*ctx*/,
        rocksdb::MemoryAllocator* /*alloc*/,
        rocksdb::Cache::ObjectPtr* out_obj,
        size_t* out_charge)
    {
        g_capturedCompressionType = type;
        auto* payload = new TestPayload{std::string(data.data(), data.size())};
        *out_obj = payload;
        *out_charge = payload->data.size();
        return rocksdb::Status::OK();
    }
} // namespace

// ---------------------------------------------------------------------------
// Primary fixture: exercises real filesystem I/O via LocalFilesystem.
// ---------------------------------------------------------------------------
class FileBasedCompressedSecondaryCacheTests : public ::testing::Test
{
protected:
    std::filesystem::path m_cacheDir;
    std::shared_ptr<AVEVA::RocksDB::Plugin::Core::LocalFilesystem> m_fs;
    std::unique_ptr<FileBasedCompressedSecondaryCache> m_cache;

    // A helper without secondary-cache support, required as the
    // without_secondary_compat back-pointer for m_helper.
    rocksdb::Cache::CacheItemHelper m_helperNoSec{
        rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};

    // A helper with secondary-cache support.  without_secondary_compat must
    // point to a valid no-secondary helper with the same role and deleter.
    rocksdb::Cache::CacheItemHelper m_helper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        TestSaveToCb,
        TestCreateCb,
        &m_helperNoSec};

    void SetUp() override
    {
        m_cacheDir = MakeTempDir(::testing::UnitTest::GetInstance()->current_test_info()->name());
        m_fs = std::make_shared<AVEVA::RocksDB::Plugin::Core::LocalFilesystem>();
        m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs,
            FileBasedCompressedSecondaryCache::kDefaultCapacity,
            FileBasedCompressedSecondaryCache::kDefaultZstdLevel,
            MakeNullLogger());
    }

    void TearDown() override
    {
        m_cache.reset();
        std::filesystem::remove_all(m_cacheDir);
    }

    static rocksdb::Slice MakeKey(const std::string& s) { return rocksdb::Slice(s); }
};

// ---------------------------------------------------------------------------
// Mock fixture: injects I/O failures via NiceMock<FilesystemMock>.
// NiceMock suppresses warnings for unexpected calls; the default bool return
// is false and the default unique_ptr return is nullptr — both useful here.
// ---------------------------------------------------------------------------
class FileBasedCompressedSecondaryCacheMockTests : public ::testing::Test
{
protected:
    std::filesystem::path m_cacheDir;
    std::shared_ptr<NiceMock<FilesystemMock>> m_mockFs;

    rocksdb::Cache::CacheItemHelper m_helperNoSec{
        rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};
    rocksdb::Cache::CacheItemHelper m_helper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        TestSaveToCb,
        TestCreateCb,
        &m_helperNoSec};

    void SetUp() override
    {
        m_cacheDir = std::filesystem::temp_directory_path() / "aveva_sec_cache_mock_test";
        m_mockFs = std::make_shared<NiceMock<FilesystemMock>>();
        // The FileBasedCompressedSecondaryCache constructor calls DeleteDir and
        // CreateDir (×257) but ignores their return values, so the NiceMock
        // default of false is harmless.
    }

    static rocksdb::Slice MakeKey(const std::string& s) { return rocksdb::Slice(s); }
};
