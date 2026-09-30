// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Factory and options behavior that is independent of the backend.

#include "SecondaryCacheTestSupport.hpp"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

using namespace AVEVA::RocksDB::Plugin::Core;
using namespace AVEVA::RocksDB::Plugin::Core::Testing;

TEST(SecondaryCacheFactoryTests, FileBasedBackendCreatesFileBasedCache) {
    const auto dir = MakeUniqueTempDir("factory");
    auto cache = CreateSecondaryCache(MakeTestOptions(SecondaryCacheBackend::FileBased, dir));
    ASSERT_NE(cache, nullptr);
    EXPECT_STREQ(cache->Name(), "FileBasedCompressedSecondaryCache");
    cache.reset();
    std::filesystem::remove_all(dir);
}

TEST(SecondaryCacheFactoryTests, DefaultBackendPrefersCacheLibWhenAvailable) {
    const auto dir = MakeUniqueTempDir("factory");
    auto cache = CreateSecondaryCache(MakeTestOptions(SecondaryCacheBackend::Default, dir));
    ASSERT_NE(cache, nullptr);
    if (IsCacheLibSecondaryCacheAvailable()) {
        EXPECT_STREQ(cache->Name(), "CacheLibSecondaryCache");
    } else {
        EXPECT_STREQ(cache->Name(), "FileBasedCompressedSecondaryCache");
    }
    cache.reset();
    std::filesystem::remove_all(dir);
}

TEST(SecondaryCacheFactoryTests, CacheLibBackendThrowsWhenUnavailable) {
    if (IsCacheLibSecondaryCacheAvailable()) {
        GTEST_SKIP() << "CacheLib is available in this build";
    }
    const auto dir = MakeUniqueTempDir("factory");
    EXPECT_THROW((void)CreateSecondaryCache(MakeTestOptions(SecondaryCacheBackend::CacheLib, dir)),
                 std::invalid_argument);
    std::filesystem::remove_all(dir);
}

TEST(SecondaryCacheFactoryTests, EmptyCacheDirIsRejected) {
    SecondaryCacheOptions options;
    options.backend = SecondaryCacheBackend::FileBased;
    options.logger = MakeQuietLogger();
    EXPECT_THROW((void)CreateSecondaryCache(options), std::invalid_argument);
}
