// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"

#include <rocksdb/file_system.h>

#include <gtest/gtest.h>

#include <memory>

using AVEVA::RocksDB::Plugin::Azure::BlobFilesystem;

TEST(BlobFilesystemTests, SupportedOpsAdvertisesOnlyAsyncIO) {
    BlobFilesystem filesystem(rocksdb::FileSystem::Default(), nullptr, nullptr);

    int64_t supported = 0;
    filesystem.SupportedOps(supported);

    EXPECT_EQ(supported, int64_t{1} << rocksdb::FSSupportedOps::kAsyncIO);
}

TEST(BlobFilesystemTests, AreFilesSameComparesNormalizedPaths) {
    BlobFilesystem filesystem(rocksdb::FileSystem::Default(), nullptr, nullptr);
    bool same = false;
    ASSERT_TRUE(filesystem.AreFilesSame("db//a.sst", "db/a.sst", {}, &same, nullptr).ok());
    EXPECT_TRUE(same);
    ASSERT_TRUE(filesystem.AreFilesSame("db\\a.sst", "db/a.sst", {}, &same, nullptr).ok());
    EXPECT_TRUE(same);
    ASSERT_TRUE(filesystem.AreFilesSame("db/a.sst", "db/b.sst", {}, &same, nullptr).ok());
    EXPECT_FALSE(same);
}

TEST(BlobFilesystemTests, TruncateRejectsSizesBeyondInt64) {
    BlobFilesystem filesystem(rocksdb::FileSystem::Default(), nullptr, nullptr);
    const auto status = filesystem.Truncate("db/a.sst", static_cast<size_t>(INT64_MAX) + 1, {}, nullptr);
    EXPECT_TRUE(status.IsInvalidArgument());
}