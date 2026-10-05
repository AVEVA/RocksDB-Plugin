// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"

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
