// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/SequentialFile.hpp"
#include "consumer/MainApiConsumer.hpp"

#include <gtest/gtest.h>
#include <rocksdb/convenience.h>

#include <type_traits>

namespace {
using namespace AVEVA::RocksDB::Plugin::ConsumerFixtures;
}

// Calls written against main (no io_context) still resolve to the deprecated overloads. The empty account URL makes\n//
// filesystem creation fail fast instead of talking to the network; the status proves the call was dispatched.
TEST(SourceCompatTests, MainStyleRegistration_CompilesAndReachesTheDeprecatedOverloads) {
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;

    const auto servicePrincipal = RegisterServicePrincipalLikeMain(configOptions, &env, &guard, "");
    const auto chained = RegisterChainedCredentialLikeMain(configOptions, &env, &guard, "");

    EXPECT_TRUE(servicePrincipal.IsInvalidArgument()) << servicePrincipal.ToString();
    EXPECT_TRUE(chained.IsInvalidArgument()) << chained.ToString();
    guard.reset();
}

// Instantiates the file-usage fixture so a signature change in the RocksDB-facing interface fails to compile.
TEST(SourceCompatTests, MainStyleFileUsage_Compiles) {
    static_assert(std::is_invocable_r_v<rocksdb::IOStatus, decltype(&UseFilesLikeMain), rocksdb::FileSystem&>);
    SUCCEED();
}

// Intentional breaks documented in README.md ("Source and binary compatibility"). If one of these starts to compile
// again, update the note: it is a deliberate boundary, not an accident.
TEST(SourceCompatTests, IntentionalBreaks_AreEnforced) {
    using AVEVA::RocksDB::Plugin::Azure::ReadableFile;
    using AVEVA::RocksDB::Plugin::Azure::SequentialFile;
    // ReadableFile is random access only; sequential reads moved to SequentialFile.
    static_assert(!std::is_base_of_v<rocksdb::FSSequentialFile, ReadableFile>);
    static_assert(std::is_base_of_v<rocksdb::FSSequentialFile, SequentialFile>);
    // The file classes are created only by BlobFilesystem; consumers get them from rocksdb::FileSystem.
    static_assert(
        !std::is_constructible_v<ReadableFile, std::shared_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl>>);
    static_assert(!std::is_default_constructible_v<ReadableFile>);
    SUCCEED();
}
