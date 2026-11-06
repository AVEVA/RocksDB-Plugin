// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "consumer/MainApiConsumer.hpp"

#include <gtest/gtest.h>
#include <rocksdb/convenience.h>

#include <type_traits>

namespace {
using namespace AVEVA::RocksDB::Plugin::ConsumerFixtures;
}

// The empty account URL makes filesystem creation fail fast instead of talking to the network; the status proves the
// call was dispatched.
TEST(SourceCompatTests, Registration_CompilesAndReachesTheSingleRegisterEntryPoint) {
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;
    boost::asio::io_context ioContext;

    const auto servicePrincipal = RegisterServicePrincipalLikeMain(configOptions, &env, &guard, ioContext, "");
    const auto chained = RegisterChainedCredentialLikeMain(configOptions, &env, &guard, ioContext, "");

    EXPECT_TRUE(servicePrincipal.IsInvalidArgument()) << servicePrincipal.ToString();
    EXPECT_TRUE(chained.IsInvalidArgument()) << chained.ToString();
    guard.reset();
    UnregisterLikeMain("");
}

// Instantiates the file-usage fixture so a signature change in the RocksDB-facing interface fails to compile.
TEST(SourceCompatTests, MainStyleFileUsage_Compiles) {
    static_assert(std::is_invocable_r_v<rocksdb::IOStatus, decltype(&UseFilesLikeMain), rocksdb::FileSystem&>);
    SUCCEED();
}
