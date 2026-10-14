// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"

#include <gtest/gtest.h>
#include <rocksdb/convenience.h>
#include <rocksdb/file_system.h>
#include <rocksdb/utilities/object_registry.h>

#include <boost/log/sources/severity_logger.hpp>

#include <cctype>
#include <memory>
#include <string>

#if defined(_MSC_VER)
#define DEPRECATED_CALL_BEGIN __pragma(warning(push)) __pragma(warning(disable : 4996))
#define DEPRECATED_CALL_END __pragma(warning(pop))
#else
#define DEPRECATED_CALL_BEGIN                                                                                          \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#define DEPRECATED_CALL_END _Pragma("GCC diagnostic pop")
#endif

using AVEVA::RocksDB::Plugin::Azure::Plugin;
using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

namespace {
ServicePrincipalStorageInfo MakeInfo(const std::string& dbName, const std::string& account = "myaccount") {
    return ServicePrincipalStorageInfo{dbName, "https://" + account + ".blob.core.windows.net", "client", "secret",
                                       "tenant"};
}
} // namespace

TEST(PluginTests, NameFor_StartsWithPluginNameAndIsHexEncoded) {
    const auto name = Plugin::NameFor(MakeInfo("container"));

    ASSERT_EQ(0U, name.find(std::string(Plugin::Name) + "-"));
    for (const char ch : name.substr(Plugin::Name.size() + 1)) {
        EXPECT_TRUE(std::isxdigit(static_cast<unsigned char>(ch))) << ch;
    }
}

TEST(PluginTests, NameFor_DistinguishesAccountsDatabasesAndBackup) {
    const auto base = Plugin::NameFor(MakeInfo("db"));

    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db2")));
    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db", "other")));
    EXPECT_NE(base, Plugin::NameFor(MakeInfo("db"), MakeInfo("db")));
    EXPECT_EQ(base, Plugin::NameFor(MakeInfo("db")));
}

// The pre-io_context overload still compiles, owns its io_context, and publishes the old registry name too.
TEST(PluginTests, DeprecatedRegister_WithoutIoContext_PublishesBothNamesAndShutsDownCleanly) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;

    {
        DEPRECATED_CALL_BEGIN
        [[maybe_unused]] const auto status =
            Plugin::Register(configOptions, &env, &guard, MakeInfo("legacy-db"), MakeInfo("legacy-backup"), logger);
        DEPRECATED_CALL_END
    }

    auto* library = rocksdb::ObjectLibrary::Default().get();
    EXPECT_NE(nullptr, library->FindFactory<rocksdb::FileSystem>(
                           Plugin::NameFor(MakeInfo("legacy-db"), MakeInfo("legacy-backup"))));
    EXPECT_NE(nullptr, library->FindFactory<rocksdb::FileSystem>(std::string(Plugin::Name) + "legacy-dblegacy-backup"));
    // Destroying the Env must not hang or touch a stopped io_context.
    guard.reset();
}
