// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"

#include <gtest/gtest.h>

#include <cctype>
#include <string>

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
