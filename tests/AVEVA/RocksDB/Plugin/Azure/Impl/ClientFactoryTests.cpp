// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.hpp"

#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory::AccountNameFromUrl;

TEST(ClientFactoryTests, AccountNameFromUrl_HostStyleEndpoint_UsesTheFirstHostLabel) {
    EXPECT_EQ("acct", AccountNameFromUrl("https://acct.blob.core.windows.net"));
    EXPECT_EQ("acct", AccountNameFromUrl("https://acct.blob.core.windows.net/"));
    EXPECT_EQ("acct", AccountNameFromUrl("https://acct.blob.core.windows.net:443/container"));
}

TEST(ClientFactoryTests, AccountNameFromUrl_PathStyleEndpoint_UsesTheFirstPathSegment) {
    EXPECT_EQ("devstoreaccount1", AccountNameFromUrl("http://127.0.0.1:10000/devstoreaccount1"));
    EXPECT_EQ("devstoreaccount1", AccountNameFromUrl("http://localhost:10000/devstoreaccount1/"));
    EXPECT_EQ("devstoreaccount1", AccountNameFromUrl("http://azurite:10000/devstoreaccount1/container"));
    EXPECT_EQ("devstoreaccount1", AccountNameFromUrl("http://[::1]:10000/devstoreaccount1"));
}

TEST(ClientFactoryTests, AccountNameFromUrl_PathStyleHostWithoutAPath_FallsBackToTheHost) {
    EXPECT_EQ("localhost", AccountNameFromUrl("http://localhost:10000"));
}
