// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

TEST(HttpClientConstruction, FactoryReturnsUsableClients)
{
    boost::asio::io_context context;
    bool handlerRan = false;
    boost::asio::post(context,
        [&handlerRan]
    {
        handlerRan = true;
    });

    {
        auto client = AVEVA::IHttpClient::Create(context);
        auto secondClient = AVEVA::IHttpClient::Create(context);

        ASSERT_TRUE(client);
        ASSERT_TRUE(secondClient);
        EXPECT_FALSE(handlerRan);
        EXPECT_FALSE(context.stopped());
        EXPECT_EQ(context.run(), 1);
        EXPECT_TRUE(handlerRan);
        context.restart();
    }

    EXPECT_FALSE(context.stopped());
    handlerRan = false;
    boost::asio::post(context,
        [&handlerRan]
    {
        handlerRan = true;
    });
    EXPECT_EQ(context.run(), 1);
    EXPECT_TRUE(handlerRan);
}