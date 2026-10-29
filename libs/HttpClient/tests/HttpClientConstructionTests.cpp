// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <fstream>
#include <stdexcept>
#include <string>

TEST(HttpClientConstruction, BadTrustSettingsFailConstructionWithAClearError) {
    boost::asio::io_context context;
    const auto messageOf = [&context](AVEVA::HttpClientOptions options) {
        try {
            auto client = AVEVA::HttpClient::Create(context, std::move(options));
        } catch (const std::runtime_error& error) {
            return std::string(error.what());
        }
        return std::string{};
    };

    AVEVA::HttpClientOptions missingFile;
    missingFile.SetCaFile("this-ca-file-does-not-exist.pem");
    EXPECT_NE(messageOf(missingFile).find("CA file"), std::string::npos);

    AVEVA::HttpClientOptions missingDirectory;
    missingDirectory.SetCaDirectory("this-ca-directory-does-not-exist");
    EXPECT_NE(messageOf(missingDirectory).find("CA directory"), std::string::npos);

    AVEVA::HttpClientOptions garbagePem;
    garbagePem.SetCaPem("this is not a certificate");
    EXPECT_NE(messageOf(garbagePem).find("CA PEM"), std::string::npos);

    const HttpClientTests::TempFile notPem("aveva-http-client-bad-ca-");
    {
        std::ofstream(notPem.Path()) << "not a pem file";
    }
    AVEVA::HttpClientOptions garbageFile;
    garbageFile.SetCaFile(notPem.Path().string());
    EXPECT_NE(messageOf(garbageFile).find("CA file"), std::string::npos);

    // Without peer verification the trust settings are never used, so they are not checked.
    AVEVA::HttpClientOptions unverified = missingFile;
    unverified.SetVerifyPeer(false);
    EXPECT_TRUE(messageOf(unverified).empty());
}

TEST(HttpClientConstruction, FactoryReturnsUsableClients) {
    boost::asio::io_context context;
    bool handlerRan = false;
    boost::asio::post(context, [&handlerRan] { handlerRan = true; });

    {
        auto client = AVEVA::HttpClient::Create(context);
        auto secondClient = AVEVA::HttpClient::Create(context);

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
    boost::asio::post(context, [&handlerRan] { handlerRan = true; });
    EXPECT_EQ(context.run(), 1);
    EXPECT_TRUE(handlerRan);
}
