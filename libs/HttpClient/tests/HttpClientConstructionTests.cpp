// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>

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
TEST(HttpClientConstruction, LastOwnerReleasedOnAWriterThreadDoesNotSelfJoin) {
    boost::asio::io_context context;
    std::atomic<int> done{0};
    constexpr int Queued = 50;
    for (const bool viaBoundCompletion : {false, true}) {
        done = 0;
        auto client = AVEVA::HttpClient::Create(context);
        const auto writer = client->GetDownloadWriter();
        boost::asio::post(writer, [&] { ++done; });
        for (int i = 0; i < Queued; ++i) {
            boost::asio::post(writer, [&] { ++done; });
        }
        auto release = [owner = std::move(client), &done]() mutable {
            owner.reset();
            ++done;
        };
        if (viaBoundCompletion) {
            // A completion bound to the writer executor runs there, like an HTTP handler would.
            boost::asio::post(context, boost::asio::bind_executor(writer, std::move(release)));
            context.restart();
            context.run();
        } else {
            boost::asio::post(writer, std::move(release));
        }
        // The helper thread joins the pool, so every queued task still runs before the client goes away.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (done.load() < Queued + 2 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_EQ(done.load(), Queued + 2);
    }
}
TEST(HttpClientConstruction, DownloadWriterRunsManyTasksOnABoundedClientOwnedPool) {
    boost::asio::io_context context;
    std::mutex mutex;
    std::set<std::thread::id> threads;
    std::atomic<int> done{0};
    constexpr int Tasks = 200;
    {
        AVEVA::HttpClient client(context);
        for (int i = 0; i < Tasks; ++i) {
            boost::asio::post(client.GetDownloadWriter(), [&] {
                {
                    const std::scoped_lock lock(mutex);
                    threads.insert(std::this_thread::get_id());
                }
                ++done;
            });
        }
        // Destruction joins the pool, so every queued task has run by then.
    }
    EXPECT_EQ(done.load(), Tasks);
    EXPECT_LE(threads.size(), 4U);
    EXPECT_FALSE(threads.contains(std::this_thread::get_id()));
}