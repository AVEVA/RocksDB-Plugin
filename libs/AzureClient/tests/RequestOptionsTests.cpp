// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/WithRequestOptions.hpp>

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <boost/asio/detached.hpp>
#include <boost/asio/use_future.hpp>

#include <future>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <utility>

namespace
{
    using AVEVA::HttpRequestOptions;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobServiceClient;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::DeleteBlobOptions;
    using AVEVA::AzureClient::WithRequestOptions;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
    using namespace std::chrono_literals;

    [[nodiscard]] HttpRequestOptions MakeRequestOptions(std::chrono::milliseconds timeout, std::uint64_t bodyLimit)
    {
        HttpRequestOptions options;
        options.SetTimeout(timeout);
        options.SetResponseBodyLimit(bodyLimit);
        return options;
    }

    constexpr auto IgnoreResult = [](auto&&) {};
} // namespace

TEST(RequestOptionsTests, ClientDefaultRequestOptionsApplyWhenNoneAreGiven)
{
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(1234ms, 77);
    BlockBlobClient client{httpClient, options};

    client.DeleteAsync(IgnoreResult);
    httpClient.Poll();

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 1234ms);
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), 77U);
    EXPECT_EQ(client.GetDefaultRequestOptions().GetTimeout(), 1234ms);
}

TEST(RequestOptionsTests, WithoutClientDefaultsTheHttpRequestOptionsDefaultsApply)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    client.DeleteAsync(IgnoreResult);
    httpClient.Poll();

    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), HttpRequestOptions{}.GetTimeout());
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), HttpRequestOptions{}.GetResponseBodyLimit());
}

TEST(RequestOptionsTests, ExplicitTrailingRequestOptionsReplaceTheClientDefault)
{
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(1234ms, 77);
    BlockBlobClient client{httpClient, options};

    client.DeleteAsync(IgnoreResult, MakeRequestOptions(55ms, 66));
    httpClient.Poll();

    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 55ms);
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), 66U);
}

TEST(RequestOptionsTests, WithRequestOptionsAndDefaultedTokenIsCoAwaitable)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{202, MakeCanonicalSuccessHeaders(), ""};
    auto options = MakeBlobClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(1234ms, 77);
    BlockBlobClient client{httpClient, options};

    bool succeeded = false;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitHasValue(&succeeded,
            [&]
    {
        return client.DeleteAsync(WithRequestOptions(MakeRequestOptions(250ms, 88)));
    }),
        boost::asio::detached);
    httpClient.Poll();

    EXPECT_TRUE(succeeded);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 250ms);
    EXPECT_EQ(httpClient.LastRequestOptions().GetResponseBodyLimit(), 88U);
}

TEST(RequestOptionsTests, WithRequestOptionsWrapsAnyTokenAndTakesPrecedenceOverTrailingArgument)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{202, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    int callbacks = 0;
    client.DeleteAsync(DeleteBlobOptions{},
        WithRequestOptions(MakeRequestOptions(300ms, 1),
            [&](auto&&)
    {
        ++callbacks;
    }),
        MakeRequestOptions(999ms, 2));
    httpClient.Poll();
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 300ms);

    auto future = client.GetPropertiesAsync(WithRequestOptions(MakeRequestOptions(400ms, 3), boost::asio::use_future));
    httpClient.Poll();
    EXPECT_EQ(future.wait_for(0s), std::future_status::ready);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 400ms);
}

TEST(RequestOptionsTests, ContainerDefaultsApplyToContainerAndChildBlobOperations)
{
    FakeHttpClient httpClient;
    auto options = MakeBlobContainerClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(777ms, 5);
    BlobContainerClient container{httpClient, options};

    container.CreateAsync(IgnoreResult);
    httpClient.Poll();
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 777ms);

    const auto blob = container.GetBlockBlobClient("child");
    blob->DeleteAsync(IgnoreResult);
    httpClient.Poll();
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 777ms);
    EXPECT_EQ(blob->GetDefaultRequestOptions().GetResponseBodyLimit(), 5U);

    container.ListBlobsAsync(WithRequestOptions(MakeRequestOptions(10ms, 6), IgnoreResult));
    httpClient.Poll();
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 10ms);
}

TEST(RequestOptionsTests, AllThreeLevelsPickWithRequestOptionsForEveryTokenForm)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{202, MakeCanonicalSuccessHeaders(), ""};
    auto options = MakeBlobClientOptions();
    options.DefaultRequestOptions = MakeRequestOptions(1ms, 1);
    BlockBlobClient client{httpClient, options};

    client.DeleteAsync(DeleteBlobOptions{},
        WithRequestOptions(MakeRequestOptions(11ms, 11), IgnoreResult),
        MakeRequestOptions(22ms, 22));
    httpClient.Poll();
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 11ms);

    auto future = client.DeleteAsync(DeleteBlobOptions{},
        WithRequestOptions(MakeRequestOptions(33ms, 33), boost::asio::use_future),
        MakeRequestOptions(44ms, 44));
    httpClient.Poll();
    EXPECT_EQ(future.wait_for(0s), std::future_status::ready);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 33ms);

    bool succeeded = false;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitHasValue(&succeeded,
            [&]
    {
        return client.DeleteAsync(DeleteBlobOptions{},
            WithRequestOptions(MakeRequestOptions(55ms, 55)),
            MakeRequestOptions(66ms, 66));
    }),
        boost::asio::detached);
    httpClient.Poll();
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 55ms);

    client.DeleteAsync(DeleteBlobOptions{}, IgnoreResult, MakeRequestOptions(77ms, 77));
    httpClient.Poll();
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), 77ms);
}

TEST(RequestOptionsTests, TokenCancellationSlotIsUsedOnlyWhenEffectiveOptionsHaveNone)
{
    {
        FakeHttpClient httpClient;
        httpClient.DeferByDefault() = true;
        BlockBlobClient client{httpClient, MakeBlobClientOptions()};
        boost::asio::cancellation_signal tokenSignal;
        bool cancelled = false;
        client.DeleteAsync(boost::asio::bind_cancellation_slot(tokenSignal.slot(),
            [&](auto result)
        {
            cancelled = !result.has_value();
        }));
        ASSERT_EQ(httpClient.PendingCount(), 1U);
        tokenSignal.emit(boost::asio::cancellation_type::terminal);
        httpClient.Poll();
        EXPECT_TRUE(cancelled);
    }
    {
        FakeHttpClient httpClient;
        httpClient.DeferByDefault() = true;
        BlockBlobClient client{httpClient, MakeBlobClientOptions()};
        boost::asio::cancellation_signal tokenSignal;
        boost::asio::cancellation_signal optionsSignal;
        HttpRequestOptions requestOptions;
        requestOptions.SetCancellationSlot(optionsSignal.slot());
        bool completed = false;
        bool failed = false;
        client.DeleteAsync(boost::asio::bind_cancellation_slot(tokenSignal.slot(),
                               [&](auto result)
        {
            completed = true;
            failed = !result.has_value();
        }),
            requestOptions);
        ASSERT_EQ(httpClient.PendingCount(), 1U);
        tokenSignal.emit(boost::asio::cancellation_type::terminal);
        httpClient.Poll();
        EXPECT_FALSE(completed);
        optionsSignal.emit(boost::asio::cancellation_type::terminal);
        httpClient.Poll();
        EXPECT_TRUE(completed);
        EXPECT_TRUE(failed);
    }
}
