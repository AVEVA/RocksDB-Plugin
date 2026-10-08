// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestHelpers.hpp"

#include <AVEVA/AzureClient/ITokenCredential.hpp>

#include <cstddef>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace
{
    using AVEVA::AzureClient::AccessToken;
    using AVEVA::AzureClient::CachingTokenCredential;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::MakeToken;
    using AVEVA::AzureClient::Tests::ScriptedTokenCredential;

    void PrimeTokenRequest(const std::shared_ptr<CachingTokenCredential>& credential, std::vector<std::string> scopes)
    {
        credential->GetTokenAsync(std::move(scopes), [](std::error_code, const AccessToken&) {});
    }

    void RequestTokenCapturingValue(const std::shared_ptr<CachingTokenCredential>& credential,
        std::vector<std::string> scopes,
        std::string& receivedToken,
        CallbackExpectation* callback = nullptr)
    {
        credential->GetTokenAsync(std::move(scopes),
            [&](std::error_code error, AccessToken token)
        {
            EXPECT_FALSE(error);
            receivedToken = std::move(token.Token);
            if (callback != nullptr)
            {
                callback->MarkInvoked();
            }
        });
    }

    void RequestTokenAppendingValue(const std::shared_ptr<CachingTokenCredential>& credential,
        std::vector<std::string> scopes,
        std::vector<std::string>& receivedTokens,
        CallbackExpectation& callback)
    {
        credential->GetTokenAsync(std::move(scopes),
            [&](std::error_code error, AccessToken token)
        {
            EXPECT_FALSE(error);
            receivedTokens.push_back(std::move(token.Token));
            callback.MarkInvoked();
        });
    }

    void RequestWaitersForSharedToken(const std::shared_ptr<CachingTokenCredential>& credential,
        int waiterCount,
        std::vector<std::string>& receivedTokens,
        std::vector<CallbackExpectation>& callbacks)
    {
        for (int i = 0; i < waiterCount; ++i)
        {
            credential->GetTokenAsync({"scope-a"},
                [&, i](std::error_code error, AccessToken token)
            {
                EXPECT_FALSE(error);
                receivedTokens.push_back(std::move(token.Token));
                callbacks.at(static_cast<std::size_t>(i)).MarkInvoked();
            });
        }
    }
} // namespace

TEST(ITokenCredentialTests, CachingTokenCredential_ReusesCachedTokenOutsideRefreshWindow)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    std::vector<std::string> receivedTokens;
    CallbackExpectation firstCallback;
    CallbackExpectation secondCallback;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
    {
        EXPECT_FALSE(error);
        receivedTokens.push_back(std::move(token.Token));
        firstCallback.MarkInvoked();
    });
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
    {
        EXPECT_FALSE(error);
        receivedTokens.push_back(std::move(token.Token));
        secondCallback.MarkInvoked();
    });

    EXPECT_EQ(inner->CallCount(), 1);
    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"cached-token", "cached-token"}));
}

TEST(ITokenCredentialTests, CachingTokenCredential_ServesCachedTokenAndRefreshesInBackgroundInsideRefreshWindow)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("old-token", std::chrono::seconds(30)));
    inner->EnqueueDeferred({}, MakeToken("new-token", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    PrimeTokenRequest(credential, {"scope-a"});

    // The token is still valid but inside the refresh window: the caller gets it immediately and
    // exactly one background refresh starts.
    std::string inWindowToken;
    CallbackExpectation inWindowCallback;
    RequestTokenCapturingValue(credential, {"scope-a"}, inWindowToken, &inWindowCallback);
    EXPECT_EQ(inWindowToken, "old-token");
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);

    EXPECT_TRUE(inner->CompleteNext());

    std::string refreshedToken;
    CallbackExpectation refreshedCallback;
    RequestTokenCapturingValue(credential, {"scope-a"}, refreshedToken, &refreshedCallback);
    EXPECT_EQ(refreshedToken, "new-token");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(ITokenCredentialTests, CachingTokenCredential_TokenShorterThanRefreshWindowIsServedBeforeRefreshCompletes)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("short-lived", std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("refreshed", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    PrimeTokenRequest(credential, {"scope-a"});

    std::string receivedToken;
    CallbackExpectation callback;
    RequestTokenCapturingValue(credential, {"scope-a"}, receivedToken, &callback);
    EXPECT_EQ(receivedToken, "short-lived");
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);

    EXPECT_TRUE(inner->CompleteNext());

    std::string refreshedToken;
    CallbackExpectation refreshedCallback;
    RequestTokenCapturingValue(credential, {"scope-a"}, refreshedToken, &refreshedCallback);
    EXPECT_EQ(refreshedToken, "refreshed");
}

TEST(ITokenCredentialTests, CachingTokenCredential_ExpiredTokenWaitsForTheRefresh)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("expired-token", -std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("new-token", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});

    std::string receivedToken;
    CallbackExpectation callback;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
    {
        EXPECT_FALSE(error);
        receivedToken = std::move(token.Token);
        callback.MarkInvoked();
    });
    EXPECT_TRUE(receivedToken.empty());
    EXPECT_EQ(inner->PendingCount(), 1U);

    EXPECT_TRUE(inner->CompleteNext());
    EXPECT_EQ(receivedToken, "new-token");
}

TEST(ITokenCredentialTests, CachingTokenCredential_InvalidatesCacheWhenScopesChange)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("token-b", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    std::string secondToken;
    PrimeTokenRequest(credential, {"scope-a"});
    CallbackExpectation callback;
    RequestTokenCapturingValue(credential, {"scope-b"}, secondToken, &callback);

    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(secondToken, "token-b");
    ASSERT_EQ(inner->RequestedScopes().size(), 2U);
    EXPECT_EQ(inner->RequestedScopes().at(0), (std::vector<std::string>{"scope-a"}));
    EXPECT_EQ(inner->RequestedScopes().at(1), (std::vector<std::string>{"scope-b"}));
}

TEST(ITokenCredentialTests, CachingTokenCredential_RefreshFailureDoesNotDiscardPreviousScopeCache)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    std::error_code refreshError;
    std::string cachedTokenAfterFailure;
    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});
    CallbackExpectation failedRefresh;
    CallbackExpectation cachedRead;
    credential->GetTokenAsync({"scope-b"},
        [&](std::error_code error, const AVEVA::AzureClient::AccessToken&)
    {
        refreshError = error;
        failedRefresh.MarkInvoked();
    });
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
    {
        EXPECT_FALSE(error);
        cachedTokenAfterFailure = std::move(token.Token);
        cachedRead.MarkInvoked();
    });

    EXPECT_EQ(refreshError, std::make_error_code(std::errc::permission_denied));
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(cachedTokenAfterFailure, "token-a");
}

TEST(ITokenCredentialTests, CachingTokenCredential_ServesStaleTokenDuringRefreshBackoff)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::seconds(30)));
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});

    // In the refresh window: served from cache while the background refresh fails, which starts
    // the failed-refresh backoff instead of surfacing the error to a caller with a valid token.
    std::error_code inWindowError = std::make_error_code(std::errc::io_error);
    std::string inWindowToken;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
    {
        inWindowError = error;
        inWindowToken = std::move(token.Token);
    });

    std::string staleToken;
    CallbackExpectation callback;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
    {
        EXPECT_FALSE(error);
        staleToken = std::move(token.Token);
        callback.MarkInvoked();
    });

    EXPECT_FALSE(inWindowError);
    EXPECT_EQ(inWindowToken, "token-a");
    EXPECT_EQ(staleToken, "token-a");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(ITokenCredentialTests, CachingTokenCredential_ConcurrentInWindowCallersShareOneBackgroundRefresh)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("old-token", std::chrono::seconds(10)));
    inner->EnqueueDeferred({}, MakeToken("refreshed-token", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    std::vector<std::string> receivedTokens;
    PrimeTokenRequest(credential, {"scope-a"});
    CallbackExpectation firstCallback;
    CallbackExpectation secondCallback;
    RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, firstCallback);
    RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, secondCallback);

    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"old-token", "old-token"}));
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(inner->CompleteNext());
}

TEST(ITokenCredentialTests, CachingTokenCredential_CoalescesConcurrentCallersWithoutAValidToken)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("expired-token", -std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("refreshed-token", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    std::vector<std::string> receivedTokens;
    PrimeTokenRequest(credential, {"scope-a"});
    CallbackExpectation firstCallback;
    CallbackExpectation secondCallback;
    RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, firstCallback);
    RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, secondCallback);

    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(receivedTokens.empty());

    EXPECT_TRUE(inner->CompleteNext());

    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"refreshed-token", "refreshed-token"}));
}

TEST(ITokenCredentialTests, CachingTokenCredential_NullInnerCompletesAllConcurrentWaiters)
{
    auto credential = CachingTokenCredential::Create(nullptr, std::chrono::minutes(5));

    std::error_code firstError;
    std::error_code secondError;
    CallbackExpectation firstCallback;
    CallbackExpectation secondCallback;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, const AVEVA::AzureClient::AccessToken&)
    {
        firstError = error;
        firstCallback.MarkInvoked();
    });
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, const AVEVA::AzureClient::AccessToken&)
    {
        secondError = error;
        secondCallback.MarkInvoked();
    });

    EXPECT_EQ(firstError, std::make_error_code(std::errc::operation_not_permitted));
    EXPECT_EQ(secondError, std::make_error_code(std::errc::operation_not_permitted));
}

TEST(ITokenCredentialTests, CachingTokenCredential_NullInnerResetsInFlightStateForSubsequentCalls)
{
    auto credential = CachingTokenCredential::Create(nullptr, std::chrono::minutes(5));

    std::error_code firstError;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, const AVEVA::AzureClient::AccessToken&)
    {
        firstError = error;
    });
    EXPECT_EQ(firstError, std::make_error_code(std::errc::operation_not_permitted));

    // Without the fix, m_inFlightRefresh would still point at the first (dead) refresh state,
    // and this second call with matching scopes would hang forever instead of completing.
    std::error_code secondError;
    CallbackExpectation secondCallback;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, const AVEVA::AzureClient::AccessToken&)
    {
        secondError = error;
        secondCallback.MarkInvoked();
    });

    EXPECT_EQ(secondError, std::make_error_code(std::errc::operation_not_permitted));
}

TEST(ITokenCredentialTests, CachingTokenCredential_FanOutDeliversSameTokenToAllWaiters)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("shared-token", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    constexpr int WaiterCount = 5;
    std::vector<std::string> receivedTokens;
    std::vector<CallbackExpectation> callbacks(WaiterCount);
    RequestWaitersForSharedToken(credential, WaiterCount, receivedTokens, callbacks);

    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(inner->CompleteNext());

    ASSERT_EQ(receivedTokens.size(), static_cast<std::size_t>(WaiterCount));
    for (const auto& token : receivedTokens)
    {
        EXPECT_EQ(token, "shared-token");
    }
}

TEST(ITokenCredentialTests, CachingTokenCredential_CompletesOutstandingRequestAfterDestruction)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("late-token", std::chrono::hours(1)));

    std::weak_ptr<CachingTokenCredential> weakCredential;
    std::string receivedToken;
    CallbackExpectation callback;

    {
        auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));
        weakCredential = credential;
        credential->GetTokenAsync({"scope-a"},
            [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
        {
            EXPECT_FALSE(error);
            receivedToken = std::move(token.Token);
            callback.MarkInvoked();
        });
        credential.reset();
    }

    EXPECT_TRUE(weakCredential.expired());
    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(inner->CompleteNext());

    EXPECT_EQ(receivedToken, "late-token");
}

TEST(ITokenCredentialTests, CachingTokenCredential_DeliversToCoalescedWaitersAfterDestruction)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("late-token", std::chrono::hours(1)));
    std::vector<std::string> receivedTokens;
    CallbackExpectation first;
    CallbackExpectation second;
    {
        auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));
        RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, first);
        RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, second);
    }

    EXPECT_EQ(inner->CallCount(), 1);
    EXPECT_TRUE(inner->CompleteNext());
    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"late-token", "late-token"}));
}

TEST(ITokenCredentialTests, CachingTokenCredential_DoesNotDeadlockWhenHandlerReentersCacheHitPath)
{
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = CachingTokenCredential::Create(inner, std::chrono::minutes(5));

    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});

    std::vector<std::string> tokens;
    CallbackExpectation outerCallback;
    CallbackExpectation innerCallback;
    credential->GetTokenAsync({"scope-a"},
        [&](std::error_code error, AVEVA::AzureClient::AccessToken token)
    {
        EXPECT_FALSE(error);
        tokens.push_back(std::move(token.Token));
        credential->GetTokenAsync({"scope-a"},
            [&](std::error_code nestedError, AVEVA::AzureClient::AccessToken nestedToken)
        {
            EXPECT_FALSE(nestedError);
            tokens.push_back(std::move(nestedToken.Token));
            innerCallback.MarkInvoked();
        });
        outerCallback.MarkInvoked();
    });

    EXPECT_EQ(tokens, (std::vector<std::string>{"cached-token", "cached-token"}));
    EXPECT_EQ(inner->CallCount(), 1);
}

TEST(ITokenCredentialTests, CachingTokenCredential_FixedClockDrivesRefreshWindowAndExpiryBoundaries)
{
    using Clock = std::chrono::system_clock;
    const Clock::time_point start{std::chrono::hours{1000000}};
    auto now = start;

    auto inner = std::make_shared<ScriptedTokenCredential>();
    AVEVA::AzureClient::AccessToken first{.Token = "first", .ExpiresOn = start + std::chrono::minutes(10)};
    AVEVA::AzureClient::AccessToken second{.Token = "second", .ExpiresOn = start + std::chrono::hours(2)};
    inner->EnqueueImmediate({}, first);
    inner->EnqueueImmediate({}, second);
    auto credential = CachingTokenCredential::Create(inner,
        std::chrono::minutes(5),
        std::nullopt,
        [&now]
    {
        return now;
    });

    std::string token;
    const auto request = [&]
    {
        credential->GetTokenAsync({"scope-a"},
            [&](std::error_code error, AVEVA::AzureClient::AccessToken result)
        {
            EXPECT_FALSE(error);
            token = std::move(result.Token);
        });
    };

    request();
    EXPECT_EQ(token, "first");
    EXPECT_EQ(inner->CallCount(), 1);

    // One tick before the refresh window opens: still served from the cache.
    now = start + std::chrono::minutes(5) - std::chrono::seconds(1);
    request();
    EXPECT_EQ(token, "first");
    EXPECT_EQ(inner->CallCount(), 1);

    // Exactly at the refresh-window boundary a refresh starts.
    now = start + std::chrono::minutes(5);
    request();
    EXPECT_EQ(inner->CallCount(), 2);

    // Past the refreshed token's start the new token is served without another call.
    now = start + std::chrono::minutes(6);
    request();
    EXPECT_EQ(token, "second");
    EXPECT_EQ(inner->CallCount(), 2);
}
