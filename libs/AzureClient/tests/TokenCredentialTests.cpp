// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/TokenCredential.hpp>

#include <cstddef>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace {
using AVEVA::AzureClient::AccessToken;
using AVEVA::AzureClient::CachingTokenCredential;
using AVEVA::AzureClient::Tests::CallbackExpectation;
using AVEVA::AzureClient::Tests::MakeToken;
using AVEVA::AzureClient::Tests::ScriptedTokenCredential;
struct NowRef {
    const std::chrono::system_clock::time_point* Now;
    std::chrono::system_clock::time_point operator()() const { return *Now; }
};
using TestCachingCredential = CachingTokenCredential<ScriptedTokenCredential>;
using ClockedCachingCredential = CachingTokenCredential<ScriptedTokenCredential, NowRef>;

void PrimeTokenRequest(const std::shared_ptr<TestCachingCredential>& credential, std::vector<std::string> scopes) {
    credential->GetTokenAsync(std::move(scopes), [](std::error_code, const AccessToken&) {});
}

void RequestTokenCapturingValue(const std::shared_ptr<TestCachingCredential>& credential,
                                std::vector<std::string> scopes, std::string& receivedToken,
                                CallbackExpectation* callback = nullptr) {
    credential->GetTokenAsync(std::move(scopes), [&](std::error_code error, AccessToken token) {
        EXPECT_FALSE(error);
        receivedToken = std::move(token.Token);
        if (callback != nullptr) {
            callback->MarkInvoked();
        }
    });
}

void RequestTokenAppendingValue(const std::shared_ptr<TestCachingCredential>& credential,
                                std::vector<std::string> scopes, std::vector<std::string>& receivedTokens,
                                CallbackExpectation& callback) {
    credential->GetTokenAsync(std::move(scopes), [&](std::error_code error, AccessToken token) {
        EXPECT_FALSE(error);
        receivedTokens.push_back(std::move(token.Token));
        callback.MarkInvoked();
    });
}

void RequestWaitersForSharedToken(const std::shared_ptr<TestCachingCredential>& credential, int waiterCount,
                                  std::vector<std::string>& receivedTokens,
                                  std::vector<CallbackExpectation>& callbacks) {
    for (int i = 0; i < waiterCount; ++i) {
        credential->GetTokenAsync({"scope-a"}, [&, i](std::error_code error, AccessToken token) {
            EXPECT_FALSE(error);
            receivedTokens.push_back(std::move(token.Token));
            callbacks.at(static_cast<std::size_t>(i)).MarkInvoked();
        });
    }
}
} // namespace

namespace {
// A thread-safe inner credential that answers inline with a token inside the refresh window.
class ShortLivedTokenCredential final {
  public:
    template <boost::asio::completion_token_for<AVEVA::AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> /*scopes*/, CompletionToken&& token) {
        return boost::asio::async_initiate<AVEVA::AzureClient::TokenSignature>(
            [](auto handler) {
                std::move(handler)(std::error_code{}, MakeToken("short-lived", std::chrono::seconds(1)));
            },
            token);
    }
};
} // namespace

TEST(TokenCredentialTests, CachingTokenCredential_ConcurrentCallersDuringRefreshAllCompleteExactlyOnce) {
    constexpr int Threads = 8;
    constexpr int CallsPerThread = 200;
    auto credential = CachingTokenCredential<ShortLivedTokenCredential>::Create(
        std::make_shared<ShortLivedTokenCredential>(), std::chrono::minutes(5));

    std::atomic<int> completions{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < Threads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < CallsPerThread; ++i) {
                credential->GetTokenAsync({"scope-a"}, [&](std::error_code, const AccessToken&) { ++completions; });
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(completions.load(), Threads * CallsPerThread);
}
TEST(TokenCredentialTests, CachingTokenCredential_NegativeRefreshWindowIsRejected) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    EXPECT_THROW(static_cast<void>(TestCachingCredential::Create(inner, std::chrono::seconds(-1))),
                 std::invalid_argument);
    EXPECT_NO_THROW(static_cast<void>(TestCachingCredential::Create(inner, std::chrono::seconds(0))));
}

TEST(TokenCredentialTests, CachingTokenCredential_InvalidateForcesANewInnerRequestForThoseScopes) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("first", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("second", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    std::string received;
    RequestTokenCapturingValue(credential, {"scope-a"}, received);
    EXPECT_EQ(received, "first");

    credential->Invalidate(std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-b"}));
    RequestTokenCapturingValue(credential, {"scope-a"}, received);
    EXPECT_EQ(received, "first");
    EXPECT_EQ(inner->CallCount(), 1);

    credential->Invalidate(std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-a"}));
    RequestTokenCapturingValue(credential, {"scope-a"}, received);
    EXPECT_EQ(received, "second");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_InvalidateWithRejectedTokenKeepsAFresherToken) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("first", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("second", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
    const auto scopes = std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-a"});

    std::string received;
    RequestTokenCapturingValue(credential, {"scope-a"}, received);
    EXPECT_EQ(received, "first");

    // Two requests were rejected with "first": only the first invalidation may drop the cache.
    credential->Invalidate(scopes, "first");
    RequestTokenCapturingValue(credential, {"scope-a"}, received);
    EXPECT_EQ(received, "second");
    credential->Invalidate(scopes, "first");
    RequestTokenCapturingValue(credential, {"scope-a"}, received);
    EXPECT_EQ(received, "second");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_ReusesCachedTokenOutsideRefreshWindow) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    std::vector<std::string> receivedTokens;
    CallbackExpectation firstCallback;
    CallbackExpectation secondCallback;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
        EXPECT_FALSE(error);
        receivedTokens.push_back(std::move(token.Token));
        firstCallback.MarkInvoked();
    });
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
        EXPECT_FALSE(error);
        receivedTokens.push_back(std::move(token.Token));
        secondCallback.MarkInvoked();
    });

    EXPECT_EQ(inner->CallCount(), 1);
    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"cached-token", "cached-token"}));
}

namespace {
template <class T> struct CountingAllocator {
    using value_type = T;
    std::atomic<int>* Count;
    explicit CountingAllocator(std::atomic<int>* count) : Count(count) {}
    template <class U> CountingAllocator(const CountingAllocator<U>& other) : Count(other.Count) {}
    T* allocate(std::size_t n) {
        ++*Count;
        return std::allocator<T>{}.allocate(n);
    }
    void deallocate(T* p, std::size_t n) { std::allocator<T>{}.deallocate(p, n); }
    template <class U> bool operator==(const CountingAllocator<U>& other) const { return Count == other.Count; }
};

// Erasing this handler into an any_completion_handler allocates through its associated allocator.
struct CountingHandler {
    using allocator_type = CountingAllocator<char>;
    std::atomic<int>* Count;
    int* Invoked;
    allocator_type get_allocator() const noexcept { return allocator_type{Count}; }
    void operator()(std::error_code error, AVEVA::AzureClient::AccessToken) const {
        EXPECT_FALSE(error);
        ++*Invoked;
    }
};
} // namespace

TEST(TokenCredentialTests, CachingTokenCredential_CacheHitDoesNotEraseTheHandler) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
    std::atomic<int> allocations{0};
    int invoked = 0;

    credential->GetTokenAsync({"scope-a"}, CountingHandler{&allocations, &invoked});
    const int afterColdPath = allocations.load();
    credential->GetTokenAsync({"scope-a"}, CountingHandler{&allocations, &invoked});

    EXPECT_EQ(invoked, 2);
    EXPECT_GT(afterColdPath, 0);
    EXPECT_EQ(allocations.load(), afterColdPath);
}

TEST(TokenCredentialTests, CachingTokenCredential_SharedScopeListIsServedFromTheCacheAndMatchesVectorCalls) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
    const auto scopes = std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-a"});

    std::vector<std::string> receivedTokens;
    const auto record = [&](std::error_code error, AccessToken token) {
        EXPECT_FALSE(error);
        receivedTokens.push_back(std::move(token.Token));
    };
    credential->GetTokenForScopesAsync(scopes, record);
    credential->GetTokenForScopesAsync(scopes, record);
    credential->GetTokenAsync({"scope-a"}, record);

    EXPECT_EQ(inner->CallCount(), 1);
    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"cached-token", "cached-token", "cached-token"}));
    ASSERT_EQ(inner->RequestedScopes().size(), 1U);
    EXPECT_EQ(inner->RequestedScopes().front(), *scopes);
}

TEST(TokenCredentialTests, CachingTokenCredential_ServesCachedTokenAndRefreshesInBackgroundInsideRefreshWindow) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("old-token", std::chrono::seconds(30)));
    inner->EnqueueDeferred({}, MakeToken("new-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

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

TEST(TokenCredentialTests, CachingTokenCredential_TokenShorterThanRefreshWindowIsServedBeforeRefreshCompletes) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("short-lived", std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("refreshed", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

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

TEST(TokenCredentialTests, CachingTokenCredential_ExpiredTokenWaitsForTheRefresh) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("expired-token", -std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("new-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});

    std::string receivedToken;
    CallbackExpectation callback;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
        EXPECT_FALSE(error);
        receivedToken = std::move(token.Token);
        callback.MarkInvoked();
    });
    EXPECT_TRUE(receivedToken.empty());
    EXPECT_EQ(inner->PendingCount(), 1U);

    EXPECT_TRUE(inner->CompleteNext());
    EXPECT_EQ(receivedToken, "new-token");
}

TEST(TokenCredentialTests, CachingTokenCredential_InvalidatesCacheWhenScopesChange) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("token-b", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

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

TEST(TokenCredentialTests, CachingTokenCredential_RefreshFailureDoesNotDiscardPreviousScopeCache) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    std::error_code refreshError;
    std::string cachedTokenAfterFailure;
    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});
    CallbackExpectation failedRefresh;
    CallbackExpectation cachedRead;
    credential->GetTokenAsync({"scope-b"}, [&](std::error_code error, const AVEVA::AzureClient::AccessToken&) {
        refreshError = error;
        failedRefresh.MarkInvoked();
    });
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
        EXPECT_FALSE(error);
        cachedTokenAfterFailure = std::move(token.Token);
        cachedRead.MarkInvoked();
    });

    EXPECT_EQ(refreshError, std::make_error_code(std::errc::permission_denied));
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(cachedTokenAfterFailure, "token-a");
}

TEST(TokenCredentialTests, CachingTokenCredential_RemembersFailureBrieflyWhenNoValidTokenExists) {
    auto now = std::chrono::system_clock::time_point{std::chrono::hours(1000)};
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    auto credential = ClockedCachingCredential::Create(inner, std::chrono::minutes(5), std::nullopt, NowRef{&now});

    std::error_code first;
    std::error_code second;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, const AccessToken&) { first = error; });
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, const AccessToken&) { second = error; });

    EXPECT_EQ(first, std::make_error_code(std::errc::permission_denied));
    EXPECT_EQ(second, std::make_error_code(std::errc::permission_denied));
    EXPECT_EQ(inner->CallCount(), 1);

    // Once the window has passed the inner credential is asked again.
    now += AVEVA::AzureClient::DefaultTokenCredentialNegativeCacheWindow + std::chrono::seconds(1);
    std::string token;
    credential->GetTokenAsync({"scope-a"},
                              [&](std::error_code, AccessToken received) { token = std::move(received.Token); });
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(token, "token-a");
}

TEST(TokenCredentialTests, CachingTokenCredential_ServesStaleTokenDuringRefreshBackoff) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::seconds(30)));
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});

    // In the refresh window: served from cache while the background refresh fails, which starts
    // the failed-refresh backoff instead of surfacing the error to a caller with a valid token.
    std::error_code inWindowError = std::make_error_code(std::errc::io_error);
    std::string inWindowToken;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
        inWindowError = error;
        inWindowToken = std::move(token.Token);
    });

    std::string staleToken;
    CallbackExpectation callback;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
        EXPECT_FALSE(error);
        staleToken = std::move(token.Token);
        callback.MarkInvoked();
    });

    EXPECT_FALSE(inWindowError);
    EXPECT_EQ(inWindowToken, "token-a");
    EXPECT_EQ(staleToken, "token-a");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_ConcurrentInWindowCallersShareOneBackgroundRefresh) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("old-token", std::chrono::seconds(10)));
    inner->EnqueueDeferred({}, MakeToken("refreshed-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

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

TEST(TokenCredentialTests, CachingTokenCredential_CoalescesConcurrentCallersWithoutAValidToken) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("expired-token", -std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("refreshed-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

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

TEST(TokenCredentialTests, CachingTokenCredential_NullInnerCompletesAllConcurrentWaiters) {
    auto credential = TestCachingCredential::Create(nullptr, std::chrono::minutes(5));

    std::error_code firstError;
    std::error_code secondError;
    CallbackExpectation firstCallback;
    CallbackExpectation secondCallback;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, const AVEVA::AzureClient::AccessToken&) {
        firstError = error;
        firstCallback.MarkInvoked();
    });
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, const AVEVA::AzureClient::AccessToken&) {
        secondError = error;
        secondCallback.MarkInvoked();
    });

    EXPECT_EQ(firstError, std::make_error_code(std::errc::operation_not_permitted));
    EXPECT_EQ(secondError, std::make_error_code(std::errc::operation_not_permitted));
}

TEST(TokenCredentialTests, CachingTokenCredential_NullInnerResetsInFlightStateForSubsequentCalls) {
    auto credential = TestCachingCredential::Create(nullptr, std::chrono::minutes(5));

    std::error_code firstError;
    credential->GetTokenAsync(
        {"scope-a"}, [&](std::error_code error, const AVEVA::AzureClient::AccessToken&) { firstError = error; });
    EXPECT_EQ(firstError, std::make_error_code(std::errc::operation_not_permitted));

    // Without the fix, m_inFlightRefresh would still point at the first (dead) refresh state,
    // and this second call with matching scopes would hang forever instead of completing.
    std::error_code secondError;
    CallbackExpectation secondCallback;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, const AVEVA::AzureClient::AccessToken&) {
        secondError = error;
        secondCallback.MarkInvoked();
    });

    EXPECT_EQ(secondError, std::make_error_code(std::errc::operation_not_permitted));
}

namespace {
// Throws std::bad_alloc from initiation on the first call, after running `OnFirstCall` (used to join a refresh
// that has been published but has no producer yet). Later calls answer inline.
class ThrowOnceCredential final {
  public:
    template <boost::asio::completion_token_for<AVEVA::AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> /*scopes*/, CompletionToken&& token) {
        return boost::asio::async_initiate<AVEVA::AzureClient::TokenSignature>(
            [this](auto handler) {
                ++Calls;
                if (Calls == 1) {
                    if (OnFirstCall) {
                        OnFirstCall();
                    }
                    throw std::bad_alloc();
                }
                std::move(handler)(std::error_code{}, MakeToken("recovered", std::chrono::hours(1)));
            },
            token);
    }

    int Calls = 0;
    std::function<void()> OnFirstCall;
};
} // namespace

TEST(TokenCredentialTests, CachingTokenCredential_InnerInitiationThrowFailsAdmittedWaitersAndAllowsRetry) {
    auto inner = std::make_shared<ThrowOnceCredential>();
    auto credential = CachingTokenCredential<ThrowOnceCredential>::Create(inner, std::chrono::minutes(5));

    std::vector<std::error_code> errors;
    inner->OnFirstCall = [&] {
        credential->GetTokenAsync({"scope-a"},
                                  [&](std::error_code error, const AccessToken&) { errors.push_back(error); });
    };
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, const AccessToken&) { errors.push_back(error); });

    ASSERT_EQ(errors.size(), 2U);
    EXPECT_EQ(errors[0], std::make_error_code(std::errc::not_enough_memory));
    EXPECT_EQ(errors[1], std::make_error_code(std::errc::not_enough_memory));

    std::string token;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AccessToken value) {
        EXPECT_FALSE(error);
        token = std::move(value.Token);
    });
    EXPECT_EQ(token, "recovered");
    EXPECT_EQ(inner->Calls, 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_InnerInitiationThrowDuringBackgroundRefreshKeepsServingToken) {
    auto inner = std::make_shared<ThrowOnceCredential>();
    auto now = std::chrono::system_clock::now();
    auto credential = CachingTokenCredential<ThrowOnceCredential, NowRef>::Create(inner, std::chrono::minutes(5),
                                                                                  std::nullopt, NowRef{&now});

    // Prime a token that is valid but inside the refresh window, bypassing the throw on the first call.
    inner->Calls = 1;
    std::string primed;
    credential->GetTokenAsync({"scope-a"},
                              [&](std::error_code, AccessToken value) { primed = std::move(value.Token); });
    EXPECT_EQ(primed, "recovered");

    // Make the cached token fall inside the refresh window, then fail the background refresh's initiation.
    now += std::chrono::minutes(58);
    inner->Calls = 0;
    std::vector<std::error_code> errors;
    inner->OnFirstCall = [&] {
        credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, const AccessToken& value) {
            errors.push_back(error);
            EXPECT_EQ(value.Token, "recovered");
        });
    };
    std::string served;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AccessToken value) {
        EXPECT_FALSE(error);
        served = std::move(value.Token);
    });
    EXPECT_EQ(served, "recovered");
    EXPECT_EQ(errors.size(), 1U);

    // The failed claim was released, so the next in-window call starts a fresh refresh.
    inner->OnFirstCall = nullptr;
    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AccessToken&) {});
    EXPECT_EQ(inner->Calls, 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_FanOutDeliversSameTokenToAllWaiters) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("shared-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    constexpr int WaiterCount = 5;
    std::vector<std::string> receivedTokens;
    std::vector<CallbackExpectation> callbacks(WaiterCount);
    RequestWaitersForSharedToken(credential, WaiterCount, receivedTokens, callbacks);

    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(inner->CompleteNext());

    ASSERT_EQ(receivedTokens.size(), static_cast<std::size_t>(WaiterCount));
    for (const auto& token : receivedTokens) {
        EXPECT_EQ(token, "shared-token");
    }
}

namespace {
// Five coalesced waiters where waiter `throwingIndex` throws; checks all run once and the first exception surfaces.
void ExpectFanOutSurvivesThrowingRecipient(std::size_t throwingIndex, std::error_code innerError) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred(innerError, MakeToken("shared-token", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("next-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    constexpr std::size_t WaiterCount = 5;
    std::vector<int> invocations(WaiterCount, 0);
    std::vector<std::error_code> errors(WaiterCount);
    for (std::size_t i = 0; i < WaiterCount; ++i) {
        credential->GetTokenAsync({"scope-a"}, [&, i](std::error_code error, const AccessToken&) {
            ++invocations.at(i);
            errors.at(i) = error;
            if (i == throwingIndex || i == throwingIndex + 1) {
                throw std::runtime_error("recipient " + std::to_string(i));
            }
        });
    }

    try {
        static_cast<void>(inner->CompleteNext());
        ADD_FAILURE() << "the first recipient exception was swallowed";
    } catch (const std::runtime_error& failure) {
        EXPECT_EQ(std::string(failure.what()), "recipient " + std::to_string(throwingIndex));
    }

    EXPECT_EQ(invocations, std::vector<int>(WaiterCount, 1));
    for (const auto& error : errors) {
        EXPECT_EQ(error, innerError);
    }

    if (innerError) {
        // The failure is negatively cached, so a later request is answered without throwing or hanging.
        std::string received;
        bool completed = false;
        credential->GetTokenAsync({"scope-b"}, [&](std::error_code error, AccessToken token) {
            EXPECT_FALSE(error);
            received = std::move(token.Token);
            completed = true;
        });
        EXPECT_TRUE(completed);
        EXPECT_EQ(received, "next-token");
    } else {
        std::string received;
        credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AccessToken token) {
            EXPECT_FALSE(error);
            received = std::move(token.Token);
        });
        EXPECT_EQ(received, "shared-token");
    }
}
} // namespace

TEST(TokenCredentialTests, CachingTokenCredential_ThrowingFirstRecipientDoesNotStrandOthersOnSuccess) {
    ExpectFanOutSurvivesThrowingRecipient(0U, {});
}

TEST(TokenCredentialTests, CachingTokenCredential_ThrowingMiddleRecipientDoesNotStrandOthersOnSuccess) {
    ExpectFanOutSurvivesThrowingRecipient(2U, {});
}

TEST(TokenCredentialTests, CachingTokenCredential_ThrowingFirstRecipientDoesNotStrandOthersOnError) {
    ExpectFanOutSurvivesThrowingRecipient(0U, std::make_error_code(std::errc::connection_reset));
}

TEST(TokenCredentialTests, CachingTokenCredential_ThrowingMiddleRecipientDoesNotStrandOthersOnError) {
    ExpectFanOutSurvivesThrowingRecipient(2U, std::make_error_code(std::errc::connection_reset));
}

TEST(TokenCredentialTests, CachingTokenCredential_CompletesOutstandingRequestAfterDestruction) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("late-token", std::chrono::hours(1)));

    std::weak_ptr<TestCachingCredential> weakCredential;
    std::string receivedToken;
    CallbackExpectation callback;

    {
        auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
        weakCredential = credential;
        credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
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

TEST(TokenCredentialTests, CachingTokenCredential_DeliversToCoalescedWaitersAfterDestruction) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("late-token", std::chrono::hours(1)));
    std::vector<std::string> receivedTokens;
    CallbackExpectation first;
    CallbackExpectation second;
    {
        auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
        RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, first);
        RequestTokenAppendingValue(credential, {"scope-a"}, receivedTokens, second);
    }

    EXPECT_EQ(inner->CallCount(), 1);
    EXPECT_TRUE(inner->CompleteNext());
    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"late-token", "late-token"}));
}

TEST(TokenCredentialTests, CachingTokenCredential_DoesNotDeadlockWhenHandlerReentersCacheHitPath) {
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    credential->GetTokenAsync({"scope-a"}, [](std::error_code, const AVEVA::AzureClient::AccessToken&) {});

    std::vector<std::string> tokens;
    CallbackExpectation outerCallback;
    CallbackExpectation innerCallback;
    credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken token) {
        EXPECT_FALSE(error);
        tokens.push_back(std::move(token.Token));
        credential->GetTokenAsync({"scope-a"},
                                  [&](std::error_code nestedError, AVEVA::AzureClient::AccessToken nestedToken) {
                                      EXPECT_FALSE(nestedError);
                                      tokens.push_back(std::move(nestedToken.Token));
                                      innerCallback.MarkInvoked();
                                  });
        outerCallback.MarkInvoked();
    });

    EXPECT_EQ(tokens, (std::vector<std::string>{"cached-token", "cached-token"}));
    EXPECT_EQ(inner->CallCount(), 1);
}

TEST(TokenCredentialTests, CachingTokenCredential_FixedClockDrivesRefreshWindowAndExpiryBoundaries) {
    using Clock = std::chrono::system_clock;
    const Clock::time_point start{std::chrono::hours{1000000}};
    auto now = start;
    struct ClockRef {
        const Clock::time_point* Now;
        Clock::time_point operator()() const { return *Now; }
    };

    auto inner = std::make_shared<ScriptedTokenCredential>();
    AVEVA::AzureClient::AccessToken first{.Token = "first", .ExpiresOn = start + std::chrono::minutes(10)};
    AVEVA::AzureClient::AccessToken second{.Token = "second", .ExpiresOn = start + std::chrono::hours(2)};
    inner->EnqueueImmediate({}, first);
    inner->EnqueueImmediate({}, second);
    auto credential = CachingTokenCredential<ScriptedTokenCredential, ClockRef>::Create(inner, std::chrono::minutes(5),
                                                                                        std::nullopt, ClockRef{&now});

    std::string token;
    const auto request = [&] {
        credential->GetTokenAsync({"scope-a"}, [&](std::error_code error, AVEVA::AzureClient::AccessToken result) {
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
