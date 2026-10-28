// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/TokenCredential.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

// Lets a test make the Nth upcoming large allocation on this thread throw; -1 means disarmed. Token strings are
// copied inside the standard library, so a global replacement is the only way to reach those copies. Only
// allocations of at least LargeAllocationBytes are counted, which keeps small bookkeeping allocations out of it.
namespace AllocationFailure {
constexpr std::size_t LargeAllocationBytes = 2048;
thread_local int largeAllocationsUntilFailure = -1;
thread_local int largeAllocationsSeen = 0;
} // namespace AllocationFailure

void* operator new(std::size_t size) {
    if (size >= AllocationFailure::LargeAllocationBytes) {
        ++AllocationFailure::largeAllocationsSeen;
        if (AllocationFailure::largeAllocationsUntilFailure >= 0 &&
            AllocationFailure::largeAllocationsUntilFailure-- == 0) {
            throw std::bad_alloc();
        }
    }
    if (void* memory = std::malloc(size != 0 ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc();
}

void operator delete(void* memory) noexcept { std::free(memory); }

void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

namespace {
using AVEVA::AzureClient::AccessToken;
using AVEVA::AzureClient::CachingTokenCredential;
using AVEVA::AzureClient::TokenResult;
using AVEVA::AzureClient::Tests::MakeToken;
using AVEVA::AzureClient::Tests::Operation;
using AVEVA::AzureClient::Tests::RunAwaitable;
using AVEVA::AzureClient::Tests::ScriptedTokenCredential;
using AVEVA::AzureClient::Tests::StartAwaitable;
struct NowRef {
    const std::chrono::system_clock::time_point* Now;
    std::chrono::system_clock::time_point operator()() const { return *Now; }
};
using TestCachingCredential = CachingTokenCredential<ScriptedTokenCredential>;
using ClockedCachingCredential = CachingTokenCredential<ScriptedTokenCredential, NowRef>;

// Runs everything the credential posts: waiter resumptions and detached background refreshes.
void Poll(boost::asio::io_context& context) {
    context.restart();
    context.poll();
}

// Requests a token to completion, then lets a background refresh the request may have started run up to its first
// suspension.
template <class Credential>
TokenResult Get(boost::asio::io_context& context, const std::shared_ptr<Credential>& credential,
                std::vector<std::string> scopes = {"scope-a"}) {
    auto result = RunAwaitable(context, credential->GetTokenAsync(std::move(scopes)));
    Poll(context);
    return result;
}

template <class Credential>
Operation<TokenResult> Start(boost::asio::io_context& context, const std::shared_ptr<Credential>& credential,
                             std::vector<std::string> scopes = {"scope-a"}) {
    return StartAwaitable(context, credential->GetTokenAsync(std::move(scopes)));
}

std::string TokenOf(const TokenResult& result) {
    EXPECT_TRUE(result.has_value()) << result.error().message();
    return result ? result->Token : std::string{};
}

// A thread-safe inner credential that answers inline with a token inside the refresh window.
class ShortLivedTokenCredential final {
  public:
    boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> /*scopes*/) {
        co_return MakeToken("short-lived", std::chrono::seconds(1));
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
            boost::asio::io_context context;
            for (int i = 0; i < CallsPerThread; ++i) {
                if (Get(context, credential)) {
                    ++completions;
                }
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
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("first", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("second", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    EXPECT_EQ(TokenOf(Get(context, credential)), "first");

    credential->Invalidate(std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-b"}));
    EXPECT_EQ(TokenOf(Get(context, credential)), "first");
    EXPECT_EQ(inner->CallCount(), 1);

    credential->Invalidate(std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-a"}));
    EXPECT_EQ(TokenOf(Get(context, credential)), "second");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_InvalidateWithRejectedTokenKeepsAFresherToken) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("first", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("second", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
    const auto scopes = std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-a"});

    EXPECT_EQ(TokenOf(Get(context, credential)), "first");

    // Two requests were rejected with "first": only the first invalidation may drop the cache.
    credential->Invalidate(scopes, "first");
    EXPECT_EQ(TokenOf(Get(context, credential)), "second");
    credential->Invalidate(scopes, "first");
    EXPECT_EQ(TokenOf(Get(context, credential)), "second");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_ReusesCachedTokenOutsideRefreshWindow) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    EXPECT_EQ(TokenOf(Get(context, credential)), "cached-token");
    EXPECT_EQ(TokenOf(Get(context, credential)), "cached-token");

    EXPECT_EQ(inner->CallCount(), 1);
}

TEST(TokenCredentialTests, CachingTokenCredential_SharedScopeListIsServedFromTheCacheAndMatchesVectorCalls) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("cached-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
    const auto scopes = std::make_shared<const std::vector<std::string>>(std::vector<std::string>{"scope-a"});

    std::vector<std::string> receivedTokens;
    for (int i = 0; i < 2; ++i) {
        receivedTokens.push_back(TokenOf(RunAwaitable(context, credential->GetTokenForScopesAsync(scopes))));
    }
    receivedTokens.push_back(TokenOf(Get(context, credential)));

    EXPECT_EQ(inner->CallCount(), 1);
    EXPECT_EQ(receivedTokens, (std::vector<std::string>{"cached-token", "cached-token", "cached-token"}));
    ASSERT_EQ(inner->RequestedScopes().size(), 1U);
    EXPECT_EQ(inner->RequestedScopes().front(), *scopes);
}

TEST(TokenCredentialTests, CachingTokenCredential_ServesCachedTokenAndRefreshesInBackgroundInsideRefreshWindow) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("old-token", std::chrono::seconds(30)));
    inner->EnqueueDeferred({}, MakeToken("new-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential));

    // The token is still valid but inside the refresh window: the caller gets it immediately and
    // exactly one background refresh starts.
    EXPECT_EQ(TokenOf(Get(context, credential)), "old-token");
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);

    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    EXPECT_EQ(TokenOf(Get(context, credential)), "new-token");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_TokenShorterThanRefreshWindowIsServedBeforeRefreshCompletes) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("short-lived", std::chrono::seconds(10)));
    inner->EnqueueDeferred({}, MakeToken("refreshed", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential));

    EXPECT_EQ(TokenOf(Get(context, credential)), "short-lived");
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);

    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    EXPECT_EQ(TokenOf(Get(context, credential)), "refreshed");
}

TEST(TokenCredentialTests, CachingTokenCredential_ExpiredTokenWaitsForTheRefresh) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("expired-token", -std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("new-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential));

    auto operation = Start(context, credential);
    EXPECT_FALSE(operation.Done());
    EXPECT_EQ(inner->PendingCount(), 1U);

    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    ASSERT_TRUE(operation.Done());
    EXPECT_EQ(TokenOf(operation.Result()), "new-token");
}

TEST(TokenCredentialTests, CachingTokenCredential_InvalidatesCacheWhenScopesChange) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("token-b", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential, {"scope-a"}));
    EXPECT_EQ(TokenOf(Get(context, credential, {"scope-b"})), "token-b");

    EXPECT_EQ(inner->CallCount(), 2);
    ASSERT_EQ(inner->RequestedScopes().size(), 2U);
    EXPECT_EQ(inner->RequestedScopes().at(0), (std::vector<std::string>{"scope-a"}));
    EXPECT_EQ(inner->RequestedScopes().at(1), (std::vector<std::string>{"scope-b"}));
}

TEST(TokenCredentialTests, CachingTokenCredential_RefreshFailureDoesNotDiscardPreviousScopeCache) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential, {"scope-a"}));
    const auto failed = Get(context, credential, {"scope-b"});
    const auto cachedAfterFailure = Get(context, credential, {"scope-a"});

    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), std::make_error_code(std::errc::permission_denied));
    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(TokenOf(cachedAfterFailure), "token-a");
}

TEST(TokenCredentialTests, CachingTokenCredential_RemembersFailureBrieflyWhenNoValidTokenExists) {
    boost::asio::io_context context;
    auto now = std::chrono::system_clock::time_point{std::chrono::hours(1000)};
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::hours(1)));
    auto credential = ClockedCachingCredential::Create(inner, std::chrono::minutes(5), NowRef{&now});

    const auto first = Get(context, credential);
    const auto second = Get(context, credential);

    ASSERT_FALSE(first.has_value());
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(first.error(), std::make_error_code(std::errc::permission_denied));
    EXPECT_EQ(second.error(), std::make_error_code(std::errc::permission_denied));
    EXPECT_EQ(inner->CallCount(), 1);

    // Once the window has passed the inner credential is asked again.
    now += AVEVA::AzureClient::DefaultTokenCredentialNegativeCacheWindow + std::chrono::seconds(1);
    EXPECT_EQ(TokenOf(Get(context, credential)), "token-a");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_ServesStaleTokenDuringRefreshBackoff) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token-a", std::chrono::seconds(30)));
    inner->EnqueueImmediate(std::make_error_code(std::errc::permission_denied), {});
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential));

    // In the refresh window: served from cache while the background refresh fails, which starts
    // the failed-refresh backoff instead of surfacing the error to a caller with a valid token.
    EXPECT_EQ(TokenOf(Get(context, credential)), "token-a");
    EXPECT_EQ(inner->CallCount(), 2);

    EXPECT_EQ(TokenOf(Get(context, credential)), "token-a");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_ConcurrentInWindowCallersShareOneBackgroundRefresh) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("old-token", std::chrono::seconds(10)));
    inner->EnqueueDeferred({}, MakeToken("refreshed-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential));
    EXPECT_EQ(TokenOf(Get(context, credential)), "old-token");
    EXPECT_EQ(TokenOf(Get(context, credential)), "old-token");

    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);
    EXPECT_EQ(TokenOf(Get(context, credential)), "refreshed-token");
}

TEST(TokenCredentialTests, CachingTokenCredential_CoalescesConcurrentCallersWithoutAValidToken) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("expired-token", -std::chrono::seconds(1)));
    inner->EnqueueDeferred({}, MakeToken("refreshed-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    static_cast<void>(Get(context, credential));
    auto first = Start(context, credential);
    auto second = Start(context, credential);

    EXPECT_EQ(inner->CallCount(), 2);
    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_FALSE(first.Done());
    EXPECT_FALSE(second.Done());

    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    ASSERT_TRUE(first.Done());
    ASSERT_TRUE(second.Done());
    EXPECT_EQ(TokenOf(first.Result()), "refreshed-token");
    EXPECT_EQ(TokenOf(second.Result()), "refreshed-token");
}

TEST(TokenCredentialTests, CachingTokenCredential_NullInnerFailsEveryCallWithoutHanging) {
    boost::asio::io_context context;
    auto credential = TestCachingCredential::Create(nullptr, std::chrono::minutes(5));

    const auto first = Get(context, credential);
    ASSERT_FALSE(first.has_value());
    EXPECT_EQ(first.error(), std::make_error_code(std::errc::operation_not_permitted));

    // The failed refresh must not stay registered as in flight, or a second call with matching scopes would wait
    // forever.
    const auto second = Get(context, credential);
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), std::make_error_code(std::errc::operation_not_permitted));
}

TEST(TokenCredentialTests, CachingTokenCredential_InnerThrowFailsTheRequestAndAllowsRetryAfterTheFailureWindow) {
    boost::asio::io_context context;
    auto now = std::chrono::system_clock::now();
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueThrow();
    inner->EnqueueImmediate({}, MakeToken("recovered", std::chrono::hours(1)));
    auto credential = ClockedCachingCredential::Create(inner, std::chrono::minutes(5), NowRef{&now});

    const auto failed = Get(context, credential);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), std::make_error_code(std::errc::not_enough_memory));

    // The failure is remembered, so the inner credential is not asked again straight away.
    EXPECT_FALSE(Get(context, credential).has_value());
    EXPECT_EQ(inner->CallCount(), 1);

    now += AVEVA::AzureClient::DefaultTokenCredentialNegativeCacheWindow + std::chrono::seconds(1);
    EXPECT_EQ(TokenOf(Get(context, credential)), "recovered");
    EXPECT_EQ(inner->CallCount(), 2);
}

TEST(TokenCredentialTests, CachingTokenCredential_InnerThrowDuringBackgroundRefreshKeepsServingToken) {
    boost::asio::io_context context;
    auto now = std::chrono::system_clock::now();
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueImmediate({}, MakeToken("token", std::chrono::seconds(100)));
    inner->EnqueueThrow();
    inner->EnqueueImmediate({}, MakeToken("fresh", std::chrono::hours(1)));
    auto credential = ClockedCachingCredential::Create(inner, std::chrono::minutes(5), NowRef{&now});

    static_cast<void>(Get(context, credential));

    // The token is inside the refresh window, so the caller is served while the background refresh throws.
    EXPECT_EQ(TokenOf(Get(context, credential)), "token");
    EXPECT_EQ(inner->CallCount(), 2);

    // The failed refresh backs off, then the next in-window call starts a fresh one.
    EXPECT_EQ(TokenOf(Get(context, credential)), "token");
    EXPECT_EQ(inner->CallCount(), 2);
    now += AVEVA::AzureClient::DefaultTokenCredentialFailedRefreshBackoff + std::chrono::seconds(1);
    EXPECT_EQ(TokenOf(Get(context, credential)), "token");
    EXPECT_EQ(inner->CallCount(), 3);
    EXPECT_EQ(TokenOf(Get(context, credential)), "fresh");
}

TEST(TokenCredentialTests, CachingTokenCredential_FanOutDeliversSameTokenToAllWaiters) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("shared-token", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    constexpr int WaiterCount = 5;
    std::vector<Operation<TokenResult>> operations;
    for (int i = 0; i < WaiterCount; ++i) {
        operations.push_back(Start(context, credential));
    }

    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    for (auto& operation : operations) {
        ASSERT_TRUE(operation.Done());
        EXPECT_EQ(TokenOf(operation.Result()), "shared-token");
    }
    EXPECT_EQ(inner->CallCount(), 1);
}

TEST(TokenCredentialTests, CachingTokenCredential_DeliversInnerErrorToAllWaiters) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred(std::make_error_code(std::errc::connection_reset), {});
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    auto leader = Start(context, credential);
    auto waiter = Start(context, credential);
    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    for (auto* operation : {&leader, &waiter}) {
        ASSERT_TRUE(operation->Done());
        ASSERT_FALSE(operation->Result().has_value());
        EXPECT_EQ(operation->Result().error(), std::make_error_code(std::errc::connection_reset));
    }
    EXPECT_EQ(inner->CallCount(), 1);
}

TEST(TokenCredentialTests, CachingTokenCredential_CancelledLeaderLetsWaitersAskAgain) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("never-delivered", std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken("second", std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    auto leader = Start(context, credential);
    auto waiter = Start(context, credential);
    EXPECT_EQ(inner->PendingCount(), 1U);

    leader.Cancel();
    Poll(context);

    ASSERT_TRUE(leader.Done());
    ASSERT_FALSE(leader.Result().has_value());
    EXPECT_EQ(leader.Result().error(), std::make_error_code(std::errc::operation_canceled));
    // The cancellation says nothing about the credential, so it is neither cached nor handed to the waiter.
    ASSERT_TRUE(waiter.Done());
    EXPECT_EQ(TokenOf(waiter.Result()), "second");
    EXPECT_EQ(inner->CallCount(), 2);
}

namespace {
struct FanOutOutcome {
    std::vector<int> Invocations;
    std::vector<std::error_code> Errors;
    std::vector<std::size_t> TokenSizes;
};

// Three coalesced callers share one deferred inner completion carrying a long token. The `failingAllocation`-th
// large allocation made while that completion is delivered throws (-1: none), then a later acquisition runs.
FanOutOutcome RunFanOutWithFailingTokenCopy(int failingAllocation) {
    constexpr std::size_t TokenLength = 4096;
    constexpr std::size_t WaiterCount = 3;
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken(std::string(TokenLength, 'x'), std::chrono::hours(1)));
    inner->EnqueueImmediate({}, MakeToken(std::string(TokenLength, 'y'), std::chrono::hours(1)));
    auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));

    std::vector<Operation<TokenResult>> operations;
    for (std::size_t i = 0; i < WaiterCount; ++i) {
        operations.push_back(Start(context, credential));
    }

    AllocationFailure::largeAllocationsUntilFailure = failingAllocation;
    try {
        EXPECT_TRUE(inner->CompleteNext());
        Poll(context);
    } catch (...) {
        // The outcome below is what matters; a surfaced exception must not strand any caller.
    }
    AllocationFailure::largeAllocationsUntilFailure = -1;

    FanOutOutcome outcome;
    outcome.Invocations.assign(WaiterCount, 0);
    outcome.Errors.assign(WaiterCount, {});
    outcome.TokenSizes.assign(WaiterCount, 0);
    for (std::size_t i = 0; i < WaiterCount; ++i) {
        if (!operations.at(i).Done()) {
            continue;
        }
        outcome.Invocations.at(i) = 1;
        const auto& result = operations.at(i).Result();
        if (result) {
            outcome.TokenSizes.at(i) = result->Token.size();
        } else {
            outcome.Errors.at(i) = result.error();
        }
    }

    const auto later = Get(context, credential);
    EXPECT_TRUE(later.has_value());
    EXPECT_EQ(later ? later->Token.size() : 0U, TokenLength);
    return outcome;
}
} // namespace

TEST(TokenCredentialTests, CachingTokenCredential_FailedCachePublicationStillDeliversToAllWaiters) {
    // Allocation 0 is the copy that publishes the token into the cache.
    const auto outcome = RunFanOutWithFailingTokenCopy(0);
    EXPECT_EQ(outcome.Invocations, std::vector<int>(3, 1));
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(outcome.Errors.at(i)) << i;
        EXPECT_EQ(outcome.TokenSizes.at(i), 4096U) << i;
    }
}

TEST(TokenCredentialTests, CachingTokenCredential_OutstandingRequestKeepsTheCredentialAliveUntilItCompletes) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("late-token", std::chrono::hours(1)));

    std::weak_ptr<TestCachingCredential> weakCredential;
    std::optional<Operation<TokenResult>> operation;
    {
        auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
        weakCredential = credential;
        operation = Start(context, credential);
    }

    EXPECT_EQ(inner->PendingCount(), 1U);
    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    ASSERT_TRUE(operation->Done());
    EXPECT_EQ(TokenOf(operation->Result()), "late-token");
    EXPECT_TRUE(weakCredential.expired());
}

TEST(TokenCredentialTests, CachingTokenCredential_DeliversToCoalescedWaitersAfterTheCallerReleasesTheCredential) {
    boost::asio::io_context context;
    auto inner = std::make_shared<ScriptedTokenCredential>();
    inner->EnqueueDeferred({}, MakeToken("late-token", std::chrono::hours(1)));
    std::vector<Operation<TokenResult>> operations;
    {
        auto credential = TestCachingCredential::Create(inner, std::chrono::minutes(5));
        operations.push_back(Start(context, credential));
        operations.push_back(Start(context, credential));
    }

    EXPECT_EQ(inner->CallCount(), 1);
    EXPECT_TRUE(inner->CompleteNext());
    Poll(context);

    for (auto& operation : operations) {
        ASSERT_TRUE(operation.Done());
        EXPECT_EQ(TokenOf(operation.Result()), "late-token");
    }
}

TEST(TokenCredentialTests, CachingTokenCredential_FixedClockDrivesRefreshWindowAndExpiryBoundaries) {
    using Clock = std::chrono::system_clock;
    const Clock::time_point start{std::chrono::hours{1000000}};
    auto now = start;
    boost::asio::io_context context;

    auto inner = std::make_shared<ScriptedTokenCredential>();
    AccessToken first{.Token = "first", .ExpiresOn = start + std::chrono::minutes(10)};
    AccessToken second{.Token = "second", .ExpiresOn = start + std::chrono::hours(2)};
    inner->EnqueueImmediate({}, first);
    inner->EnqueueImmediate({}, second);
    auto credential = ClockedCachingCredential::Create(inner, std::chrono::minutes(5), NowRef{&now});

    EXPECT_EQ(TokenOf(Get(context, credential)), "first");
    EXPECT_EQ(inner->CallCount(), 1);

    // One tick before the refresh window opens: still served from the cache.
    now = start + std::chrono::minutes(5) - std::chrono::seconds(1);
    EXPECT_EQ(TokenOf(Get(context, credential)), "first");
    EXPECT_EQ(inner->CallCount(), 1);

    // Exactly at the refresh-window boundary a refresh starts.
    now = start + std::chrono::minutes(5);
    static_cast<void>(Get(context, credential));
    EXPECT_EQ(inner->CallCount(), 2);

    // Past the refreshed token's start the new token is served without another call.
    now = start + std::chrono::minutes(6);
    EXPECT_EQ(TokenOf(Get(context, credential)), "second");
    EXPECT_EQ(inner->CallCount(), 2);
}
