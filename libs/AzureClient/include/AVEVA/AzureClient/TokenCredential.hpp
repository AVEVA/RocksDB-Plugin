// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <concepts>
#include <exception>
#include <expected>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient {
inline constexpr std::chrono::seconds DefaultTokenCredentialRefreshWindow{300};
inline constexpr std::chrono::seconds DefaultTokenCredentialFailedRefreshBackoff{30};
// After a failed acquisition with no usable token, the same failure is reported for this long without
// calling the inner credential again, so a failing identity provider is not hammered by every request.
inline constexpr std::chrono::seconds DefaultTokenCredentialNegativeCacheWindow{5};

struct AccessToken {
    std::string Token;
    std::chrono::system_clock::time_point ExpiresOn = std::chrono::system_clock::time_point::max();
};

// The outcome of every token acquisition. Every configuration, transport and parsing failure is reported as a
// non-zero std::error_code; an acquisition does not throw for such failures.
using TokenResult = std::expected<AccessToken, std::error_code>;

// A scope list shared between calls, so callers that ask for the same scopes on every request (and caches
// that only compare them) need not copy the strings.
using ScopeList = std::shared_ptr<const std::vector<std::string>>;

// A token credential is any type with
//   boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes)
// A credential may additionally provide `awaitable<TokenResult> GetTokenForScopesAsync(ScopeList)`, which the
// clients prefer because it avoids copying the scope list on every request.
template <class T>
concept TokenCredentialLike = requires(T& credential, std::vector<std::string> scopes) {
    { credential.GetTokenAsync(std::move(scopes)) } -> std::same_as<boost::asio::awaitable<TokenResult>>;
};

// Requests a token for `scopes` through the shared-scope overload when the credential has one.
template <TokenCredentialLike TCred>
[[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenForScopes(TCred& credential, ScopeList scopes) {
    if constexpr (requires { credential.GetTokenForScopesAsync(scopes); }) {
        co_return co_await credential.GetTokenForScopesAsync(std::move(scopes));
    } else {
        co_return co_await credential.GetTokenAsync(scopes ? *scopes : std::vector<std::string>{});
    }
}

// The credential of clients that are not given one: every request fails with std::errc::operation_not_permitted.
// Clients authorized by SAS or Shared Key use it as their TCred.
class NoTokenCredential {
  public:
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> /*scopes*/) {
        co_return std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
    }
};

// "Now" for expiry, refresh-window and backoff decisions of CachingTokenCredential.
struct SystemTokenClock {
    [[nodiscard]] std::chrono::system_clock::time_point operator()() const noexcept {
        return std::chrono::system_clock::now();
    }
};

template <class T>
concept TokenClockLike =
    std::invocable<T&> && std::convertible_to<std::invoke_result_t<T&>, std::chrono::system_clock::time_point>;

// Caches the inner credential's token. The cache holds one token keyed by the exact (ordered) scope
// list; a request with different scopes does not reuse it. A refresh starts once less than
// `refreshWindow` remains before expiry (the stale-but-valid token is served meanwhile), concurrent
// requests for the same scopes coalesce into a single inner request, and after a failed refresh of a
// still-valid token the cached token is reused for DefaultTokenCredentialFailedRefreshBackoff (30 s)
// before another refresh is attempted. When there is no valid token at all, a failure is remembered for
// DefaultTokenCredentialNegativeCacheWindow (5 s) and returned without calling the inner credential.
//
// Thread safety: GetTokenAsync may be called concurrently from several threads; internal state is
// mutex-protected. The first caller without a usable token performs the inner request inside its own
// coroutine and the others wait for its outcome; a background refresh runs as a detached coroutine on the
// executor of the caller that triggered it. If the leading caller is cancelled, the waiters ask again.
template <TokenCredentialLike TInner, TokenClockLike TClock = SystemTokenClock>
class CachingTokenCredential : public std::enable_shared_from_this<CachingTokenCredential<TInner, TClock>> {
    // Keeps the constructor unusable outside Create while still allowing std::make_shared.
    struct CreateKey {
        explicit CreateKey() = default;
    };
    // Waiters of one refresh; erased because callers resume on their own executors.
    using Waiter = boost::asio::any_completion_handler<void()>;

  public:
    // The cache relies on shared ownership, so instances can only be created through this factory.
    [[nodiscard]] static std::shared_ptr<CachingTokenCredential>
    Create(std::shared_ptr<TInner> inner, std::chrono::seconds refreshWindow = DefaultTokenCredentialRefreshWindow,
           TClock clock = {}) {
        return std::make_shared<CachingTokenCredential>(CreateKey{}, std::move(inner), refreshWindow, std::move(clock));
    }

    CachingTokenCredential(CreateKey, std::shared_ptr<TInner> inner, std::chrono::seconds refreshWindow, TClock clock)
        : m_inner(std::move(inner)), m_refreshWindow(refreshWindow), m_clock(std::move(clock)) {
        // A negative window makes every token look about to expire, so each request would start a refresh.
        if (m_refreshWindow < std::chrono::seconds::zero()) {
            throw std::invalid_argument("CachingTokenCredential refreshWindow must not be negative.");
        }
    }

    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        co_return co_await GetTokenForScopesAsync(std::make_shared<const std::vector<std::string>>(std::move(scopes)));
    }

    // The cache-hit path compares against the shared list without copying it.
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenForScopesAsync(ScopeList scopes) {
        if (!scopes) {
            scopes = std::make_shared<const std::vector<std::string>>();
        }
        const auto self = this->shared_from_this();
        for (;;) {
            Lookup lookup = LookUp(scopes);
            if (lookup.Immediate.has_value()) {
                if (lookup.BackgroundRefresh) {
                    // Started first, so a re-entrant call joins it.
                    boost::asio::co_spawn(co_await boost::asio::this_coro::executor,
                                          Refresh(self, scopes, std::move(lookup.BackgroundRefresh)),
                                          boost::asio::detached);
                }
                co_return std::move(*lookup.Immediate);
            }
            if (lookup.Leader) {
                co_return co_await Refresh(self, scopes, std::move(lookup.State));
            }
            co_await WaitFor(lookup.State);
            // A cancelled leader tells nothing about the credential, so ask again.
            if (lookup.State->Result.has_value() ||
                lookup.State->Result.error() != std::make_error_code(std::errc::operation_canceled)) {
                co_return lookup.State->Result;
            }
        }
    }

    // Drops the cached token (and any remembered failure) for `scopes`, so the next request asks the inner
    // credential again. Used after the service rejected the token with a 401.
    void Invalidate(const ScopeList& scopes) { InvalidateIf(scopes, nullptr); }

    // As above, but the cached token is dropped only while it still is `rejectedToken`. Concurrent requests that
    // were all rejected with the same token then cause a single refresh instead of discarding each other's result.
    void Invalidate(const ScopeList& scopes, std::string_view rejectedToken) { InvalidateIf(scopes, &rejectedToken); }

  private:
    struct RefreshState {
        ScopeList Scopes;
        std::mutex Mutex;
        std::vector<Waiter> Waiters;
        TokenResult Result = std::unexpected(std::make_error_code(std::errc::operation_canceled));
        bool Done = false;
    };

    // Result of consulting the cache under lock: a token or failure to serve right away (optionally alongside a
    // background refresh to start), or a refresh to lead or to wait for.
    struct Lookup {
        std::optional<TokenResult> Immediate;
        std::shared_ptr<RefreshState> BackgroundRefresh;
        std::shared_ptr<RefreshState> State;
        bool Leader = false;
    };

    void InvalidateIf(const ScopeList& scopes, const std::string_view* rejectedToken) {
        std::scoped_lock const lock(m_mutex);
        if (m_hasToken && SameScopes(m_cachedScopes, scopes) &&
            (rejectedToken == nullptr || m_cachedToken.Token == *rejectedToken)) {
            m_hasToken = false;
            m_cachedToken = {};
            m_nextRefreshRetryAfter.reset();
        }
        if (SameScopes(m_failedScopes, scopes)) {
            m_failureRetryAfter.reset();
        }
    }

    // Callers normally share one list, so pointer identity answers the common case and the element-wise
    // comparison only runs when two distinct lists are compared. A null list is an empty one.
    static bool SameScopes(const ScopeList& left, const ScopeList& right) {
        if (left == right) {
            return true;
        }
        static const std::vector<std::string> empty;
        return (left ? *left : empty) == (right ? *right : empty);
    }

    [[nodiscard]] std::shared_ptr<RefreshState> NewRefreshState(const ScopeList& scopes) {
        auto state = std::make_shared<RefreshState>();
        state->Scopes = scopes;
        m_inFlightRefresh = state;
        return state;
    }

    Lookup LookUp(const ScopeList& scopes) {
        std::scoped_lock const lock(m_mutex);
        const auto now = m_clock();
        const bool hasMatchingToken = m_hasToken && SameScopes(m_cachedScopes, scopes);
        const bool tokenStillValid = hasMatchingToken && m_cachedToken.ExpiresOn > now;
        const bool outsideRefreshWindow = tokenStillValid && (m_cachedToken.ExpiresOn - now) > m_refreshWindow;
        const bool refreshBackoffActive =
            tokenStillValid && m_nextRefreshRetryAfter.has_value() && now < *m_nextRefreshRetryAfter;

        Lookup result;
        if (outsideRefreshWindow || refreshBackoffActive) {
            result.Immediate = m_cachedToken;
            return result;
        }
        if (!tokenStillValid && m_failureRetryAfter.has_value() && now < *m_failureRetryAfter &&
            SameScopes(m_failedScopes, scopes)) {
            result.Immediate = std::unexpected(m_lastFailure);
            return result;
        }
        if (tokenStillValid) {
            // Inside the refresh window: serve the still-valid token now and refresh in the background
            // (joining a refresh that is already running).
            result.Immediate = m_cachedToken;
            if (!m_inFlightRefresh || !SameScopes(m_inFlightRefresh->Scopes, scopes)) {
                result.BackgroundRefresh = NewRefreshState(scopes);
            }
            return result;
        }
        if (m_inFlightRefresh && SameScopes(m_inFlightRefresh->Scopes, scopes)) {
            result.State = m_inFlightRefresh;
            return result;
        }
        result.State = NewRefreshState(scopes);
        result.Leader = true;
        return result;
    }

    // Suspends until `state` is done; resumes inline-free through post.
    static boost::asio::awaitable<void> WaitFor(std::shared_ptr<RefreshState> state) {
        auto token = boost::asio::as_tuple(boost::asio::use_awaitable);
        co_await boost::asio::async_initiate<decltype(token), void()>(
            [](auto handler, std::shared_ptr<RefreshState> st) {
                auto executor = boost::asio::get_associated_executor(handler);
                // Resuming an awaitable handler runs the coroutine in place, so hop to its executor first.
                Waiter waiter = [executor, handler = std::move(handler)]() mutable {
                    boost::asio::post(executor, std::move(handler));
                };
                std::unique_lock lock(st->Mutex);
                if (st->Done) {
                    lock.unlock();
                    std::move(waiter)();
                    return;
                }
                st->Waiters.emplace_back(std::move(waiter));
            },
            token, std::move(state));
    }

    void Finish(const std::shared_ptr<RefreshState>& state, TokenResult result) {
        {
            std::scoped_lock const lock(m_mutex);
            if (m_inFlightRefresh == state) {
                m_inFlightRefresh.reset();
            }
        }
        std::vector<Waiter> waiters;
        {
            std::scoped_lock const lock(state->Mutex);
            state->Result = std::move(result);
            state->Done = true;
            waiters = std::move(state->Waiters);
        }
        for (auto& waiter : waiters) {
            std::move(waiter)();
        }
    }

    // Asks the inner credential and publishes the outcome to the cache and to every waiter.
    static boost::asio::awaitable<TokenResult> Refresh(std::shared_ptr<CachingTokenCredential> self, ScopeList scopes,
                                                       std::shared_ptr<RefreshState> state) {
        TokenResult result = std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
        if (self->m_inner) {
            try {
                result = co_await GetTokenForScopes(*self->m_inner, scopes);
            } catch (const std::bad_alloc&) {
                result = std::unexpected(std::make_error_code(std::errc::not_enough_memory));
            } catch (const std::system_error& e) {
                result = std::unexpected(e.code());
            } catch (...) {
                result = std::unexpected(std::make_error_code(std::errc::io_error));
            }
        }
        self->Publish(scopes, result);
        self->Finish(state, result);
        co_return result;
    }

    // Caching is an optimisation: a failure to publish must not prevent delivering the outcome.
    void Publish(const ScopeList& scopes, const TokenResult& result) noexcept {
        const auto now = m_clock();
        if (!result.has_value() && result.error() == std::make_error_code(std::errc::operation_canceled)) {
            return;
        }
        try {
            if (result) {
                auto cachedToken = *result;
                std::scoped_lock const lock(m_mutex);
                m_cachedScopes = scopes;
                m_cachedToken = std::move(cachedToken);
                m_hasToken = true;
                m_nextRefreshRetryAfter.reset();
                m_failureRetryAfter.reset();
            } else {
                std::scoped_lock const lock(m_mutex);
                if (m_hasToken && SameScopes(m_cachedScopes, scopes) && m_cachedToken.ExpiresOn > now) {
                    m_nextRefreshRetryAfter = now + DefaultTokenCredentialFailedRefreshBackoff;
                } else {
                    m_failureRetryAfter = now + DefaultTokenCredentialNegativeCacheWindow;
                    m_lastFailure = result.error();
                    m_failedScopes = scopes;
                }
            }
        } catch (...) {
        }
    }

    std::shared_ptr<TInner> m_inner;
    std::chrono::seconds m_refreshWindow;
    TClock m_clock;
    std::mutex m_mutex;
    std::shared_ptr<RefreshState> m_inFlightRefresh;
    ScopeList m_cachedScopes;
    AccessToken m_cachedToken;
    bool m_hasToken = false;
    std::optional<std::chrono::system_clock::time_point> m_nextRefreshRetryAfter;
    std::optional<std::chrono::system_clock::time_point> m_failureRetryAfter;
    std::error_code m_lastFailure;
    ScopeList m_failedScopes;
};

// Deduces the inner credential and clock types.
template <TokenCredentialLike TInner, TokenClockLike TClock = SystemTokenClock>
[[nodiscard]] std::shared_ptr<CachingTokenCredential<TInner, TClock>>
MakeCachingTokenCredential(std::shared_ptr<TInner> inner,
                           std::chrono::seconds refreshWindow = DefaultTokenCredentialRefreshWindow,
                           TClock clock = {}) {
    return CachingTokenCredential<TInner, TClock>::Create(std::move(inner), refreshWindow, std::move(clock));
}

template <class T> struct IsCachingTokenCredential : std::false_type {};

template <class TInner, class TClock>
struct IsCachingTokenCredential<CachingTokenCredential<TInner, TClock>> : std::true_type {};

// The credential a client actually uses for TCred: TCred itself when it already caches, otherwise TCred
// wrapped in a CachingTokenCredential.
template <TokenCredentialLike TCred>
using CachedTokenCredential =
    std::conditional_t<IsCachingTokenCredential<TCred>::value, TCred, CachingTokenCredential<TCred>>;
} // namespace AVEVA::AzureClient
