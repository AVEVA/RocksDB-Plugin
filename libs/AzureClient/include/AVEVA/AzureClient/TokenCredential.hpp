// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/append.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <chrono>
#include <concepts>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
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

// Completion signature of every token acquisition. Every configuration, transport and parsing failure is
// reported through it as a non-zero std::error_code (the AccessToken is then unspecified); the operation
// itself does not throw for such failures. The completion may run inline, before the initiating call
// returns, or later on an executor or transport thread.
using TokenSignature = void(std::error_code, AccessToken);

// A scope list shared between calls, so callers that ask for the same scopes on every request (and caches
// that only compare them) need not copy the strings.
using ScopeList = std::shared_ptr<const std::vector<std::string>>;

// A token credential is any type with
//   GetTokenAsync(std::vector<std::string> scopes, CompletionToken&&)
// that accepts every Boost.Asio completion token for TokenSignature. A credential may additionally provide
// GetTokenForScopesAsync(ScopeList, CompletionToken&&), which the clients prefer because it avoids copying
// the scope list on every request.
template <class T>
concept TokenCredentialLike = requires(T& credential, std::vector<std::string> scopes) {
    credential.GetTokenAsync(std::move(scopes), boost::asio::detached);
};

// Requests a token for `scopes` through the shared-scope overload when the credential has one.
template <TokenCredentialLike TCred, class CompletionToken>
auto GetTokenForScopes(TCred& credential, ScopeList scopes, CompletionToken&& token) {
    if constexpr (requires { credential.GetTokenForScopesAsync(scopes, std::forward<CompletionToken>(token)); }) {
        return credential.GetTokenForScopesAsync(std::move(scopes), std::forward<CompletionToken>(token));
    } else {
        return credential.GetTokenAsync(scopes ? *scopes : std::vector<std::string>{},
                                        std::forward<CompletionToken>(token));
    }
}

namespace Private {
// Completes on `executor` when one is set, otherwise inline.
template <class THandler, class... Args>
void CompleteOnExecutorIfSet(const std::optional<boost::asio::io_context::executor_type>& executor,
                             THandler&& completion, Args&&... args) {
    if (executor.has_value()) {
        boost::asio::post(*executor,
                          boost::asio::append(std::forward<THandler>(completion), std::forward<Args>(args)...));
    } else {
        std::forward<THandler>(completion)(std::forward<Args>(args)...);
    }
}
} // namespace Private

// The credential of clients that are not given one: every request fails with std::errc::operation_not_permitted.
// Clients authorized by SAS or Shared Key use it as their TCred.
class NoTokenCredential {
  public:
    template <boost::asio::completion_token_for<TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> /*scopes*/, CompletionToken&& token) {
        return boost::asio::async_initiate<TokenSignature>(
            [](auto handler) {
                std::move(handler)(std::make_error_code(std::errc::operation_not_permitted), AccessToken{});
            },
            token);
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
// mutex-protected and concurrent callers during a refresh are coalesced onto one request. Completions
// may run inline on the calling thread or on the executor of the underlying credential.
//
// Callers coalesced onto one refresh come with different handler types, so the handlers waiting on a refresh
// are stored as boost::asio::any_completion_handler, Asio's own erased handler; this is the only type
// erasure left in the credential path.
template <TokenCredentialLike TInner, TokenClockLike TClock = SystemTokenClock>
class CachingTokenCredential : public std::enable_shared_from_this<CachingTokenCredential<TInner, TClock>> {
    // Keeps the constructor unusable outside Create while still allowing std::make_shared.
    struct CreateKey {
        explicit CreateKey() = default;
    };

    using Waiter = boost::asio::any_completion_handler<TokenSignature>;

  public:
    // The cache relies on shared ownership, so instances can only be created through this factory.
    [[nodiscard]] static std::shared_ptr<CachingTokenCredential>
    Create(std::shared_ptr<TInner> inner, std::chrono::seconds refreshWindow = DefaultTokenCredentialRefreshWindow,
           std::optional<boost::asio::io_context::executor_type> executor = std::nullopt, TClock clock = {}) {
        return std::make_shared<CachingTokenCredential>(CreateKey{}, std::move(inner), refreshWindow,
                                                        std::move(executor), std::move(clock));
    }

    CachingTokenCredential(CreateKey, std::shared_ptr<TInner> inner, std::chrono::seconds refreshWindow,
                           std::optional<boost::asio::io_context::executor_type> executor, TClock clock)
        : m_inner(std::move(inner)), m_refreshWindow(refreshWindow), m_executor(std::move(executor)),
          m_clock(std::move(clock)) {
        // A negative window makes every token look about to expire, so each request would start a refresh.
        if (m_refreshWindow < std::chrono::seconds::zero()) {
            throw std::invalid_argument("CachingTokenCredential refreshWindow must not be negative.");
        }
    }

    template <boost::asio::completion_token_for<TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return GetTokenForScopesAsync(std::make_shared<const std::vector<std::string>>(std::move(scopes)),
                                      std::forward<CompletionToken>(token));
    }

    // The cache-hit path compares against the shared list without copying it.
    template <boost::asio::completion_token_for<TokenSignature> CompletionToken>
    auto GetTokenForScopesAsync(ScopeList scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<TokenSignature>(
            [this](auto handler, ScopeList requestedScopes) { Start(std::move(requestedScopes), std::move(handler)); },
            token, std::move(scopes));
    }

    // Drops the cached token (and any remembered failure) for `scopes`, so the next request asks the inner
    // credential again. Used after the service rejected the token with a 401.
    void Invalidate(const ScopeList& scopes) { InvalidateIf(scopes, nullptr); }

    // As above, but the cached token is dropped only while it still is `rejectedToken`. Concurrent requests that
    // were all rejected with the same token then cause a single refresh instead of discarding each other's result.
    void Invalidate(const ScopeList& scopes, std::string_view rejectedToken) { InvalidateIf(scopes, &rejectedToken); }

  private:
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

  private:
    // Callers normally share one list, so pointer identity answers the common case and the element-wise
    // comparison only runs when two distinct lists are compared. A null list is an empty one.
    static bool SameScopes(const ScopeList& left, const ScopeList& right) {
        if (left == right) {
            return true;
        }
        static const std::vector<std::string> empty;
        return (left ? *left : empty) == (right ? *right : empty);
    }

    struct RefreshState {
        ScopeList Scopes;
        std::optional<boost::asio::io_context::executor_type> Executor;
        std::vector<Waiter> Completions;
        // Guards Completions; the state can outlive the credential that created it.
        std::mutex Mutex;
        // Set once the waiters were taken for completion (guarded by Mutex).
        bool Drained = false;
    };

    // Result of consulting the cache under lock: either a cached token to serve immediately, a refresh to
    // start (optionally alongside the cached token), or an indication that `completion` was already queued
    // onto an in-flight refresh and must not be used again by the caller.
    struct LookupResult {
        std::optional<AccessToken> CachedToken;
        // Set when a recent acquisition failed and there is no valid token to fall back to.
        std::optional<std::error_code> CachedFailure;
        std::shared_ptr<RefreshState> RefreshToStart;
    };

    // Hands `token` to the last pending completion by move and copies it to the others, without the
    // move-inside-a-loop pattern that would leave `token` in a moved-from state mid-iteration.
    static void CompleteAll(const std::optional<boost::asio::io_context::executor_type>& executor,
                            std::vector<Waiter> completions, std::error_code error, AccessToken token) {
        if (completions.empty()) {
            return;
        }

        // With no completion executor the handlers run inline. A throwing recipient must not strand the ones
        // behind it, so every recipient is invoked and the first exception is rethrown afterwards.
        std::exception_ptr firstFailure;
        const auto deliver = [&](Waiter&& pending, std::error_code outcome, AccessToken&& value) {
            try {
                Private::CompleteOnExecutorIfSet(executor, std::move(pending), outcome, std::move(value));
            } catch (...) {
                if (!firstFailure) {
                    firstFailure = std::current_exception();
                }
            }
        };

        Waiter last = std::move(completions.back());
        completions.pop_back();
        for (auto& pending : completions) {
            // The copy can throw while `pending` is still owned here, so that recipient is failed instead of
            // being dropped, and the remaining recipients are still served.
            std::optional<AccessToken> copy;
            try {
                copy.emplace(token);
            } catch (...) {
            }
            if (copy.has_value()) {
                deliver(std::move(pending), error, std::move(*copy));
            } else {
                deliver(std::move(pending), std::make_error_code(std::errc::not_enough_memory), AccessToken{});
            }
        }
        deliver(std::move(last), error, std::move(token));
        if (firstFailure) {
            std::rethrow_exception(firstFailure);
        }
    }

    // `completion` is only erased into a Waiter when it has to queue behind a refresh; otherwise the caller keeps it.
    template <class THandler>
    LookupResult LookUpCachedTokenOrStartRefresh(const ScopeList& scopes, THandler& completion) {
        std::scoped_lock const lock(m_mutex);
        const auto now = m_clock();
        const bool hasMatchingToken = m_hasToken && SameScopes(m_cachedScopes, scopes);
        const bool tokenStillValid = hasMatchingToken && m_cachedToken.ExpiresOn > now;
        const bool outsideRefreshWindow = tokenStillValid && (m_cachedToken.ExpiresOn - now) > m_refreshWindow;
        const bool refreshBackoffActive =
            tokenStillValid && m_nextRefreshRetryAfter.has_value() && now < *m_nextRefreshRetryAfter;

        if (outsideRefreshWindow || refreshBackoffActive) {
            LookupResult result;
            result.CachedToken = m_cachedToken;
            return result;
        }

        if (!tokenStillValid && m_failureRetryAfter.has_value() && now < *m_failureRetryAfter &&
            SameScopes(m_failedScopes, scopes)) {
            LookupResult result;
            result.CachedFailure = m_lastFailure;
            return result;
        }

        if (tokenStillValid) {
            // Inside the refresh window: serve the still-valid token now and refresh in the background
            // (joining a refresh that is already running).
            LookupResult result;
            result.CachedToken = m_cachedToken;
            if (!m_inFlightRefresh || !SameScopes(m_inFlightRefresh->Scopes, scopes)) {
                result.RefreshToStart = std::make_shared<RefreshState>();
                result.RefreshToStart->Scopes = scopes;
                result.RefreshToStart->Executor = m_executor;
                m_inFlightRefresh = result.RefreshToStart;
            }
            return result;
        }

        if (m_inFlightRefresh && SameScopes(m_inFlightRefresh->Scopes, scopes)) {
            std::scoped_lock const stateLock(m_inFlightRefresh->Mutex);
            m_inFlightRefresh->Completions.push_back(Waiter{std::move(completion)});
            return {};
        }

        LookupResult result;
        result.RefreshToStart = std::make_shared<RefreshState>();
        result.RefreshToStart->Scopes = scopes;
        result.RefreshToStart->Executor = m_executor;
        result.RefreshToStart->Completions.push_back(Waiter{std::move(completion)});
        m_inFlightRefresh = result.RefreshToStart;
        return result;
    }

    // The two mutexes are never held together: the state is detached under the cache mutex first, so no lock
    // order exists to violate. Waiters can no longer join once it is detached, so draining it afterwards is complete.
    // `alreadyDrained` reports whether an earlier drain (the inner completion) already ran for this state.
    std::vector<Waiter> DrainAndReset(const std::shared_ptr<RefreshState>& refreshState,
                                      bool* alreadyDrained = nullptr) {
        {
            std::scoped_lock const lock(m_mutex);
            if (m_inFlightRefresh == refreshState) {
                m_inFlightRefresh.reset();
            }
        }
        std::scoped_lock const stateLock(refreshState->Mutex);
        if (alreadyDrained != nullptr) {
            *alreadyDrained = refreshState->Drained;
        }
        refreshState->Drained = true;
        return std::move(refreshState->Completions);
    }

    // The inner credential threw before it could deliver a result, so nothing will ever complete the waiters
    // admitted to this refresh. Releases the claim and fails them once. Returns false when the inner credential
    // had already delivered, in which case the exception came from a completion and must not be swallowed.
    bool FailRefreshAfterInitiationThrow(const std::shared_ptr<RefreshState>& refreshState,
                                         std::exception_ptr failure) {
        bool alreadyDrained = false;
        std::vector<Waiter> completions = DrainAndReset(refreshState, &alreadyDrained);
        if (alreadyDrained) {
            return false;
        }
        std::error_code error = std::make_error_code(std::errc::io_error);
        try {
            std::rethrow_exception(failure);
        } catch (const std::bad_alloc&) {
            error = std::make_error_code(std::errc::not_enough_memory);
        } catch (...) {
        }
        CompleteAll(refreshState->Executor, std::move(completions), error, AccessToken{});
        return true;
    }

    void CompleteWithNoInnerCredentialError(const std::shared_ptr<RefreshState>& refreshState) {
        std::vector<Waiter> completions = DrainAndReset(refreshState);
        CompleteAll(refreshState->Executor, std::move(completions),
                    std::make_error_code(std::errc::operation_not_permitted), AccessToken{});
    }

    void HandleInnerTokenResult(std::error_code error, AccessToken token,
                                const std::shared_ptr<RefreshState>& refreshState, const ScopeList& scopes) {
        const auto now = m_clock();
        // The copies are made before taking the lock and published by (noexcept) moves, so a failed copy leaves
        // the cache untouched. Publishing is an optimisation: the waiters below must get their outcome regardless.
        try {
            if (!error) {
                auto cachedToken = token;
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
                    m_lastFailure = error;
                    m_failedScopes = scopes;
                }
            }
        } catch (...) {
        }

        std::vector<Waiter> completions = DrainAndReset(refreshState);
        CompleteAll(refreshState->Executor, std::move(completions), error, std::move(token));
    }

    template <class THandler> void Start(ScopeList scopes, THandler completion) {
        if (!scopes) {
            scopes = std::make_shared<const std::vector<std::string>>();
        }
        const std::optional<boost::asio::io_context::executor_type> completionExecutor = m_executor;
        LookupResult lookup = LookUpCachedTokenOrStartRefresh(scopes, completion);
        if (lookup.CachedFailure.has_value()) {
            Private::CompleteOnExecutorIfSet(completionExecutor, std::move(completion), *lookup.CachedFailure,
                                             AccessToken{});
            return;
        }
        if (!lookup.RefreshToStart && !lookup.CachedToken.has_value()) {
            // The completion was queued onto a refresh that is already running; it will be invoked from there.
            return;
        }

        if (lookup.CachedToken.has_value() && !lookup.RefreshToStart) {
            Private::CompleteOnExecutorIfSet(completionExecutor, std::move(completion), std::error_code{},
                                             std::move(*lookup.CachedToken));
            return;
        }

        if (!m_inner) {
            CompleteWithNoInnerCredentialError(lookup.RefreshToStart);
            return;
        }

        std::exception_ptr initiationFailure;
        try {
            GetTokenForScopes(*m_inner, scopes,
                              [weakSelf = this->weak_from_this(), refreshState = lookup.RefreshToStart,
                               scopes](std::error_code error, AccessToken token) mutable {
                                  if (const auto self = weakSelf.lock()) {
                                      self->HandleInnerTokenResult(error, std::move(token), refreshState, scopes);
                                      return;
                                  }

                                  std::vector<Waiter> completions;
                                  {
                                      std::scoped_lock const stateLock(refreshState->Mutex);
                                      refreshState->Drained = true;
                                      completions = std::move(refreshState->Completions);
                                  }
                                  CompleteAll(refreshState->Executor, std::move(completions), error, std::move(token));
                              });
        } catch (...) {
            initiationFailure = std::current_exception();
            if (FailRefreshAfterInitiationThrow(lookup.RefreshToStart, initiationFailure)) {
                initiationFailure = nullptr;
            }
            if (!lookup.CachedToken.has_value()) {
                if (initiationFailure) {
                    std::rethrow_exception(initiationFailure);
                }
                return;
            }
        }

        // The background refresh is started first, so a re-entrant call from this completion joins it.
        if (lookup.CachedToken.has_value()) {
            Private::CompleteOnExecutorIfSet(completionExecutor, std::move(completion), std::error_code{},
                                             std::move(*lookup.CachedToken));
        }
        if (initiationFailure) {
            std::rethrow_exception(initiationFailure);
        }
    }

    std::shared_ptr<TInner> m_inner;
    std::chrono::seconds m_refreshWindow;
    std::optional<boost::asio::io_context::executor_type> m_executor;
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
[[nodiscard]] std::shared_ptr<CachingTokenCredential<TInner, TClock>> MakeCachingTokenCredential(
    std::shared_ptr<TInner> inner, std::chrono::seconds refreshWindow = DefaultTokenCredentialRefreshWindow,
    std::optional<boost::asio::io_context::executor_type> executor = std::nullopt, TClock clock = {}) {
    return CachingTokenCredential<TInner, TClock>::Create(std::move(inner), refreshWindow, std::move(executor),
                                                          std::move(clock));
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
