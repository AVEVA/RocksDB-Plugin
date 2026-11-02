// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>

#include <chrono>
#include <concepts>
#include <cstdint>
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
/// <summary>
/// How long before expiry a cached token starts being refreshed.
/// </summary>
inline constexpr std::chrono::seconds DefaultTokenCredentialRefreshWindow{300};

/// <summary>
/// How long a still-valid cached token is reused after a failed refresh before another refresh is attempted.
/// </summary>
inline constexpr std::chrono::seconds DefaultTokenCredentialFailedRefreshBackoff{30};

/// <summary>
/// After a failed acquisition with no usable token, the same failure is reported for this long without
/// calling the inner credential again, so a failing identity provider is not hammered by every request.
/// </summary>
inline constexpr std::chrono::seconds DefaultTokenCredentialNegativeCacheWindow{5};

/// <summary>
/// A bearer token together with the moment it stops being valid.
/// </summary>
struct AccessToken {
    /// <summary>The token text.</summary>
    std::string Token;
    /// <summary>When the token expires; the maximum time point when it never does.</summary>
    std::chrono::system_clock::time_point ExpiresOn = std::chrono::system_clock::time_point::max();
};

/// <summary>
/// The outcome of every token acquisition. Every configuration, transport and parsing failure is reported as a
/// non-zero std::error_code; an acquisition does not throw for such failures.
/// </summary>
using TokenResult = std::expected<AccessToken, std::error_code>;

/// <summary>
/// A scope list shared between calls, so callers that ask for the same scopes on every request (and caches
/// that only compare them) need not copy the strings.
/// </summary>
using ScopeList = std::shared_ptr<const std::vector<std::string>>;

/// <summary>
/// A token credential is any type with
/// <c>boost::asio::awaitable&lt;TokenResult&gt; GetTokenAsync(std::vector&lt;std::string&gt; scopes)</c>.
/// A credential may additionally provide <c>awaitable&lt;TokenResult&gt; GetTokenForScopesAsync(ScopeList)</c>,
/// which the clients prefer because it avoids copying the scope list on every request.
/// </summary>
/// <typeparam name="T">The type to test.</typeparam>
template <class T>
concept TokenCredentialLike = requires(T& credential, std::vector<std::string> scopes) {
    { credential.GetTokenAsync(std::move(scopes)) } -> std::same_as<boost::asio::awaitable<TokenResult>>;
};

/// <summary>
/// Requests a token for <paramref name="scopes"/> through the shared-scope overload when the credential has one.
/// </summary>
/// <typeparam name="TCred">The credential type.</typeparam>
/// <param name="credential">The credential to ask.</param>
/// <param name="scopes">The scopes the token must cover; null is treated as empty.</param>
/// <returns>The credential's result.</returns>
template <TokenCredentialLike TCred>
[[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenForScopes(TCred& credential, ScopeList scopes) {
    if constexpr (requires { credential.GetTokenForScopesAsync(scopes); }) {
        co_return co_await credential.GetTokenForScopesAsync(std::move(scopes));
    } else {
        co_return co_await credential.GetTokenAsync(scopes ? *scopes : std::vector<std::string>{});
    }
}

/// <summary>
/// The credential of clients that are not given one: every request fails with std::errc::operation_not_permitted.
/// Clients authorized by SAS or Shared Key use it as their TCred.
/// </summary>
class NoTokenCredential {
  public:
    /// <summary>
    /// Always fails, because there is no credential to ask.
    /// </summary>
    /// <param name="scopes">Ignored.</param>
    /// <returns>An error of <c>std::errc::operation_not_permitted</c>.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> /*scopes*/) {
        co_return std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
    }
};

/// <summary>
/// "Now" for expiry, refresh-window and backoff decisions of <c>CachingTokenCredential</c>.
/// </summary>
struct SystemTokenClock {
    /// <summary>
    /// Reads the system clock.
    /// </summary>
    /// <returns>The current time.</returns>
    [[nodiscard]] std::chrono::system_clock::time_point operator()() const noexcept {
        return std::chrono::system_clock::now();
    }
};

/// <summary>
/// A clock is any callable that returns a <c>std::chrono::system_clock::time_point</c>; tests substitute their own.
/// </summary>
/// <typeparam name="T">The type to test.</typeparam>
template <class T>
concept TokenClockLike =
    std::invocable<T&> && std::convertible_to<std::invoke_result_t<T&>, std::chrono::system_clock::time_point>;

/// <summary>
/// Caches the inner credential's token. The cache holds one token keyed by the exact (ordered) scope
/// list; a request with different scopes does not reuse it. A refresh starts once less than
/// <c>refreshWindow</c> remains before expiry (the stale-but-valid token is served meanwhile), concurrent
/// requests for the same scopes coalesce into a single inner request, and after a failed refresh of a
/// still-valid token the cached token is reused for <c>DefaultTokenCredentialFailedRefreshBackoff</c> (30 s)
/// before another refresh is attempted. When there is no valid token at all, a failure is remembered for
/// <c>DefaultTokenCredentialNegativeCacheWindow</c> (5 s) and returned without calling the inner credential.
///
/// Thread safety: <c>GetTokenAsync</c> may be called concurrently from several threads; internal state is
/// mutex-protected. The first caller without a usable token performs the inner request inside its own
/// coroutine and the others wait for its outcome; a background refresh runs as a detached coroutine on the
/// executor of the caller that triggered it. If the leading caller is cancelled, the waiters ask again.
/// </summary>
/// <typeparam name="TInner">The credential whose tokens are cached.</typeparam>
/// <typeparam name="TClock">The clock used for expiry and backoff decisions.</typeparam>
template <TokenCredentialLike TInner, TokenClockLike TClock = SystemTokenClock>
class CachingTokenCredential : public std::enable_shared_from_this<CachingTokenCredential<TInner, TClock>> {
    /// <summary>
    /// Keeps the constructor unusable outside Create while still allowing std::make_shared.
    /// </summary>
    struct CreateKey {
        explicit CreateKey() = default;
    };
    /// <summary>
    /// Waiters of one refresh; erased because callers resume on their own executors. Completed with an
    /// error_code so a cancelled wait can be told apart from a normal completion (see <c>WaitFor</c>).
    /// </summary>
    using Waiter = boost::asio::any_completion_handler<void(boost::system::error_code)>;
    /// <summary>
    /// Identifies one waiter registration in <c>RefreshState::Waiters</c> so a cancellation can find and remove
    /// exactly that entry; the type-erased Waiter itself cannot be compared or inspected once stored.
    /// </summary>
    using WaiterId = std::shared_ptr<void>;

  public:
    /// <summary>
    /// The cache relies on shared ownership, so instances can only be created through this factory.
    /// </summary>
    /// <param name="inner">The credential to cache.</param>
    /// <param name="refreshWindow">How long before expiry a refresh starts.</param>
    /// <param name="clock">The clock to read the time from.</param>
    /// <returns>The new cache.</returns>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="refreshWindow"/> is negative.</exception>
    [[nodiscard]] static std::shared_ptr<CachingTokenCredential>
    Create(std::shared_ptr<TInner> inner, std::chrono::seconds refreshWindow = DefaultTokenCredentialRefreshWindow,
           TClock clock = {}) {
        return std::make_shared<CachingTokenCredential>(CreateKey{}, std::move(inner), refreshWindow, std::move(clock));
    }

    /// <summary>
    /// Builds the cache; reachable only through <c>Create</c>.
    /// </summary>
    /// <param name="key">Proof that the call came from <c>Create</c>.</param>
    /// <param name="inner">The credential to cache.</param>
    /// <param name="refreshWindow">How long before expiry a refresh starts.</param>
    /// <param name="clock">The clock to read the time from.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="refreshWindow"/> is negative.</exception>
    CachingTokenCredential(CreateKey, std::shared_ptr<TInner> inner, std::chrono::seconds refreshWindow, TClock clock)
        : m_inner(std::move(inner)), m_refreshWindow(refreshWindow), m_clock(std::move(clock)) {
        // A negative window makes every token look about to expire, so each request would start a refresh.
        if (m_refreshWindow < std::chrono::seconds::zero()) {
            throw std::invalid_argument("CachingTokenCredential refreshWindow must not be negative.");
        }
    }

    /// <summary>
    /// Serves the token from the cache, refreshing it from the inner credential when needed.
    /// </summary>
    /// <param name="scopes">The scopes the token must cover.</param>
    /// <returns>The cached or freshly acquired token, or the inner credential's failure.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        co_return co_await GetTokenForScopesAsync(std::make_shared<const std::vector<std::string>>(std::move(scopes)));
    }

    /// <summary>
    /// As <c>GetTokenAsync</c>, but the cache-hit path compares against the shared list without copying it.
    /// </summary>
    /// <param name="scopes">The scopes the token must cover; null is treated as empty.</param>
    /// <returns>The cached or freshly acquired token, or the inner credential's failure.</returns>
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
            if (lookup.Role == RefreshRole::Leader) {
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

    /// <summary>
    /// Drops the cached token (and any remembered failure) for <paramref name="scopes"/>, so the next request asks
    /// the inner credential again. Used after the service rejected the token with a 401.
    /// </summary>
    /// <param name="scopes">The scopes whose entry is dropped.</param>
    void Invalidate(const ScopeList& scopes) { InvalidateIf(scopes, nullptr); }

    /// <summary>
    /// As above, but the cached token is dropped only while it still is <paramref name="rejectedToken"/>.
    /// Concurrent requests that were all rejected with the same token then cause a single refresh instead of
    /// discarding each other's result.
    /// </summary>
    /// <param name="scopes">The scopes whose entry is dropped.</param>
    /// <param name="rejectedToken">The token the service rejected.</param>
    void Invalidate(const ScopeList& scopes, std::string_view rejectedToken) { InvalidateIf(scopes, &rejectedToken); }

  private:
    /// <summary>Whether a refresh has published its outcome.</summary>
    enum class RefreshProgress : std::uint8_t { InFlight, Done };
    /// <summary>Whether a caller performs a refresh itself or waits for another caller's.</summary>
    enum class RefreshRole : std::uint8_t { Follower, Leader };
    /// <summary>Whether two scope lists hold the same scopes in the same order.</summary>
    enum class ScopeMatch : std::uint8_t { Different, Same };
    /// <summary>Whether the cache holds a token.</summary>
    enum class CacheEntry : std::uint8_t { Empty, Filled };

    /// <summary>
    /// One in-flight refresh: the scopes it is for, its outcome once done, and the callers waiting for it.
    /// </summary>
    struct RefreshState {
        ScopeList Scopes;
        std::mutex Mutex;
        std::vector<std::pair<WaiterId, Waiter>> Waiters;
        TokenResult Result = std::unexpected(std::make_error_code(std::errc::operation_canceled));
        RefreshProgress Progress = RefreshProgress::InFlight;
    };

    /// <summary>
    /// Result of consulting the cache under lock: a token or failure to serve right away (optionally alongside a
    /// background refresh to start), or a refresh to lead or to wait for.
    /// </summary>
    struct Lookup {
        std::optional<TokenResult> Immediate;
        std::shared_ptr<RefreshState> BackgroundRefresh;
        std::shared_ptr<RefreshState> State;
        RefreshRole Role = RefreshRole::Follower;
    };

    /// <summary>
    /// Drops the cached token and remembered failure for the scopes, as selected by the caller's overload.
    /// </summary>
    /// <param name="scopes">The scopes whose entry is dropped.</param>
    /// <param name="rejectedToken">When non-null, the cached token is dropped only if it equals this one.</param>
    void InvalidateIf(const ScopeList& scopes, const std::string_view* rejectedToken) {
        std::scoped_lock const lock(m_mutex);
        if (m_cacheEntry == CacheEntry::Filled && SameScopes(m_cachedScopes, scopes) == ScopeMatch::Same &&
            (rejectedToken == nullptr || m_cachedToken.Token == *rejectedToken)) {
            m_cacheEntry = CacheEntry::Empty;
            m_cachedToken = {};
            m_nextRefreshRetryAfter.reset();
        }
        if (SameScopes(m_failedScopes, scopes) == ScopeMatch::Same) {
            m_failureRetryAfter.reset();
        }
    }

    /// <summary>
    /// Callers normally share one list, so pointer identity answers the common case and the element-wise
    /// comparison only runs when two distinct lists are compared. A null list is an empty one.
    /// </summary>
    /// <param name="left">The first list.</param>
    /// <param name="right">The second list.</param>
    /// <returns>Whether the lists hold the same scopes in the same order.</returns>
    static ScopeMatch SameScopes(const ScopeList& left, const ScopeList& right) {
        if (left == right) {
            return ScopeMatch::Same;
        }
        static const std::vector<std::string> empty;
        return (left ? *left : empty) == (right ? *right : empty) ? ScopeMatch::Same : ScopeMatch::Different;
    }

    /// <summary>
    /// Registers a new in-flight refresh for the scopes. The caller must hold <c>m_mutex</c>.
    /// </summary>
    /// <param name="scopes">The scopes the refresh is for.</param>
    /// <returns>The state the refresh publishes its outcome to.</returns>
    [[nodiscard]] std::shared_ptr<RefreshState> NewRefreshState(const ScopeList& scopes) {
        auto state = std::make_shared<RefreshState>();
        state->Scopes = scopes;
        m_inFlightRefresh = state;
        return state;
    }

    /// <summary>
    /// Decides, under lock, whether the cache can answer the request and what refresh work it implies.
    /// </summary>
    /// <param name="scopes">The scopes the token must cover.</param>
    /// <returns>What to serve immediately, and which refresh to start, lead or wait for.</returns>
    Lookup LookUp(const ScopeList& scopes) {
        std::scoped_lock const lock(m_mutex);
        const auto now = m_clock();
        const auto tokenStillValid = m_cacheEntry == CacheEntry::Filled &&
                                     SameScopes(m_cachedScopes, scopes) == ScopeMatch::Same &&
                                     m_cachedToken.ExpiresOn > now;

        Lookup result;
        if (tokenStillValid && ((m_cachedToken.ExpiresOn - now) > m_refreshWindow ||
                                (m_nextRefreshRetryAfter.has_value() && now < *m_nextRefreshRetryAfter))) {
            result.Immediate = m_cachedToken;
            return result;
        }
        if (!tokenStillValid && m_failureRetryAfter.has_value() && now < *m_failureRetryAfter &&
            SameScopes(m_failedScopes, scopes) == ScopeMatch::Same) {
            result.Immediate = std::unexpected(m_lastFailure);
            return result;
        }
        if (tokenStillValid) {
            // Inside the refresh window: serve the still-valid token now and refresh in the background
            // (joining a refresh that is already running).
            result.Immediate = m_cachedToken;
            if (!m_inFlightRefresh || SameScopes(m_inFlightRefresh->Scopes, scopes) == ScopeMatch::Different) {
                result.BackgroundRefresh = NewRefreshState(scopes);
            }
            return result;
        }
        if (m_inFlightRefresh && SameScopes(m_inFlightRefresh->Scopes, scopes) == ScopeMatch::Same) {
            result.State = m_inFlightRefresh;
            return result;
        }
        result.State = NewRefreshState(scopes);
        result.Role = RefreshRole::Leader;
        return result;
    }

    /// <summary>
    /// Suspends until <paramref name="state"/> is done; resumes inline-free through post. Cancellable: a connected
    /// cancellation slot removes this waiter from <paramref name="state"/> (if Finish has not already claimed it)
    /// and completes it with operation_aborted, which is rethrown as a boost::system::system_error. Without
    /// this a follower bounded by <c>GetTokenForScopes(...) || Expire(limit)</c> would stay blocked until the
    /// leader's own refresh finished, past its own timeout.
    /// </summary>
    /// <param name="state">The refresh to wait for.</param>
    /// <exception cref="boost::system::system_error">Thrown when the wait is cancelled.</exception>
    static boost::asio::awaitable<void> WaitFor(std::shared_ptr<RefreshState> state) {
        auto token = boost::asio::as_tuple(boost::asio::use_awaitable);
        auto [ec] = co_await boost::asio::async_initiate<decltype(token), void(boost::system::error_code)>(
            [](auto handler, std::shared_ptr<RefreshState> st) {
                auto executor = boost::asio::get_associated_executor(handler);
                auto slot = boost::asio::get_associated_cancellation_slot(handler);
                WaiterId id = std::make_shared<char>();
                // Resuming an awaitable handler runs the coroutine in place, so hop to its executor first.
                Waiter waiter = [executor, handler = std::move(handler)](boost::system::error_code waitEc) mutable {
                    boost::asio::post(executor,
                                      [handler = std::move(handler), waitEc]() mutable { std::move(handler)(waitEc); });
                };
                if (slot.is_connected()) {
                    slot.assign([st, id](boost::asio::cancellation_type_t /*type*/) mutable {
                        // If the wait already completed normally, Finish has moved it out of st->Waiters
                        // and this lookup simply finds nothing to cancel.
                        Waiter fired;
                        bool found = false;
                        {
                            std::scoped_lock const lock(st->Mutex);
                            for (auto it = st->Waiters.begin(); it != st->Waiters.end(); ++it) {
                                if (it->first == id) {
                                    fired = std::move(it->second);
                                    st->Waiters.erase(it);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if (found) {
                            std::move(fired)(boost::asio::error::operation_aborted);
                        }
                    });
                }
                std::unique_lock lock(st->Mutex);
                if (st->Progress == RefreshProgress::Done) {
                    lock.unlock();
                    std::move(waiter)(boost::system::error_code{});
                    return;
                }
                st->Waiters.emplace_back(std::move(id), std::move(waiter));
            },
            token, std::move(state));
        if (ec) {
            throw boost::system::system_error(ec);
        }
    }

    /// <summary>
    /// Publishes a refresh's outcome and wakes every caller waiting for it.
    /// </summary>
    /// <param name="state">The refresh that completed.</param>
    /// <param name="result">Its outcome.</param>
    void Finish(const std::shared_ptr<RefreshState>& state, TokenResult result) {
        {
            std::scoped_lock const lock(m_mutex);
            if (m_inFlightRefresh == state) {
                m_inFlightRefresh.reset();
            }
        }
        std::vector<std::pair<WaiterId, Waiter>> waiters;
        {
            std::scoped_lock const lock(state->Mutex);
            state->Result = std::move(result);
            state->Progress = RefreshProgress::Done;
            waiters = std::move(state->Waiters);
        }
        for (auto& waiter : waiters) {
            std::move(waiter.second)(boost::system::error_code{});
        }
    }

    /// <summary>
    /// Asks the inner credential and publishes the outcome to the cache and to every waiter.
    /// </summary>
    /// <param name="self">Keeps the cache alive while the inner request is pending.</param>
    /// <param name="scopes">The scopes the token must cover.</param>
    /// <param name="state">The refresh this call performs.</param>
    /// <returns>The inner credential's result; an exception it throws is converted to an error code.</returns>
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
            } catch (const boost::system::system_error& e) {
                // boost::system::system_error derives from std::runtime_error, not std::system_error, so it
                // needs its own clause; otherwise a cancelled inner request (e.g. from cancel_after) would
                // fall through to the generic io_error catch below and be negative-cached as such.
                result = std::unexpected(e.code() == boost::asio::error::operation_aborted
                                             ? std::make_error_code(std::errc::operation_canceled)
                                             : std::error_code(e.code()));
            } catch (...) {
                result = std::unexpected(std::make_error_code(std::errc::io_error));
            }
        }
        self->Publish(scopes, result);
        self->Finish(state, result);
        co_return result;
    }

    /// <summary>
    /// Stores a token, or remembers a failure, in the cache. Caching is an optimisation: a failure to publish must
    /// not prevent delivering the outcome.
    /// </summary>
    /// <param name="scopes">The scopes the outcome is for.</param>
    /// <param name="result">The outcome to remember; a cancellation is not remembered.</param>
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
                m_cacheEntry = CacheEntry::Filled;
                m_nextRefreshRetryAfter.reset();
                m_failureRetryAfter.reset();
            } else {
                std::scoped_lock const lock(m_mutex);
                if (m_cacheEntry == CacheEntry::Filled && SameScopes(m_cachedScopes, scopes) == ScopeMatch::Same &&
                    m_cachedToken.ExpiresOn > now) {
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
    CacheEntry m_cacheEntry = CacheEntry::Empty;
    std::optional<std::chrono::system_clock::time_point> m_nextRefreshRetryAfter;
    std::optional<std::chrono::system_clock::time_point> m_failureRetryAfter;
    std::error_code m_lastFailure;
    ScopeList m_failedScopes;
};

/// <summary>
/// Deduces the inner credential and clock types.
/// </summary>
/// <typeparam name="TInner">The credential whose tokens are cached.</typeparam>
/// <typeparam name="TClock">The clock used for expiry and backoff decisions.</typeparam>
/// <param name="inner">The credential to cache.</param>
/// <param name="refreshWindow">How long before expiry a refresh starts.</param>
/// <param name="clock">The clock to read the time from.</param>
/// <returns>The new cache.</returns>
/// <exception cref="std::invalid_argument">Thrown when <paramref name="refreshWindow"/> is negative.</exception>
template <TokenCredentialLike TInner, TokenClockLike TClock = SystemTokenClock>
[[nodiscard]] std::shared_ptr<CachingTokenCredential<TInner, TClock>>
MakeCachingTokenCredential(std::shared_ptr<TInner> inner,
                           std::chrono::seconds refreshWindow = DefaultTokenCredentialRefreshWindow,
                           TClock clock = {}) {
    return CachingTokenCredential<TInner, TClock>::Create(std::move(inner), refreshWindow, std::move(clock));
}

/// <summary>
/// Whether a type is already a <c>CachingTokenCredential</c>.
/// </summary>
/// <typeparam name="T">The type to test.</typeparam>
template <class T> struct IsCachingTokenCredential : std::false_type {};

template <class TInner, class TClock>
struct IsCachingTokenCredential<CachingTokenCredential<TInner, TClock>> : std::true_type {};

/// <summary>
/// The credential a client actually uses for TCred: TCred itself when it already caches, otherwise TCred
/// wrapped in a <c>CachingTokenCredential</c>.
/// </summary>
/// <typeparam name="TCred">The credential type a client was given.</typeparam>
template <TokenCredentialLike TCred>
using CachedTokenCredential =
    std::conditional_t<IsCachingTokenCredential<TCred>::value, TCred, CachingTokenCredential<TCred>>;
} // namespace AVEVA::AzureClient
