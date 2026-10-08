// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace AVEVA::AzureClient
{
    inline constexpr std::chrono::seconds DefaultTokenCredentialRefreshWindow{300};
    inline constexpr std::chrono::seconds DefaultTokenCredentialFailedRefreshBackoff{30};
    // After a failed acquisition with no usable token, the same failure is reported for this long without
    // calling the inner credential again, so a failing identity provider is not hammered by every request.
    inline constexpr std::chrono::seconds DefaultTokenCredentialNegativeCacheWindow{5};

    struct AccessToken
    {
        std::string Token;
        std::chrono::system_clock::time_point ExpiresOn = std::chrono::system_clock::time_point::max();
    };

    class ITokenCredential
    {
      public:
        // `completion` is invoked exactly once. Every configuration, transport and parsing failure is
        // reported through it as a non-zero std::error_code (the AccessToken is then unspecified); the
        // method itself does not throw for such failures. The completion may run inline, before
        // GetTokenAsync returns, or later on an executor or transport thread. Concrete credentials that
        // take an IHttpClient borrow it: it must outlive any in-flight token acquisition.
        using GetTokenCompletionHandler = std::move_only_function<void(std::error_code, AccessToken)>;

        ITokenCredential() = default;
        ITokenCredential(const ITokenCredential&) = delete;
        ITokenCredential& operator=(const ITokenCredential&) = delete;
        ITokenCredential(ITokenCredential&&) = delete;
        ITokenCredential& operator=(ITokenCredential&&) = delete;
        virtual ~ITokenCredential() = default;

        virtual void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) = 0;
    };

    // Returns a fixed token. The requested `scopes` are ignored.
    class StaticTokenCredential final : public ITokenCredential
    {
      public:
        // `executor`, if supplied, is used to post GetTokenAsync completions.
        explicit StaticTokenCredential(std::string token,
            std::chrono::system_clock::time_point expiresOn = std::chrono::system_clock::time_point::max(),
            std::optional<boost::asio::any_io_executor> executor = std::nullopt);

        void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override;

      private:
        std::string m_token;
        std::chrono::system_clock::time_point m_expiresOn;
        std::optional<boost::asio::any_io_executor> m_executor;
    };

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
    class CachingTokenCredential final : public ITokenCredential,
                                         public std::enable_shared_from_this<CachingTokenCredential>
    {
      public:
        // `clock` supplies "now" for expiry, refresh-window and backoff decisions; it defaults to
        // std::chrono::system_clock::now and exists so tests can use fixed timestamps.
        using Clock = std::function<std::chrono::system_clock::time_point()>;

      private:
        // Keeps the constructor unusable outside Create while still allowing std::make_shared.
        struct CreateKey
        {
            explicit CreateKey() = default;
        };

      public:
        // The cache relies on shared ownership, so instances can only be created through this factory.
        [[nodiscard]] static std::shared_ptr<CachingTokenCredential> Create(std::shared_ptr<ITokenCredential> inner,
            std::chrono::seconds refreshWindow = DefaultTokenCredentialRefreshWindow,
            std::optional<boost::asio::any_io_executor> executor = std::nullopt,
            Clock clock = {});

        CachingTokenCredential(CreateKey,
            std::shared_ptr<ITokenCredential> inner,
            std::chrono::seconds refreshWindow,
            std::optional<boost::asio::any_io_executor> executor,
            Clock clock);

        void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override;

      private:
        struct RefreshState
        {
            std::vector<std::string> Scopes;
            std::optional<boost::asio::any_io_executor> Executor;
            std::vector<GetTokenCompletionHandler> Completions;
            // Guards Completions; the state can outlive the credential that created it.
            std::mutex Mutex;
        };

        // Result of consulting the cache under lock: either a cached token to serve immediately, a refresh to
        // start (optionally alongside the cached token), or an indication that `completion` was already queued
        // onto an in-flight refresh and must not be used again by the caller.
        struct LookupResult
        {
            std::optional<AccessToken> CachedToken;
            // Set when a recent acquisition failed and there is no valid token to fall back to.
            std::optional<std::error_code> CachedFailure;
            std::shared_ptr<RefreshState> RefreshToStart;
            // Empty when the lookup took ownership of the caller's completion by queuing it onto a refresh.
            GetTokenCompletionHandler PendingCompletion;
        };

        LookupResult LookUpCachedTokenOrStartRefresh(const std::vector<std::string>& scopes,
            GetTokenCompletionHandler completion);
        void CompleteWithNoInnerCredentialError(const std::shared_ptr<RefreshState>& refreshState);
        void HandleInnerTokenResult(std::error_code error,
            AccessToken token,
            const std::shared_ptr<RefreshState>& refreshState,
            const std::vector<std::string>& scopes);
        std::vector<GetTokenCompletionHandler> DrainAndReset(const std::shared_ptr<RefreshState>& refreshState);

        std::shared_ptr<ITokenCredential> m_inner;
        std::chrono::seconds m_refreshWindow;
        std::optional<boost::asio::any_io_executor> m_executor;
        Clock m_clock;
        std::mutex m_mutex;
        std::shared_ptr<RefreshState> m_inFlightRefresh;
        std::vector<std::string> m_cachedScopes;
        AccessToken m_cachedToken;
        bool m_hasToken = false;
        std::optional<std::chrono::system_clock::time_point> m_nextRefreshRetryAfter;
        std::optional<std::chrono::system_clock::time_point> m_failureRetryAfter;
        std::error_code m_lastFailure;
        std::vector<std::string> m_failedScopes;
    };
} // namespace AVEVA::AzureClient
