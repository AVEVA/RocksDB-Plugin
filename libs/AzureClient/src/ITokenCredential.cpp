// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/ITokenCredential.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/append.hpp>

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient
{
    namespace
    {
        template <class THandler, class... Args>
        void CompleteOnExecutorIfSet(const std::optional<boost::asio::any_io_executor>& executor,
            THandler&& completion,
            Args&&... args)
        {
            if (executor.has_value())
            {
                boost::asio::post(*executor,
                    boost::asio::append(std::forward<THandler>(completion), std::forward<Args>(args)...));
            }
            else
            {
                std::forward<THandler>(completion)(std::forward<Args>(args)...);
            }
        }

        // Hands `token` to the last pending completion by move and copies it to the others, without the
        // move-inside-a-loop pattern that would leave `token` in a moved-from state mid-iteration.
        void CompleteAllOnExecutorIfSet(const std::optional<boost::asio::any_io_executor>& executor,
            std::vector<ITokenCredential::GetTokenCompletionHandler> completions,
            std::error_code error,
            AccessToken token)
        {
            if (completions.empty())
            {
                return;
            }

            ITokenCredential::GetTokenCompletionHandler last = std::move(completions.back());
            completions.pop_back();
            for (auto& pending : completions)
            {
                CompleteOnExecutorIfSet(executor, std::move(pending), error, token);
            }
            CompleteOnExecutorIfSet(executor, std::move(last), error, std::move(token));
        }
    } // namespace

    StaticTokenCredential::StaticTokenCredential(std::string token,
        std::chrono::system_clock::time_point expiresOn,
        std::optional<boost::asio::any_io_executor> executor)
        : m_token(std::move(token)), m_expiresOn(expiresOn), m_executor(std::move(executor))
    {
    }

    void StaticTokenCredential::GetTokenAsync(std::vector<std::string> /*scopes*/, GetTokenCompletionHandler completion)
    {
        CompleteOnExecutorIfSet(m_executor,
            std::move(completion),
            std::error_code{},
            AccessToken{.Token = m_token, .ExpiresOn = m_expiresOn});
    }

    std::shared_ptr<CachingTokenCredential> CachingTokenCredential::Create(std::shared_ptr<ITokenCredential> inner,
        std::chrono::seconds refreshWindow,
        std::optional<boost::asio::any_io_executor> executor,
        Clock clock)
    {
        return std::make_shared<CachingTokenCredential>(
            CreateKey{}, std::move(inner), refreshWindow, std::move(executor), std::move(clock));
    }

    CachingTokenCredential::CachingTokenCredential(CreateKey,
        std::shared_ptr<ITokenCredential> inner,
        std::chrono::seconds refreshWindow,
        std::optional<boost::asio::any_io_executor> executor,
        Clock clock)
        : m_inner(std::move(inner)), m_refreshWindow(refreshWindow), m_executor(std::move(executor)),
          m_clock(clock ? std::move(clock)
                        : Clock{[]
    {
        return std::chrono::system_clock::now();
    }})
    {
    }

    CachingTokenCredential::LookupResult CachingTokenCredential::LookUpCachedTokenOrStartRefresh(
        const std::vector<std::string>& scopes,
        GetTokenCompletionHandler completion)
    {
        std::scoped_lock const lock(m_mutex);
        const auto now = m_clock();
        const bool hasMatchingToken = m_hasToken && m_cachedScopes == scopes;
        const bool tokenStillValid = hasMatchingToken && m_cachedToken.ExpiresOn > now;
        const bool outsideRefreshWindow = tokenStillValid && (m_cachedToken.ExpiresOn - now) > m_refreshWindow;
        const bool refreshBackoffActive =
            tokenStillValid && m_nextRefreshRetryAfter.has_value() && now < *m_nextRefreshRetryAfter;

        if (outsideRefreshWindow || refreshBackoffActive)
        {
            LookupResult result;
            result.CachedToken = m_cachedToken;
            result.PendingCompletion = std::move(completion);
            return result;
        }

        if (!tokenStillValid && m_failureRetryAfter.has_value() && now < *m_failureRetryAfter &&
            m_failedScopes == scopes)
        {
            LookupResult result;
            result.CachedFailure = m_lastFailure;
            result.PendingCompletion = std::move(completion);
            return result;
        }

        if (tokenStillValid)
        {
            // Inside the refresh window: serve the still-valid token now and refresh in the background
            // (joining a refresh that is already running).
            LookupResult result;
            result.CachedToken = m_cachedToken;
            if (!m_inFlightRefresh || m_inFlightRefresh->Scopes != scopes)
            {
                result.RefreshToStart = std::make_shared<RefreshState>();
                result.RefreshToStart->Scopes = scopes;
                result.RefreshToStart->Executor = m_executor;
                m_inFlightRefresh = result.RefreshToStart;
            }
            result.PendingCompletion = std::move(completion);
            return result;
        }

        if (m_inFlightRefresh && m_inFlightRefresh->Scopes == scopes)
        {
            std::scoped_lock const stateLock(m_inFlightRefresh->Mutex);
            m_inFlightRefresh->Completions.push_back(std::move(completion));
            return {};
        }

        LookupResult result;
        result.RefreshToStart = std::make_shared<RefreshState>();
        result.RefreshToStart->Scopes = scopes;
        result.RefreshToStart->Executor = m_executor;
        result.RefreshToStart->Completions.push_back(std::move(completion));
        m_inFlightRefresh = result.RefreshToStart;
        return result;
    }

    void CachingTokenCredential::CompleteWithNoInnerCredentialError(const std::shared_ptr<RefreshState>& refreshState)
    {
        std::vector<GetTokenCompletionHandler> completions = DrainAndReset(refreshState);
        for (auto& pending : completions)
        {
            CompleteOnExecutorIfSet(refreshState->Executor,
                std::move(pending),
                std::make_error_code(std::errc::operation_not_permitted),
                AccessToken{});
        }
    }

    void CachingTokenCredential::HandleInnerTokenResult(std::error_code error,
        AccessToken token,
        const std::shared_ptr<RefreshState>& refreshState,
        const std::vector<std::string>& scopes)
    {
        const auto now = m_clock();
        {
            std::scoped_lock const lock(m_mutex);
            if (!error)
            {
                m_cachedScopes = scopes;
                m_cachedToken = token;
                m_hasToken = true;
                m_nextRefreshRetryAfter.reset();
                m_failureRetryAfter.reset();
            }
            else if (m_hasToken && m_cachedScopes == scopes && m_cachedToken.ExpiresOn > now)
            {
                m_nextRefreshRetryAfter = now + DefaultTokenCredentialFailedRefreshBackoff;
            }
            else
            {
                m_failureRetryAfter = now + DefaultTokenCredentialNegativeCacheWindow;
                m_lastFailure = error;
                m_failedScopes = scopes;
            }
        }

        std::vector<GetTokenCompletionHandler> completions = DrainAndReset(refreshState);
        CompleteAllOnExecutorIfSet(refreshState->Executor, std::move(completions), error, std::move(token));
    }

    void CachingTokenCredential::GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion)
    {
        const std::optional<boost::asio::any_io_executor> completionExecutor = m_executor;
        LookupResult lookup = LookUpCachedTokenOrStartRefresh(scopes, std::move(completion));
        if (lookup.CachedFailure.has_value())
        {
            CompleteOnExecutorIfSet(
                completionExecutor, std::move(lookup.PendingCompletion), *lookup.CachedFailure, AccessToken{});
            return;
        }
        if (!lookup.RefreshToStart && !lookup.CachedToken.has_value())
        {
            // The completion was queued onto a refresh that is already running; it will be invoked from there.
            return;
        }

        if (lookup.CachedToken.has_value() && !lookup.RefreshToStart)
        {
            CompleteOnExecutorIfSet(completionExecutor,
                std::move(lookup.PendingCompletion),
                std::error_code{},
                std::move(*lookup.CachedToken));
            return;
        }

        if (!m_inner)
        {
            CompleteWithNoInnerCredentialError(lookup.RefreshToStart);
            return;
        }

        std::vector<std::string> requestScopes = scopes;
        m_inner->GetTokenAsync(std::move(requestScopes),
            [weakSelf = weak_from_this(),
                refreshState = lookup.RefreshToStart,
                scopes = std::move(scopes)](std::error_code error, AccessToken token) mutable
        {
            if (const auto self = weakSelf.lock())
            {
                self->HandleInnerTokenResult(error, std::move(token), refreshState, scopes);
                return;
            }

            std::vector<GetTokenCompletionHandler> completions;
            {
                std::scoped_lock const stateLock(refreshState->Mutex);
                completions = std::move(refreshState->Completions);
            }
            CompleteAllOnExecutorIfSet(refreshState->Executor, std::move(completions), error, std::move(token));
        });

        // The background refresh is started first, so a re-entrant call from this completion joins it.
        if (lookup.CachedToken.has_value())
        {
            CompleteOnExecutorIfSet(completionExecutor,
                std::move(lookup.PendingCompletion),
                std::error_code{},
                std::move(*lookup.CachedToken));
        }
    }

    std::vector<ITokenCredential::GetTokenCompletionHandler> CachingTokenCredential::DrainAndReset(
        const std::shared_ptr<RefreshState>& refreshState)
    {
        std::scoped_lock const lock(m_mutex);
        if (m_inFlightRefresh == refreshState)
        {
            m_inFlightRefresh.reset();
        }
        std::scoped_lock const stateLock(refreshState->Mutex);
        return std::move(refreshState->Completions);
    }
} // namespace AVEVA::AzureClient