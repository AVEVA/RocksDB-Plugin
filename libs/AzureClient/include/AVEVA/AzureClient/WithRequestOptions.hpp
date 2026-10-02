#pragma once

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <boost/asio/deferred.hpp>

#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient
{
    // Completion-token adapter carrying per-call HttpRequestOptions (timeout, response body limit,
    // cancellation slot) so the completion token can stay the last argument:
    //
    //     co_await client.DeleteAsync(WithRequestOptions(options));                    // deferred
    //     client.DeleteAsync(WithRequestOptions(options, boost::asio::use_future));    // any token
    //
    // Options supplied this way replace the client's DefaultRequestOptions for that call (and take
    // precedence over the legacy trailing requestOptions argument).
    template <class CompletionToken> struct RequestOptionsToken
    {
        HttpRequestOptions Options;
        CompletionToken Token;
    };

    template <class CompletionToken = boost::asio::deferred_t>
    [[nodiscard]] RequestOptionsToken<std::decay_t<CompletionToken>> WithRequestOptions(HttpRequestOptions options,
        CompletionToken&& token = CompletionToken{})
    {
        return {std::move(options), std::forward<CompletionToken>(token)};
    }

    template <class T> inline constexpr bool IsRequestOptionsToken = false;

    template <class CompletionToken>
    inline constexpr bool IsRequestOptionsToken<RequestOptionsToken<CompletionToken>> = true;
} // namespace AVEVA::AzureClient
