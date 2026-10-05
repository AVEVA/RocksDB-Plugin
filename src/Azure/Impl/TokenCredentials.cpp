// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Environment.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/log/trivial.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <optional>
#include <sstream>
#include <string_view>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
using Clock = std::chrono::system_clock;

constexpr unsigned int g_tooManyRequests = 429;
constexpr unsigned int g_firstServerError = 500;
constexpr std::chrono::milliseconds g_initialRetryDelay{800};
constexpr std::chrono::milliseconds g_maxRetryDelay{std::chrono::seconds(30)};
constexpr std::int64_t g_maxTokenLifetimeSeconds = 24 * 60 * 60;

// application/x-www-form-urlencoded / query component encoding (RFC 3986 unreserved characters pass through).
std::string UrlEncode(std::string_view value) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(value.size());
    for (const char c : value) {
        const auto uc = static_cast<unsigned char>(c);
        if ((uc >= 'A' && uc <= 'Z') || (uc >= 'a' && uc <= 'z') || (uc >= '0' && uc <= '9') || uc == '-' ||
            uc == '_' || uc == '.' || uc == '~') {
            encoded.push_back(c);
        } else {
            encoded.push_back('%');
            encoded.push_back(hex[uc >> 4U]);
            encoded.push_back(hex[uc & 0x0FU]);
        }
    }
    return encoded;
}

bool IsSuccess(const HttpResponse& response) { return response.GetStatus() >= 200 && response.GetStatus() <= 299; }

std::optional<boost::property_tree::ptree> ParseJson(const std::string& body) {
    try {
        boost::property_tree::ptree tree;
        std::istringstream stream{body};
        boost::property_tree::read_json(stream, tree);
        return tree;
    } catch (const boost::property_tree::json_parser_error&) {
        return std::nullopt;
    }
}

std::error_code AuthenticationFailed() {
    return AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::AuthenticationFailed);
}

std::error_code InvalidResponse() {
    return AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::InvalidResponse);
}

void CompleteOnExecutor(IHttpClient& httpClient, AzureClient::ITokenCredential::GetTokenCompletionHandler completion,
                        std::error_code error, AzureClient::AccessToken token = {}) {
    boost::asio::post(httpClient.get_executor(),
                      [completion = std::move(completion), error, token = std::move(token)]() mutable {
                          completion(error, std::move(token));
                      });
}

// Parses a delta-seconds Retry-After header; HTTP-date values are ignored.
std::optional<std::chrono::milliseconds> RetryAfter(const HttpResponse& response) {
    for (const auto& header : response.GetHeaders()) {
        const auto& name = header.GetName();
        if (name.size() != 11 || !std::equal(name.begin(), name.end(), "retry-after", [](char a, char b) {
                return std::tolower(static_cast<unsigned char>(a)) == b;
            })) {
            continue;
        }
        const auto& value = header.GetValue();
        std::int64_t seconds = 0;
        const auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), seconds);
        if (ec == std::errc{} && seconds >= 0 && seconds <= g_maxTokenLifetimeSeconds) {
            return std::chrono::milliseconds(std::chrono::seconds(seconds));
        }
    }
    return std::nullopt;
}

// Sends a request and retries transport failures, throttling (429) and server errors (5xx) with exponential
// backoff; any other response is passed to the completion handler.
void SendWithRetry(IHttpClient& httpClient, HttpRequest request, HttpRequestOptions options, int retriesLeft,
                   std::chrono::milliseconds delay, IHttpClient::CompletionHandler completion) {
    auto attempt = request;
    httpClient.SendAsync(
        std::move(attempt),
        [&httpClient, request = std::move(request), options, retriesLeft, delay,
         completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
            const bool transient =
                error ? error != std::make_error_code(std::errc::operation_canceled)
                      : response.GetStatus() == g_tooManyRequests || response.GetStatus() >= g_firstServerError;
            if (!transient || retriesLeft <= 0) {
                completion(error, std::move(response));
                return;
            }

            auto wait = delay;
            if (!error && response.GetStatus() == g_tooManyRequests) {
                wait = std::clamp(RetryAfter(response).value_or(delay), delay, g_maxRetryDelay);
            }

            auto timer = std::make_shared<boost::asio::steady_timer>(httpClient.get_executor(), wait);
            timer->async_wait([timer, &httpClient, request = std::move(request), options, retriesLeft, delay,
                               completion = std::move(completion)](boost::system::error_code waitError) mutable {
                if (waitError == boost::asio::error::operation_aborted) {
                    completion(std::make_error_code(std::errc::operation_canceled), HttpResponse{});
                    return;
                }
                SendWithRetry(httpClient, std::move(request), options, retriesLeft - 1,
                              std::min(delay * 2, g_maxRetryDelay), std::move(completion));
            });
        },
        options);
}

std::expected<AzureClient::AccessToken, std::error_code> ParseAccessToken(std::error_code error,
                                                                          const HttpResponse& response) {
    if (error) {
        return std::unexpected(error);
    }

    if (!IsSuccess(response)) {
        return std::unexpected(AuthenticationFailed());
    }

    const auto tree = ParseJson(response.GetBody());
    if (!tree) {
        return std::unexpected(InvalidResponse());
    }

    AzureClient::AccessToken token;
    token.Token = tree->get<std::string>("access_token", "");
    const auto expiresInText = tree->get<std::string>("expires_in", "");
    std::int64_t expiresIn = 0;
    const auto [end, ec] =
        std::from_chars(expiresInText.data(), expiresInText.data() + expiresInText.size(), expiresIn);
    if (token.Token.empty() || ec != std::errc{} || end != expiresInText.data() + expiresInText.size() ||
        expiresIn <= 0 || expiresIn > g_maxTokenLifetimeSeconds) {
        return std::unexpected(InvalidResponse());
    }

    token.ExpiresOn = std::chrono::time_point_cast<Clock::duration>(Clock::now() + std::chrono::seconds(expiresIn));
    return token;
}
} // namespace

ChainedTokenCredential::ChainedTokenCredential(std::vector<std::shared_ptr<AzureClient::ITokenCredential>> sources)
    : m_sources(std::move(sources)) {
    std::erase(m_sources, nullptr);
}

void ChainedTokenCredential::GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) {
    TryGetToken(0, std::move(scopes), std::move(completion), AuthenticationFailed());
}

void ChainedTokenCredential::TryGetToken(std::size_t index, std::vector<std::string> scopes,
                                         GetTokenCompletionHandler completion, std::error_code lastError) {
    if (index >= m_sources.size()) {
        completion(lastError, AzureClient::AccessToken{});
        return;
    }

    auto source = m_sources[index];
    auto scopesCopy = scopes;
    source->GetTokenAsync(std::move(scopesCopy), [self = shared_from_this(), index, scopes = std::move(scopes),
                                                  completion = std::move(completion)](
                                                     std::error_code error, AzureClient::AccessToken token) mutable {
        if (!error) {
            completion(error, std::move(token));
            return;
        }

        BOOST_LOG_TRIVIAL(warning) << "Token credential source #" << index << " failed: " << error.message()
                                   << "; trying next source";
        self->TryGetToken(index + 1, std::move(scopes), std::move(completion), error);
    });
}

RuntimeBoundCredential::RuntimeBoundCredential(std::shared_ptr<ClientRuntime> runtime,
                                               std::shared_ptr<AzureClient::ITokenCredential> inner)
    : m_runtime(std::move(runtime)), m_inner(std::move(inner)) {}

void RuntimeBoundCredential::GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) {
    m_inner->GetTokenAsync(
        std::move(scopes), [runtime = m_runtime, inner = m_inner, completion = std::move(completion)](
                               std::error_code error, AzureClient::AccessToken token) mutable {
            completion(error, std::move(token));
            auto executor = runtime->HttpClient().get_executor();
            boost::asio::post(executor, [runtime = std::move(runtime), inner = std::move(inner)]() {});
        });
}

AzurePipelinesCredential::AzurePipelinesCredential(IHttpClient& httpClient, AzurePipelinesCredentialOptions options)
    : m_httpClient(&httpClient), m_options(std::move(options)) {
    if (m_options.OidcRequestUri.empty()) {
        m_options.OidcRequestUri = GetEnvironmentValue("SYSTEM_OIDCREQUESTURI");
    }
}

void AzurePipelinesCredential::GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) {
    if (m_options.OidcRequestUri.empty() || m_options.TenantId.empty() || m_options.ClientId.empty() ||
        m_options.ServiceConnectionId.empty() || m_options.SystemAccessToken.empty()) {
        CompleteOnExecutor(*m_httpClient, std::move(completion), std::make_error_code(std::errc::invalid_argument));
        return;
    }

    const auto separator = m_options.OidcRequestUri.find('?') == std::string::npos ? '?' : '&';
    HttpRequest request;
    request.SetMethod(HttpMethod::Post);
    request.SetUrl(m_options.OidcRequestUri + separator +
                   "api-version=7.1&serviceConnectionId=" + UrlEncode(m_options.ServiceConnectionId));
    request.AddHeader(HttpHeader{"Content-Type", "application/json"});
    request.AddHeader(HttpHeader{"Authorization", "Bearer " + m_options.SystemAccessToken});
    request.SetBody(std::string{});

    SendWithRetry(*m_httpClient, std::move(request), m_options.RequestOptions, m_options.MaxRetries,
                  g_initialRetryDelay,
                  [self = shared_from_this(), scopes = std::move(scopes),
                   completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
                      if (error) {
                          completion(error, AzureClient::AccessToken{});
                          return;
                      }

                      if (!IsSuccess(response)) {
                          completion(AuthenticationFailed(), AzureClient::AccessToken{});
                          return;
                      }

                      const auto tree = ParseJson(response.GetBody());
                      const auto oidcToken = tree ? tree->get<std::string>("oidcToken", "") : std::string{};
                      if (oidcToken.empty()) {
                          completion(InvalidResponse(), AzureClient::AccessToken{});
                          return;
                      }

                      self->ExchangeOidcToken(oidcToken, scopes, std::move(completion));
                  });
}

void AzurePipelinesCredential::ExchangeOidcToken(const std::string& oidcToken, const std::vector<std::string>& scopes,
                                                 GetTokenCompletionHandler completion) {
    std::string scope;
    for (const auto& s : scopes) {
        scope += scope.empty() ? "" : " ";
        scope += s;
    }

    auto authorityHost = m_options.AuthorityHost;
    if (!authorityHost.ends_with('/')) {
        authorityHost.push_back('/');
    }

    HttpRequest request;
    request.SetMethod(HttpMethod::Post);
    request.SetUrl(authorityHost + UrlEncode(m_options.TenantId) + "/oauth2/v2.0/token");
    request.AddHeader(HttpHeader{"Content-Type", "application/x-www-form-urlencoded"});
    request.SetBody("grant_type=client_credentials&client_id=" + UrlEncode(m_options.ClientId) +
                    "&client_assertion_type=" + UrlEncode("urn:ietf:params:oauth:client-assertion-type:jwt-bearer") +
                    "&client_assertion=" + UrlEncode(oidcToken) + "&scope=" + UrlEncode(scope));

    SendWithRetry(*m_httpClient, std::move(request), m_options.RequestOptions, m_options.MaxRetries,
                  g_initialRetryDelay,
                  [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
                      auto token = ParseAccessToken(error, response);
                      if (token.has_value()) {
                          completion(std::error_code{}, std::move(*token));
                      } else {
                          completion(token.error(), AzureClient::AccessToken{});
                      }
                  });
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
