// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <system_error>

namespace AVEVA::AzureClient::Private {
namespace {
[[nodiscard]] std::optional<std::uint64_t> ParseHeaderUnsigned(std::string_view value) noexcept {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
        value.remove_suffix(1);
    }
    std::uint64_t parsed = 0;
    const auto [ptr, ec] = std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
    if (value.empty() || ec != std::errc{} || ptr != std::to_address(value.end())) {
        return std::nullopt;
    }
    return parsed;
}
} // namespace

bool IsRetriableTransportError(std::error_code error) noexcept {
    return error == HttpClientError::ResolveFailed || error == HttpClientError::ConnectFailed ||
           error == HttpClientError::WriteFailed || error == HttpClientError::ReadFailed ||
           error == HttpClientError::TimedOut || error == std::errc::timed_out ||
           error == std::errc::connection_reset || error == std::errc::connection_aborted ||
           error == std::errc::connection_refused;
}

bool IsAmbiguousTransportError(std::error_code error) noexcept {
    return error == HttpClientError::WriteFailed || error == HttpClientError::ReadFailed ||
           error == HttpClientError::TimedOut || error == std::errc::timed_out ||
           error == std::errc::connection_reset || error == std::errc::connection_aborted;
}

bool IsRetriableStatus(unsigned int status) noexcept {
    switch (status) {
    case HttpStatusRequestTimeout:
    case HttpStatusTooManyRequests:
    case HttpStatusInternalServerError:
    case HttpStatusBadGateway:
    case HttpStatusServiceUnavailable:
    case HttpStatusGatewayTimeout:
        return true;
    default:
        return false;
    }
}

bool IsRetriableFailure(std::error_code error, const HttpResponse& response) noexcept {
    return error ? IsRetriableTransportError(error) : IsRetriableStatus(response.GetStatus());
}

std::optional<std::chrono::milliseconds> ParseRetryAfter(const HttpResponse& response) noexcept {
    constexpr std::uint64_t MillisecondsPerSecond = 1000ULL;
    constexpr std::uint64_t MaxMilliseconds = 24ULL * 60ULL * 60ULL * 1000ULL;
    if (const auto ms = ParseHeaderUnsigned(FindHeaderValue(response, "x-ms-retry-after-ms")); ms.has_value()) {
        return std::chrono::milliseconds{
            static_cast<std::chrono::milliseconds::rep>(std::min<std::uint64_t>(*ms, MaxMilliseconds))};
    }
    if (const auto seconds = ParseHeaderUnsigned(FindHeaderValue(response, "Retry-After")); seconds.has_value()) {
        return std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(
            std::min<std::uint64_t>(*seconds, MaxMilliseconds / MillisecondsPerSecond) * MillisecondsPerSecond)};
    }
    // The HTTP-date form; a date in the past means "retry now".
    if (const auto date = ParseHttpDateHeader(FindHeaderValue(response, "Retry-After")); date.has_value()) {
        const auto wait =
            std::chrono::duration_cast<std::chrono::milliseconds>(*date - std::chrono::system_clock::now());
        return std::clamp(wait, std::chrono::milliseconds{0},
                          std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(MaxMilliseconds)});
    }
    return std::nullopt;
}

namespace RetryDetail {
[[nodiscard]] std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber) {
    thread_local std::mt19937_64 generator{std::random_device{}()};
    return ComputeBackoff(retry, retryNumber, generator);
}

void ResolveCopySourceAuthorization(HttpRequest& request, std::string_view bearerToken) {
    auto& headers = request.GetHeaders();
    for (auto it = headers.begin(); it != headers.end(); ++it) {
        if (!IEquals(it->GetName(), XMsCopySourceAuthorizationHeaderName) ||
            it->GetValue() != CopySourceClientCredentialMarker) {
            continue;
        }
        if (bearerToken.empty()) {
            headers.erase(it);
        } else {
            it->SetValue("Bearer " + std::string{bearerToken});
        }
        return;
    }
}

[[nodiscard]] std::error_code CanceledError() noexcept { return std::make_error_code(std::errc::operation_canceled); }

void SetBearerAuthorization(HttpRequest& request, std::string_view token) {
    std::erase_if(request.GetHeaders(),
                  [](const HttpHeader& header) { return IEquals(header.GetName(), AuthorizationHeaderName); });
    request.AddHeader(HttpHeader{std::string{AuthorizationHeaderName}, "Bearer " + std::string{token}});
}

[[nodiscard]] bool IsReplaySafe(const HttpRequest& request) {
    if (request.GetMethod() == HttpMethod::Delete) {
        return false;
    }
    return std::ranges::none_of(request.GetHeaders(), [](const HttpHeader& header) {
        return IEquals(header.GetName(), IfNoneMatchHeaderName) && header.GetValue() == "*";
    });
}
} // namespace RetryDetail
} // namespace AVEVA::AzureClient::Private

namespace AVEVA::AzureClient {
bool IsTransient(const BlobStorageError& error) noexcept {
    if (error.StatusCode != 0U) {
        return Private::IsRetriableStatus(error.StatusCode);
    }
    return error.Code == BlobStorageErrorCode::ServerBusy || error.Code == BlobStorageErrorCode::OperationTimedOut ||
           error.Code == BlobStorageErrorCode::InternalError || Private::IsRetriableTransportError(error.Code);
}
} // namespace AVEVA::AzureClient
