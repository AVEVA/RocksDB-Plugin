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
#include <gsl/util>
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
            gsl::narrow_cast<std::chrono::milliseconds::rep>(std::min<std::uint64_t>(*ms, MaxMilliseconds))};
    }
    if (const auto seconds = ParseHeaderUnsigned(FindHeaderValue(response, "Retry-After")); seconds.has_value()) {
        return std::chrono::milliseconds{gsl::narrow_cast<std::chrono::milliseconds::rep>(
            std::min<std::uint64_t>(*seconds, MaxMilliseconds / MillisecondsPerSecond) * MillisecondsPerSecond)};
    }
    // The HTTP-date form; a date in the past means "retry now". The server's Date header is the reference when
    // present so that client clock skew does not distort the delay.
    if (const auto date = ParseHttpDateHeader(FindHeaderValue(response, "Retry-After")); date.has_value()) {
        const auto serverNow = ParseHttpDateHeader(FindHeaderValue(response, "Date"));
        // Coarsen each timestamp before subtracting: two valid nanosecond-clock dates can differ by more than the
        // clock can represent.
        const auto wait = std::chrono::time_point_cast<std::chrono::milliseconds>(*date) -
                          std::chrono::time_point_cast<std::chrono::milliseconds>(
                              serverNow.value_or(std::chrono::system_clock::now()));
        return std::clamp(wait, std::chrono::milliseconds{0},
                          std::chrono::milliseconds{gsl::narrow_cast<std::chrono::milliseconds::rep>(MaxMilliseconds)});
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
    const auto it = std::ranges::find_if(headers, [](const HttpHeader& header) {
        return IEquals(header.GetName(), XMsCopySourceAuthorizationHeaderName) &&
               header.GetValue() == CopySourceClientCredentialMarker;
    });
    if (it == headers.end()) {
        return;
    }
    if (bearerToken.empty()) {
        headers.erase(it);
    } else {
        it->SetValue(MakeBearerValue(bearerToken));
    }
}

[[nodiscard]] std::error_code CanceledError() noexcept { return std::make_error_code(std::errc::operation_canceled); }

void SetBearerAuthorization(HttpRequest& request, std::string_view token) {
    std::erase_if(request.GetHeaders(),
                  [](const HttpHeader& header) { return IEquals(header.GetName(), AuthorizationHeaderName); });
    request.AddHeader(HttpHeader{std::string{AuthorizationHeaderName}, MakeBearerValue(token)});
}

[[nodiscard]] bool IsReplaySafe(const HttpRequest& request) {
    if (request.GetMethod() == HttpMethod::Delete) {
        return false;
    }
    const bool isRead = request.GetMethod() == HttpMethod::Get || request.GetMethod() == HttpMethod::Head;
    // A successful conditional mutation changes the ETag and last-modified time, so replaying it with the original
    // If-Match / If-Unmodified-Since would answer 412. A wildcard If-Match stays satisfied and reads change nothing.
    return std::ranges::none_of(request.GetHeaders(), [isRead](const HttpHeader& header) {
        if (IEquals(header.GetName(), IfNoneMatchHeaderName)) {
            return header.GetValue() == "*";
        }
        if (isRead) {
            return false;
        }
        return IEquals(header.GetName(), IfUnmodifiedSinceHeaderName) ||
               (IEquals(header.GetName(), IfMatchHeaderName) && header.GetValue() != "*");
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
