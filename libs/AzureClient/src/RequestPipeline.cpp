// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
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
