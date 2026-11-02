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
    // Only SP/HTAB are optional whitespace around a header field value (RFC 9110); other controls must fail.
    constexpr std::string_view OptionalWhitespace = " \t";
    const std::size_t first = value.find_first_not_of(OptionalWhitespace);
    value = first == std::string_view::npos
                ? std::string_view{}
                : value.substr(first, value.find_last_not_of(OptionalWhitespace) - first + 1U);
    return ParseExact<std::uint64_t>(value);
}
} // namespace

Retriability RetriabilityOfTransportError(std::error_code error) noexcept {
    if (error == HttpClientError::ResolveFailed || error == HttpClientError::ConnectFailed ||
        error == HttpClientError::WriteFailed || error == HttpClientError::ReadFailed ||
        error == HttpClientError::TimedOut || error == std::errc::timed_out || error == std::errc::connection_reset ||
        error == std::errc::connection_aborted || error == std::errc::connection_refused) {
        return Retriability::Retriable;
    }
    return Retriability::NotRetriable;
}

DeliveryCertainty DeliveryCertaintyOfTransportError(std::error_code error) noexcept {
    if (error == HttpClientError::WriteFailed || error == HttpClientError::ReadFailed ||
        error == HttpClientError::TimedOut || error == std::errc::timed_out || error == std::errc::connection_reset ||
        error == std::errc::connection_aborted) {
        return DeliveryCertainty::Ambiguous;
    }
    return DeliveryCertainty::Definite;
}

Retriability RetriabilityOfStatus(HttpStatusCode status) noexcept {
    if (status == HttpStatus::RequestTimeout || status == HttpStatus::TooManyRequests ||
        status == HttpStatus::InternalServerError || status == HttpStatus::BadGateway ||
        status == HttpStatus::ServiceUnavailable || status == HttpStatus::GatewayTimeout) {
        return Retriability::Retriable;
    }
    return Retriability::NotRetriable;
}

Retriability RetriabilityOfFailure(std::error_code error, const HttpResponse& response) noexcept {
    return error ? RetriabilityOfTransportError(error) : RetriabilityOfStatus(response.GetStatus());
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
        return IEquals(header.GetName(), XMsCopySourceAuthorizationHeaderName) == StringMatch::Match &&
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
    std::erase_if(request.GetHeaders(), [](const HttpHeader& header) {
        return IEquals(header.GetName(), AuthorizationHeaderName) == StringMatch::Match;
    });
    request.AddHeader(HttpHeader{std::string{AuthorizationHeaderName}, MakeBearerValue(token)});
}

[[nodiscard]] ReplaySafety ReplaySafetyOf(const HttpRequest& request) {
    if (request.GetMethod() == HttpMethod::Delete) {
        return ReplaySafety::Unsafe;
    }
    const auto method = request.GetMethod();
    const auto access = method == HttpMethod::Get || method == HttpMethod::Head ? AccessKind::Read : AccessKind::Write;
    // A successful conditional mutation changes the ETag and last-modified time, so replaying it with the original
    // If-Match / If-Unmodified-Since would answer 412. A wildcard If-Match stays satisfied and reads change nothing.
    const auto unsafe = std::ranges::any_of(request.GetHeaders(), [access](const HttpHeader& header) {
        if (IEquals(header.GetName(), IfNoneMatchHeaderName) == StringMatch::Match) {
            return header.GetValue() == "*";
        }
        if (access == AccessKind::Read) {
            return false;
        }
        return IEquals(header.GetName(), IfUnmodifiedSinceHeaderName) == StringMatch::Match ||
               (IEquals(header.GetName(), IfMatchHeaderName) == StringMatch::Match && header.GetValue() != "*");
    });
    return unsafe ? ReplaySafety::Unsafe : ReplaySafety::Safe;
}
} // namespace RetryDetail
} // namespace AVEVA::AzureClient::Private

namespace AVEVA::AzureClient {
Transience TransienceOf(const BlobStorageError& error) noexcept {
    if (error.StatusCode != HttpStatus::NoResponse) {
        return Private::RetriabilityOfStatus(error.StatusCode) == Retriability::Retriable ? Transience::Transient
                                                                                          : Transience::Permanent;
    }
    if (error.Code == BlobStorageErrorCode::ServerBusy || error.Code == BlobStorageErrorCode::OperationTimedOut ||
        error.Code == BlobStorageErrorCode::InternalError ||
        Private::RetriabilityOfTransportError(error.Code) == Retriability::Retriable) {
        return Transience::Transient;
    }
    return Transience::Permanent;
}
} // namespace AVEVA::AzureClient
