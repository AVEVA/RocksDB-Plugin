// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
namespace AVEVA::RocksDB::Plugin::Azure {
/// <summary>An HTTP response status code.</summary>
class HttpStatusCode {
  public:
    constexpr HttpStatusCode() noexcept = default;
    explicit constexpr HttpStatusCode(const unsigned int value) noexcept : m_value(value) {}

    [[nodiscard]] constexpr unsigned int Value() const noexcept { return m_value; }

    friend constexpr bool operator==(HttpStatusCode, HttpStatusCode) noexcept = default;

  private:
    unsigned int m_value = 0;
};

/// <summary>
/// HTTP status codes the plugin reacts to when an Azure Blob Storage request fails.
/// </summary>
struct HttpStatus {
    /// No HTTP response was received (transport, TLS or authentication errors).
    static constexpr HttpStatusCode NoResponse{0};
    static constexpr HttpStatusCode BadRequest{400};
    static constexpr HttpStatusCode Unauthorized{401};
    static constexpr HttpStatusCode Forbidden{403};
    static constexpr HttpStatusCode NotFound{404};
    static constexpr HttpStatusCode RequestTimeout{408};
    static constexpr HttpStatusCode Conflict{409};
    static constexpr HttpStatusCode PreconditionFailed{412};
    static constexpr HttpStatusCode TooManyRequests{429};
    static constexpr HttpStatusCode InternalServerError{500};
    static constexpr HttpStatusCode BadGateway{502};
    static constexpr HttpStatusCode ServiceUnavailable{503};
    static constexpr HttpStatusCode GatewayTimeout{504};
};

/// <summary>
/// Thrown when the filesystem refuses an operation because it can no longer prove it holds its leases. Translated to
/// rocksdb::IOStatus::IOFenced, which RocksDB treats as non-retryable.
/// </summary>
class WritesFencedException : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

/// <summary>
/// Thrown when a request to Azure Blob Storage fails (after the client's own retries).
/// StatusCode is HttpStatus::NoResponse when no HTTP response was received (transport, TLS or authentication errors).
/// </summary>
class RequestFailedException : public std::runtime_error {
  public:
    RequestFailedException(HttpStatusCode statusCode, std::string errorCode, std::string message, std::string requestId,
                           std::error_code code, std::string authenticationDetail = {},
                           std::optional<std::chrono::milliseconds> retryAfter = std::nullopt)
        : std::runtime_error(FormatWhat(statusCode, errorCode, message, requestId, code)), StatusCode(statusCode),
          ErrorCode(std::move(errorCode)), Message(std::move(message)), RequestId(std::move(requestId)), Code(code),
          AuthenticationDetail(std::move(authenticationDetail)), RetryAfter(retryAfter) {}

    /// Describes the failure as "HTTP <status> <ErrorCode>: <message> (request id: <id>)"; the request id is what
    /// Azure support asks for. The text is built once, in the constructor, and is valid for the exception's lifetime.
    std::string_view Describe() const noexcept { return what(); }

    HttpStatusCode StatusCode;
    std::string ErrorCode;
    std::string Message;
    std::string RequestId;
    std::error_code Code;
    /// Service diagnostics for a failed authentication. Kept out of what() because it can echo the string-to-sign;
    /// only log it at debug level.
    std::string AuthenticationDetail;
    /// The service's retry hint (Retry-After), when the failed response carried one.
    std::optional<std::chrono::milliseconds> RetryAfter;
    static std::string FormatWhat(HttpStatusCode statusCode, const std::string& errorCode, const std::string& message,
                                  const std::string& requestId, const std::error_code& code) {
        const std::string codeMessage = message.empty() ? code.message() : std::string{};
        const std::string& text = message.empty() ? codeMessage : message;

        std::string what;
        what.reserve(text.size() + errorCode.size() + requestId.size() + 40);
        if (statusCode != HttpStatus::NoResponse) {
            what.append("HTTP ").append(std::to_string(statusCode.Value())).append(" ");
        }

        if (!errorCode.empty()) {
            what.append(errorCode).append(": ");
        }

        what.append(text);
        if (!requestId.empty()) {
            what.append(" (request id: ").append(requestId).append(")");
        }

        return what;
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure
