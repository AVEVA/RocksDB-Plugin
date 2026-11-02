// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Models/Identifiers.hpp"

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
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
/// Thrown when a blocking call is made from a thread that runs the io_context driving the Azure clients. Such a call
/// could never complete (or would race the completion it waits for), so it is refused before anything is started.
/// Translated to rocksdb::IOStatus::InvalidArgument: the caller broke the threading contract.
/// </summary>
class BlockingOnIoThreadException : public std::logic_error {
  public:
    using std::logic_error::logic_error;
};

/// <summary>The service's error code for a failed request (x-ms-error-code), e.g. "BlobNotFound"; may be
/// empty.</summary>
using ServiceErrorCode = Models::Identifier<struct ServiceErrorCodeTag>;
/// <summary>The human-readable description of a failure.</summary>
using ErrorMessage = Models::Identifier<struct ErrorMessageTag>;
/// <summary>The service's id for a request (x-ms-request-id), which Azure support asks for; may be empty.</summary>
using ServiceRequestId = Models::Identifier<struct ServiceRequestIdTag>;

/// <summary>
/// Thrown when a request to Azure Blob Storage fails (after the client's own retries).
/// StatusCode is HttpStatus::NoResponse when no HTTP response was received (transport, TLS or authentication errors).
/// what() describes the failure as "HTTP <status> <ErrorCode>: <message> (request id: <id>)".
/// </summary>
class RequestFailedException : public std::runtime_error {
  public:
    RequestFailedException(HttpStatusCode statusCode, ServiceErrorCode errorCode, ErrorMessage message,
                           ServiceRequestId requestId, std::error_code code, std::string authenticationDetail = {},
                           std::optional<std::chrono::milliseconds> retryAfter = std::nullopt)
        : std::runtime_error(FormatWhat(statusCode, errorCode.Value(), message.Value(), requestId.Value(), code)),
          m_statusCode(statusCode), m_errorCode(std::move(errorCode)), m_message(std::move(message)),
          m_requestId(std::move(requestId)), m_code(code), m_authenticationDetail(std::move(authenticationDetail)),
          m_retryAfter(retryAfter) {}

    [[nodiscard]] HttpStatusCode StatusCode() const noexcept { return m_statusCode; }
    [[nodiscard]] const ServiceErrorCode& ErrorCode() const noexcept { return m_errorCode; }
    [[nodiscard]] const ErrorMessage& Message() const noexcept { return m_message; }
    [[nodiscard]] const ServiceRequestId& RequestId() const noexcept { return m_requestId; }
    [[nodiscard]] std::error_code Code() const noexcept { return m_code; }
    /// Service diagnostics for a failed authentication. Kept out of what() because it can echo the string-to-sign;
    /// only log it at debug level.
    [[nodiscard]] const std::string& AuthenticationDetail() const noexcept { return m_authenticationDetail; }
    /// The service's retry hint (Retry-After), when the failed response carried one.
    [[nodiscard]] std::optional<std::chrono::milliseconds> RetryAfter() const noexcept { return m_retryAfter; }

  private:
    HttpStatusCode m_statusCode;
    ServiceErrorCode m_errorCode;
    ErrorMessage m_message;
    ServiceRequestId m_requestId;
    std::error_code m_code;
    std::string m_authenticationDetail;
    std::optional<std::chrono::milliseconds> m_retryAfter;

    // Builds what() once, so it stays valid for the exception's lifetime.
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
