// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <stdexcept>
#include <string>
#include <system_error>
namespace AVEVA::RocksDB::Plugin::Azure {
/// <summary>
/// HTTP status codes the plugin reacts to when an Azure Blob Storage request fails.
/// </summary>
struct HttpStatus {
    static const constexpr unsigned int BadRequest = 400;
    static const constexpr unsigned int Unauthorized = 401;
    static const constexpr unsigned int Forbidden = 403;
    static const constexpr unsigned int NotFound = 404;
    static const constexpr unsigned int RequestTimeout = 408;
    static const constexpr unsigned int Conflict = 409;
    static const constexpr unsigned int PreconditionFailed = 412;
    static const constexpr unsigned int TooManyRequests = 429;
    static const constexpr unsigned int InternalServerError = 500;
    static const constexpr unsigned int BadGateway = 502;
    static const constexpr unsigned int ServiceUnavailable = 503;
    static const constexpr unsigned int GatewayTimeout = 504;
};

/// <summary>
/// Thrown when a request to Azure Blob Storage fails (after the client's own retries).
/// StatusCode is 0 when no HTTP response was received (transport, TLS or authentication errors).
/// </summary>
class RequestFailedException : public std::runtime_error {
  public:
    RequestFailedException(unsigned int statusCode, std::string errorCode, std::string message, std::string requestId,
                           std::error_code code)
        : std::runtime_error(FormatWhat(statusCode, errorCode, message, requestId, code)), StatusCode(statusCode),
          ErrorCode(std::move(errorCode)), Message(std::move(message)), RequestId(std::move(requestId)), Code(code) {}

    /// Describes the failure as "<status> <ErrorCode>: <message> (request id: <id>)"; the request id is what Azure
    /// support asks for.
    std::string Describe() const { return FormatWhat(StatusCode, ErrorCode, Message, RequestId, Code); }

    unsigned int StatusCode;
    std::string ErrorCode;
    std::string Message;
    std::string RequestId;
    std::error_code Code;

  private:
    static std::string FormatWhat(unsigned int statusCode, const std::string& errorCode, const std::string& message,
                                  const std::string& requestId, const std::error_code& code) {
        std::string what = message.empty() ? code.message() : message;
        if (!errorCode.empty()) {
            what = errorCode + ": " + what;
        }

        if (statusCode != 0) {
            what = std::to_string(statusCode) + " " + what;
        }

        if (!requestId.empty()) {
            what += " (request id: " + requestId + ")";
        }

        return what;
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure
