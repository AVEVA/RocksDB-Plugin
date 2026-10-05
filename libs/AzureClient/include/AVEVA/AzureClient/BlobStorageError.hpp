#pragma once

#include <string>
#include <system_error>

namespace AVEVA::AzureClient
{
    // Invariants:
    //  - Code is always populated for a failure delivered through std::unexpected; its category() is the
    //    BlobStorageErrorCategory for service-reported failures, or the system/generic category for
    //    transport and client-side validation errors.
    //  - StatusCode is the HTTP status for service-reported failures and 0 for transport and client-side
    //    errors (no response was received or none was sent).
    //  - ErrorCode (the x-ms-error-code header, falling back to the XML body's Code) is empty when the
    //    service sent none and for transport/client-side errors. An unrecognized service code still sets
    //    ErrorCode verbatim, while Code maps to BlobStorageErrorCode::ServiceError.
    //  - RequestId (x-ms-request-id) is empty when no response was received or the header was absent.
    //  - Message is the service's message, or Code.message() for client-side errors.
    struct BlobStorageError
    {
        unsigned int StatusCode = 0;
        std::string ErrorCode;
        std::string Message;
        std::string RequestId;

        // The std::error_code classifying this failure. For server-reported failures this is the mapped
        // BlobStorageErrorCode; otherwise it is the transport error (e.g. a dropped connection or
        // timeout) or a client-side validation error such as std::errc::invalid_argument. Formerly
        // named TransportError.
        std::error_code Code;

        // The service's AuthenticationErrorDetail, which may echo the string-to-sign. It is kept out of Message so
        // it does not end up in logs and exception text; read it only when diagnosing a signature mismatch.
        std::string AuthenticationDetail;
    };

    // True when retrying the same request may succeed: transport failures such as timeouts and
    // dropped connections, and 408/429/500/502/503/504 responses (e.g. ServerBusy,
    // OperationTimedOut, InternalError). This is the classification the built-in retry policy uses.
    [[nodiscard]] bool IsTransient(const BlobStorageError& error) noexcept;
} // namespace AVEVA::AzureClient
