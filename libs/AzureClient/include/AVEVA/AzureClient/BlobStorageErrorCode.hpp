// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstdint>
#include <system_error>

namespace AVEVA::AzureClient
{
    // Well-known Azure Blob Storage service error codes (subset), surfaced via the
    // "x-ms-error-code" response header when a request fails with a non-success status.
    // New enumerators are only ever appended, so existing values stay stable.
    //
    // The category maps codes onto portable conditions, so callers can write for example
    // `error == std::errc::no_such_file_or_directory` for any not-found code,
    // `std::errc::file_exists` for already-exists codes and `std::errc::permission_denied` for
    // authentication/authorization failures.
    enum class BlobStorageErrorCode : std::uint8_t
    {
        ContainerAlreadyExists = 1,
        ContainerNotFound,
        ContainerBeingDeleted,
        ContainerDisabled,
        InvalidResourceName,
        ResourceNotFound,
        AuthenticationFailed,
        AuthorizationFailure,
        InvalidAuthenticationInfo,
        ConditionNotMet,
        BlobNotFound,
        BlobAlreadyExists,
        InvalidBlobOrBlock,
        InvalidBlockList,
        // Any other non-success response, including redirects and unrecognized error codes.
        ServiceError,
        LeaseIdMissing,
        LeaseAlreadyPresent,
        LeaseIdMismatchWithBlobOperation,
        LeaseNotPresentWithBlobOperation,
        LeaseLost,
        ServerBusy,
        OperationTimedOut,
        InternalError,
        InvalidRange,
        TargetConditionNotMet,
        AppendPositionConditionNotMet,
        MaxBlobSizeConditionNotMet,
        BlobArchived,
        Md5Mismatch,
        InvalidHeaderValue,
        InvalidQueryParameterValue,
        RequestBodyTooLarge,
        BlobTierInadequateForContentLength,
        // A metadata name is not a valid C# identifier; also detected client-side before sending.
        // Maps to std::errc::invalid_argument.
        InvalidMetadata,
        // Client-side: a response reported success but could not be parsed. The parser's
        // diagnostic is in BlobStorageError::Message.
        InvalidResponse
    };

    // Found via ADL by std::error_code's constructor; do not call directly, compare
    // std::error_code values against BlobStorageErrorCode instead.
    std::error_code make_error_code(BlobStorageErrorCode error) noexcept;
} // namespace AVEVA::AzureClient

namespace std
{
    template <> struct is_error_code_enum<AVEVA::AzureClient::BlobStorageErrorCode> : true_type
    {
    };
} // namespace std
