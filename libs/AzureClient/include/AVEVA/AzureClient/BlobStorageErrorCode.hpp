// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstdint>
#include <system_error>

namespace AVEVA::AzureClient {
/// <summary>
/// Well-known Azure Blob Storage service error codes (subset), surfaced via the
/// "x-ms-error-code" response header when a request fails with a non-success status.
/// New enumerators are only ever appended, so existing values stay stable.
///
/// The category maps codes onto portable conditions, so callers can write for example
/// <c>error == std::errc::no_such_file_or_directory</c> for any not-found code,
/// <c>std::errc::file_exists</c> for already-exists codes and <c>std::errc::permission_denied</c> for
/// authentication/authorization failures. Also: <c>resource_unavailable_try_again</c> (InternalError, ServerBusy,
/// ContainerBeingDeleted), <c>device_or_resource_busy</c> (Lease* codes), <c>operation_not_permitted</c>
/// (*ConditionNotMet), <c>result_out_of_range</c> (InvalidRange, InvalidPageRange), <c>invalid_argument</c>
/// (Invalid* header/query/name/block codes) and <c>io_error</c> (Md5Mismatch). A response with a missing or
/// unrecognized error code is classified from its HTTP status (403, 404, 412, 500, 503); other statuses yield
/// <c>ServiceError</c>.
/// </summary>
enum class BlobStorageErrorCode : std::uint8_t {
    /// <summary>The container being created already exists.</summary>
    ContainerAlreadyExists = 1,
    /// <summary>The container does not exist.</summary>
    ContainerNotFound,
    /// <summary>The container is being deleted.</summary>
    ContainerBeingDeleted,
    /// <summary>The container is disabled.</summary>
    ContainerDisabled,
    /// <summary>The name of a resource is not valid.</summary>
    InvalidResourceName,
    /// <summary>The specified resource does not exist.</summary>
    ResourceNotFound,
    /// <summary>The server failed to authenticate the request.</summary>
    AuthenticationFailed,
    /// <summary>The request was authenticated but is not authorized to perform the operation.</summary>
    AuthorizationFailure,
    /// <summary>The authentication information is not well formed.</summary>
    InvalidAuthenticationInfo,
    /// <summary>The condition specified by the request's conditional headers was not met.</summary>
    ConditionNotMet,
    /// <summary>The blob does not exist.</summary>
    BlobNotFound,
    /// <summary>The blob being created already exists.</summary>
    BlobAlreadyExists,
    /// <summary>The specified blob or block content is invalid.</summary>
    InvalidBlobOrBlock,
    /// <summary>The specified block list is invalid.</summary>
    InvalidBlockList,
    /// <summary>Any other non-success response, including redirects and unrecognized error codes.</summary>
    ServiceError,
    /// <summary>The blob has an active lease but the request specified no lease ID.</summary>
    LeaseIdMissing,
    /// <summary>The blob already has a lease.</summary>
    LeaseAlreadyPresent,
    /// <summary>The lease ID in the request does not match the blob's lease, for a blob operation.</summary>
    LeaseIdMismatchWithBlobOperation,
    /// <summary>The request specified a lease ID but the blob has no active lease, for a blob operation.</summary>
    LeaseNotPresentWithBlobOperation,
    /// <summary>A lease ID was specified but the lease has expired.</summary>
    LeaseLost,
    /// <summary>The server is busy.</summary>
    ServerBusy,
    /// <summary>The operation timed out on the server.</summary>
    OperationTimedOut,
    /// <summary>The server encountered an internal error.</summary>
    InternalError,
    /// <summary>The range specified is invalid for the current size of the resource.</summary>
    InvalidRange,
    /// <summary>The target condition specified by the request was not met.</summary>
    TargetConditionNotMet,
    /// <summary>The append position condition specified by the request was not met.</summary>
    AppendPositionConditionNotMet,
    /// <summary>The max blob size condition specified by the request was not met.</summary>
    MaxBlobSizeConditionNotMet,
    /// <summary>The operation is not permitted because the blob is archived.</summary>
    BlobArchived,
    /// <summary>The MD5 hash specified in the request does not match the one computed by the service.</summary>
    Md5Mismatch,
    /// <summary>The value of a header in the request is invalid.</summary>
    InvalidHeaderValue,
    /// <summary>The value of a query parameter in the request is invalid.</summary>
    InvalidQueryParameterValue,
    /// <summary>The request body is too large.</summary>
    RequestBodyTooLarge,
    /// <summary>The blob's tier is inadequate for the content length.</summary>
    BlobTierInadequateForContentLength,
    /// <summary>
    /// A metadata name is not a valid C# identifier; also detected client-side before sending.
    /// Maps to <c>std::errc::invalid_argument</c>.
    /// </summary>
    InvalidMetadata,
    /// <summary>
    /// Client-side: a response reported success but could not be parsed. The parser's
    /// diagnostic is in <c>BlobStorageError::Message</c>.
    /// </summary>
    InvalidResponse,
    /// <summary>
    /// 409 from Lease Blob: the lease being released/renewed/changed does not exist (e.g. already released).
    /// </summary>
    LeaseNotPresentWithLeaseOperation,
    /// <summary>The lease ID in the request does not match the blob's lease, for a lease operation.</summary>
    LeaseIdMismatchWithLeaseOperation,
    /// <summary>The lease has been broken and cannot be renewed.</summary>
    LeaseIsBrokenAndCannotBeRenewed,
    /// <summary>The lease has already been broken.</summary>
    LeaseAlreadyBroken,
    /// <summary>The lease is breaking and cannot be acquired.</summary>
    LeaseIsBreakingAndCannotBeAcquired,
    /// <summary>The sequence number condition specified by the request was not met.</summary>
    SequenceNumberConditionNotMet,
    /// <summary>The page range specified is invalid.</summary>
    InvalidPageRange,
    /// <summary>
    /// The identity is authenticated but lacks the RBAC permission. Maps to <c>std::errc::permission_denied</c>.
    /// </summary>
    AuthorizationPermissionMismatch
};

/// <summary>
/// Found via ADL by std::error_code's constructor; do not call directly, compare
/// std::error_code values against <see cref="BlobStorageErrorCode"/> instead.
/// </summary>
/// <param name="error">The error code to wrap.</param>
/// <returns>An error_code in the Blob Storage error category.</returns>
std::error_code make_error_code(BlobStorageErrorCode error) noexcept;
} // namespace AVEVA::AzureClient

namespace std {
template <> struct is_error_code_enum<AVEVA::AzureClient::BlobStorageErrorCode> : true_type {};
} // namespace std
