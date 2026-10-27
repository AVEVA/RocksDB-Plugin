// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "BlobStorageErrorCategory.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <system_error>

namespace AVEVA::AzureClient::Private {
namespace {
struct ErrorCodeEntry {
    std::string_view Name;
    BlobStorageErrorCode Code;
    std::string_view Message;
};

// Sorted by Name (ordinal) so ParseBlobStorageErrorCode can binary-search it.
constexpr std::array KnownCodes{
    ErrorCodeEntry{.Name = "AppendPositionConditionNotMet",
                   .Code = BlobStorageErrorCode::AppendPositionConditionNotMet,
                   .Message = "The append position condition specified was not met"},
    ErrorCodeEntry{.Name = "AuthenticationFailed",
                   .Code = BlobStorageErrorCode::AuthenticationFailed,
                   .Message = "Server failed to authenticate the request"},
    ErrorCodeEntry{.Name = "AuthorizationFailure",
                   .Code = BlobStorageErrorCode::AuthorizationFailure,
                   .Message = "The caller is not authorized to perform this operation"},
    ErrorCodeEntry{.Name = "AuthorizationPermissionMismatch",
                   .Code = BlobStorageErrorCode::AuthorizationPermissionMismatch,
                   .Message = "This request is not authorized to perform this operation using this permission"},
    ErrorCodeEntry{.Name = "BlobAlreadyExists",
                   .Code = BlobStorageErrorCode::BlobAlreadyExists,
                   .Message = "The specified blob already exists"},
    ErrorCodeEntry{.Name = "BlobArchived",
                   .Code = BlobStorageErrorCode::BlobArchived,
                   .Message = "The operation is not permitted on an archived blob"},
    ErrorCodeEntry{.Name = "BlobNotFound",
                   .Code = BlobStorageErrorCode::BlobNotFound,
                   .Message = "The specified blob does not exist"},
    ErrorCodeEntry{.Name = "BlobTierInadequateForContentLength",
                   .Code = BlobStorageErrorCode::BlobTierInadequateForContentLength,
                   .Message = "The specified blob tier size limit cannot be less than the content length"},
    ErrorCodeEntry{.Name = "ConditionNotMet",
                   .Code = BlobStorageErrorCode::ConditionNotMet,
                   .Message = "The condition specified in the request was not met"},
    ErrorCodeEntry{.Name = "ContainerAlreadyExists",
                   .Code = BlobStorageErrorCode::ContainerAlreadyExists,
                   .Message = "The specified container already exists"},
    ErrorCodeEntry{.Name = "ContainerBeingDeleted",
                   .Code = BlobStorageErrorCode::ContainerBeingDeleted,
                   .Message = "The specified container is being deleted"},
    ErrorCodeEntry{.Name = "ContainerDisabled",
                   .Code = BlobStorageErrorCode::ContainerDisabled,
                   .Message = "The specified container has been disabled"},
    ErrorCodeEntry{.Name = "ContainerNotFound",
                   .Code = BlobStorageErrorCode::ContainerNotFound,
                   .Message = "The specified container does not exist"},
    ErrorCodeEntry{.Name = "InternalError",
                   .Code = BlobStorageErrorCode::InternalError,
                   .Message = "The server encountered an internal error"},
    ErrorCodeEntry{.Name = "InvalidAuthenticationInfo",
                   .Code = BlobStorageErrorCode::InvalidAuthenticationInfo,
                   .Message = "The authentication information was not provided in the correct format"},
    ErrorCodeEntry{.Name = "InvalidBlobOrBlock",
                   .Code = BlobStorageErrorCode::InvalidBlobOrBlock,
                   .Message = "The specified blob or block content is invalid"},
    ErrorCodeEntry{.Name = "InvalidBlockList",
                   .Code = BlobStorageErrorCode::InvalidBlockList,
                   .Message = "The specified block list is invalid"},
    ErrorCodeEntry{.Name = "InvalidHeaderValue",
                   .Code = BlobStorageErrorCode::InvalidHeaderValue,
                   .Message = "The value provided for one of the HTTP headers was not in the correct format"},
    ErrorCodeEntry{.Name = "InvalidMetadata",
                   .Code = BlobStorageErrorCode::InvalidMetadata,
                   .Message = "The metadata specified is invalid; names must be valid C# identifiers"},
    ErrorCodeEntry{.Name = "InvalidPageRange",
                   .Code = BlobStorageErrorCode::InvalidPageRange,
                   .Message = "The page range specified is invalid"},
    ErrorCodeEntry{.Name = "InvalidQueryParameterValue",
                   .Code = BlobStorageErrorCode::InvalidQueryParameterValue,
                   .Message = "An invalid value was specified for one of the query parameters"},
    ErrorCodeEntry{.Name = "InvalidRange",
                   .Code = BlobStorageErrorCode::InvalidRange,
                   .Message = "The range specified is invalid for the current size of the resource"},
    ErrorCodeEntry{.Name = "InvalidResourceName",
                   .Code = BlobStorageErrorCode::InvalidResourceName,
                   .Message = "The specified resource name is invalid"},
    ErrorCodeEntry{.Name = "LeaseAlreadyBroken",
                   .Code = BlobStorageErrorCode::LeaseAlreadyBroken,
                   .Message = "The lease has already been broken and cannot be broken again"},
    ErrorCodeEntry{.Name = "LeaseAlreadyPresent",
                   .Code = BlobStorageErrorCode::LeaseAlreadyPresent,
                   .Message = "There is already a lease present"},
    ErrorCodeEntry{.Name = "LeaseIdMismatchWithBlobOperation",
                   .Code = BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation,
                   .Message = "The lease ID specified did not match the lease ID for the blob"},
    ErrorCodeEntry{.Name = "LeaseIdMismatchWithLeaseOperation",
                   .Code = BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation,
                   .Message = "The lease ID specified did not match the lease ID for the blob"},
    ErrorCodeEntry{.Name = "LeaseIdMissing",
                   .Code = BlobStorageErrorCode::LeaseIdMissing,
                   .Message =
                       "There is currently a lease on the resource and no lease ID was specified in the request"},
    ErrorCodeEntry{.Name = "LeaseIsBreakingAndCannotBeAcquired",
                   .Code = BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired,
                   .Message = "The lease ID matched, but the lease is currently in breaking state and cannot be "
                              "acquired until it is broken"},
    ErrorCodeEntry{.Name = "LeaseIsBrokenAndCannotBeRenewed",
                   .Code = BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed,
                   .Message = "The lease ID matched, but the lease has been broken explicitly and cannot be renewed"},
    ErrorCodeEntry{.Name = "LeaseLost",
                   .Code = BlobStorageErrorCode::LeaseLost,
                   .Message = "A lease ID was specified, but the lease has expired"},
    ErrorCodeEntry{.Name = "LeaseNotPresentWithBlobOperation",
                   .Code = BlobStorageErrorCode::LeaseNotPresentWithBlobOperation,
                   .Message = "There is currently no lease on the blob"},
    ErrorCodeEntry{.Name = "LeaseNotPresentWithLeaseOperation",
                   .Code = BlobStorageErrorCode::LeaseNotPresentWithLeaseOperation,
                   .Message = "There is currently no lease on the blob"},
    ErrorCodeEntry{.Name = "MaxBlobSizeConditionNotMet",
                   .Code = BlobStorageErrorCode::MaxBlobSizeConditionNotMet,
                   .Message = "The max blob size condition specified was not met"},
    ErrorCodeEntry{.Name = "Md5Mismatch",
                   .Code = BlobStorageErrorCode::Md5Mismatch,
                   .Message =
                       "The MD5 value specified in the request did not match the MD5 value calculated by the server"},
    ErrorCodeEntry{.Name = "OperationTimedOut",
                   .Code = BlobStorageErrorCode::OperationTimedOut,
                   .Message = "The operation could not be completed within the permitted time"},
    ErrorCodeEntry{.Name = "RequestBodyTooLarge",
                   .Code = BlobStorageErrorCode::RequestBodyTooLarge,
                   .Message = "The request body is too large"},
    ErrorCodeEntry{.Name = "ResourceNotFound",
                   .Code = BlobStorageErrorCode::ResourceNotFound,
                   .Message = "The specified resource does not exist"},
    ErrorCodeEntry{.Name = "SequenceNumberConditionNotMet",
                   .Code = BlobStorageErrorCode::SequenceNumberConditionNotMet,
                   .Message = "The sequence number condition specified was not met"},
    ErrorCodeEntry{.Name = "ServerBusy",
                   .Code = BlobStorageErrorCode::ServerBusy,
                   .Message = "The server is currently unable to receive requests"},
    ErrorCodeEntry{.Name = "TargetConditionNotMet",
                   .Code = BlobStorageErrorCode::TargetConditionNotMet,
                   .Message = "The target condition specified using HTTP conditional header(s) is not met"},
};

static_assert(std::ranges::is_sorted(KnownCodes, {}, &ErrorCodeEntry::Name));

constexpr std::string_view ServiceErrorMessage = "The Blob Storage service returned an error response";
} // namespace

const char* BlobStorageErrorCategory::name() const noexcept { return "aveva.azure-client.blob-storage"; }

std::string BlobStorageErrorCategory::message(int value) const {
    const auto code = static_cast<BlobStorageErrorCode>(value);
    if (code == BlobStorageErrorCode::InvalidResponse) {
        return "The service response could not be parsed";
    }

    const auto it = std::ranges::find(KnownCodes, code, &ErrorCodeEntry::Code);
    return std::string{it != KnownCodes.end() ? it->Message : ServiceErrorMessage};
}

std::error_condition BlobStorageErrorCategory::default_error_condition(int value) const noexcept {
    switch (static_cast<BlobStorageErrorCode>(value)) {
    case BlobStorageErrorCode::ContainerNotFound:
    case BlobStorageErrorCode::ResourceNotFound:
    case BlobStorageErrorCode::BlobNotFound:
        return std::errc::no_such_file_or_directory;
    case BlobStorageErrorCode::ContainerAlreadyExists:
    case BlobStorageErrorCode::BlobAlreadyExists:
        return std::errc::file_exists;
    case BlobStorageErrorCode::AuthenticationFailed:
    case BlobStorageErrorCode::AuthorizationFailure:
    case BlobStorageErrorCode::InvalidAuthenticationInfo:
    case BlobStorageErrorCode::AuthorizationPermissionMismatch:
        return std::errc::permission_denied;
    case BlobStorageErrorCode::ContainerDisabled:
    case BlobStorageErrorCode::BlobArchived:
        return std::errc::permission_denied;
    case BlobStorageErrorCode::ServerBusy:
    case BlobStorageErrorCode::InternalError:
    case BlobStorageErrorCode::ContainerBeingDeleted:
        return std::errc::resource_unavailable_try_again;
    case BlobStorageErrorCode::LeaseIdMissing:
    case BlobStorageErrorCode::LeaseAlreadyPresent:
    case BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation:
    case BlobStorageErrorCode::LeaseNotPresentWithBlobOperation:
    case BlobStorageErrorCode::LeaseLost:
    case BlobStorageErrorCode::LeaseNotPresentWithLeaseOperation:
    case BlobStorageErrorCode::LeaseIdMismatchWithLeaseOperation:
    case BlobStorageErrorCode::LeaseIsBrokenAndCannotBeRenewed:
    case BlobStorageErrorCode::LeaseAlreadyBroken:
    case BlobStorageErrorCode::LeaseIsBreakingAndCannotBeAcquired:
        return std::errc::device_or_resource_busy;
    case BlobStorageErrorCode::ConditionNotMet:
    case BlobStorageErrorCode::TargetConditionNotMet:
    case BlobStorageErrorCode::AppendPositionConditionNotMet:
    case BlobStorageErrorCode::MaxBlobSizeConditionNotMet:
    case BlobStorageErrorCode::SequenceNumberConditionNotMet:
        return std::errc::operation_not_permitted;
    case BlobStorageErrorCode::InvalidRange:
    case BlobStorageErrorCode::InvalidPageRange:
        return std::errc::result_out_of_range;
    case BlobStorageErrorCode::InvalidHeaderValue:
    case BlobStorageErrorCode::InvalidQueryParameterValue:
    case BlobStorageErrorCode::InvalidResourceName:
    case BlobStorageErrorCode::InvalidBlockList:
    case BlobStorageErrorCode::InvalidBlobOrBlock:
        return std::errc::invalid_argument;
    case BlobStorageErrorCode::Md5Mismatch:
        return std::errc::io_error;
    case BlobStorageErrorCode::OperationTimedOut:
        return std::errc::timed_out;
    case BlobStorageErrorCode::RequestBodyTooLarge:
        return std::errc::file_too_large;
    case BlobStorageErrorCode::InvalidResponse:
        return std::errc::bad_message;
    case BlobStorageErrorCode::InvalidMetadata:
        return std::errc::invalid_argument;
    default:
        return {value, *this};
    }
}

BlobStorageErrorCode ParseBlobStorageErrorCode(std::string_view xMsErrorCode) {
    const auto it = std::ranges::lower_bound(KnownCodes, xMsErrorCode, {}, &ErrorCodeEntry::Name);
    return it != KnownCodes.end() && it->Name == xMsErrorCode ? it->Code : BlobStorageErrorCode::ServiceError;
}

const std::error_category& BlobStorageErrorCategoryInstance() noexcept {
    static const BlobStorageErrorCategory Category;
    return Category;
}
} // namespace AVEVA::AzureClient::Private

namespace AVEVA::AzureClient {
std::error_code make_error_code(BlobStorageErrorCode error) noexcept {
    return {static_cast<int>(error), Private::BlobStorageErrorCategoryInstance()};
}
} // namespace AVEVA::AzureClient
