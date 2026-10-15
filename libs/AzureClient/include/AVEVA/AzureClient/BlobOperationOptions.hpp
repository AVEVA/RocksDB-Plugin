// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace AVEVA::AzureClient {
struct CreateBlobContainerOptions {
    Models::MetadataMap Metadata;
    Models::BlobRequestConditions Conditions;
};

struct ListBlobsOptions {
    std::string Prefix;
    std::string Delimiter;
    std::string Marker;
    // Count of items per page (not a total cap); the service allows 1..5000 and rejects larger values
    // with a service error; unset uses the service default (5000).
    std::optional<std::uint32_t> MaxResults;
    // Datasets to include in the listing (the `include` query parameter).
    bool IncludeMetadata = false;
    bool IncludeSnapshots = false;
    bool IncludeVersions = false;
    bool IncludeDeleted = false;
    bool IncludeTags = false;
    bool IncludeUncommittedBlobs = false;
    bool IncludeCopy = false;
};

struct CreatePageBlobOptions {
    Models::BlobHttpHeaders HttpHeaders;
    Models::MetadataMap Metadata;
    // Empty omits the x-ms-access-tier header (the service default applies).
    Models::AccessTier AccessTier;
    Models::BlobRequestConditions Conditions;
};

struct UploadPagesOptions {
    std::string ContentMd5;
    Models::BlobRequestConditions Conditions;
};

// Put Page From URL: the service copies a page range from another blob without the bytes passing through the client.
struct UploadPagesFromUrlOptions {
    // Sent as x-ms-source-if-match: fails the copy with 412 when the source ETag has changed.
    std::string SourceIfMatch;
    // The complete x-ms-copy-source-authorization value (for example "Bearer <token>") for a private source
    // that is not addressed with a SAS.
    std::string SourceAuthorization;
    // Authorizes the source with the same bearer token the client uses for the destination (renewed on
    // every retry). Needs a bearer-authorized client, with the source in a storage account that identity can
    // read; ignored when SourceAuthorization is set. A SAS or Shared Key client has no bearer token to forward:
    // address the source with a SAS URL instead.
    bool AuthorizeSourceWithClientCredential = false;
    // Conditions on the destination blob.
    Models::BlobRequestConditions Conditions;
};

struct GetPageRangesOptions {
    // Page-aligned window to inspect; unset covers the whole blob.
    std::optional<Models::BlobByteRange> Range;
    // NextMarker from a previous truncated result.
    std::string Marker;
    std::optional<std::uint32_t> MaxResults;
    Models::BlobRequestConditions Conditions;
};

struct DeleteBlobOptions {
    Models::BlobRequestConditions Conditions;
};

struct DeleteBlobsOptions {
    // Blob Batch requests (up to 256 deletes each) allowed in flight at once when more than 256 names
    // are given. Values below 1 are treated as 1.
    std::size_t MaxConcurrentRequests = 4;
};

struct GetBlobPropertiesOptions {
    Models::BlobRequestConditions Conditions;
};

struct DownloadBlobOptions {
    std::optional<Models::BlobByteRange> Range;
    Models::BlobRequestConditions Conditions;
    // Largest download accepted; a bigger blob (or range) fails with std::errc::file_too_large before any
    // body is buffered. Unset applies no limit beyond available memory.
    std::optional<std::uint64_t> MaxSize;
};

// Chunked download options. The first request is a ranged GET of ChunkSize bytes that also learns
// the blob's size and ETag; the remaining bytes are fetched in ChunkSize ranges with up to
// Concurrency requests in flight and written to the destination strictly in order (no seeking), so
// peak buffered memory is about Concurrency * ChunkSize. Every chunk after the first sends
// If-Match: <first ETag> (unless Conditions.IfMatch is set), so a concurrent overwrite fails with
// ConditionNotMet instead of producing a torn result. Each request's response-body limit is raised
// to at least ChunkSize plus a small margin.
// Default per-request chunk size used by the chunked download/upload helpers.
inline constexpr std::size_t DefaultChunkSize = std::size_t{4} * 1024U * 1024U; // 4 MiB

struct DownloadToOptions {
    std::optional<Models::BlobByteRange> Range;
    Models::BlobRequestConditions Conditions;
    std::size_t ChunkSize = DefaultChunkSize; // bytes; 0 selects the default (4 MiB); no upper bound
    // Count of concurrent chunk requests; 0 is treated as 1; no upper bound.
    std::size_t Concurrency = 1;
    // Largest download accepted; a bigger blob (or range) fails with std::errc::file_too_large before the
    // destination is written. Unset applies no limit.
    std::optional<std::uint64_t> MaxSize;
};

struct SetBlobMetadataOptions {
    Models::MetadataMap Metadata;
    Models::BlobRequestConditions Conditions;
};

struct AcquireLeaseOptions {
    Models::BlobRequestConditions Conditions;
    std::string ProposedLeaseId;
    // std::nullopt acquires an infinite lease; otherwise 15 to 60 seconds (validated client-side).
    std::optional<std::chrono::seconds> Duration;
};

// Conditions.LeaseId is ignored by every lease operation.
struct RenewLeaseOptions {
    std::string LeaseId;
    Models::BlobRequestConditions Conditions;
};

struct ReleaseLeaseOptions {
    std::string LeaseId;
    Models::BlobRequestConditions Conditions;
};

// Set Blob Properties with x-ms-blob-content-length: only the access conditions apply.
struct ResizePageBlobOptions {
    Models::BlobRequestConditions Conditions;
};

} // namespace AVEVA::AzureClient
