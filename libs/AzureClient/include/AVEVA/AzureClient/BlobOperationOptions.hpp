#pragma once
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace AVEVA::AzureClient
{
    struct CreateBlobContainerOptions
    {
        Models::MetadataMap Metadata;
        Models::BlobRequestConditions Conditions;
    };

    struct ListBlobsOptions
    {
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

    // Get User Delegation Key (requires TokenCredential/BearerToken authorization). ExpiresOn must be set and
    // within seven days of now; StartsOn defaults to now.
    struct GetUserDelegationKeyOptions
    {
        std::optional<std::chrono::system_clock::time_point> StartsOn;
        std::chrono::system_clock::time_point ExpiresOn;
    };

    struct UploadBlockBlobOptions
    {
        Models::BlobHttpHeaders HttpHeaders;
        Models::MetadataMap Metadata;
        // Empty omits the x-ms-access-tier header (the service default applies).
        Models::AccessTier AccessTier;
        Models::BlobRequestConditions Conditions;
        // Base64-encoded MD5 / CRC64 of the request body, verified by the service (Put Blob, Put Block).
        std::string TransactionalContentMd5;
        std::string TransactionalContentCrc64;
    };

    // Put Block honours only the lease condition and the transactional checksums; metadata, HTTP headers,
    // access tier and access conditions belong on Put Block List (CommitBlockListOptions).
    struct StageBlockOptions
    {
        // Only Conditions.LeaseId is sent.
        Models::BlobRequestConditions Conditions;
        // Base64-encoded MD5 / CRC64 of the block, verified by the service.
        std::string TransactionalContentMd5;
        std::string TransactionalContentCrc64;
    };

    struct CommitBlockListOptions
    {
        Models::BlobHttpHeaders HttpHeaders;
        Models::MetadataMap Metadata;
        // Empty omits the x-ms-access-tier header (the service default applies).
        Models::AccessTier AccessTier;
        Models::BlobRequestConditions Conditions;
    };

    struct CreatePageBlobOptions
    {
        Models::BlobHttpHeaders HttpHeaders;
        Models::MetadataMap Metadata;
        // Empty omits the x-ms-access-tier header (the service default applies).
        Models::AccessTier AccessTier;
        Models::BlobRequestConditions Conditions;
    };

    struct UploadPagesOptions
    {
        std::string ContentMd5;
        Models::BlobRequestConditions Conditions;
    };

    struct ClearPagesOptions
    {
        Models::BlobRequestConditions Conditions;
    };

    struct DeleteBlobOptions
    {
        Models::BlobRequestConditions Conditions;
    };

    struct GetBlobPropertiesOptions
    {
        Models::BlobRequestConditions Conditions;
    };

    struct DownloadBlobOptions
    {
        std::optional<Models::BlobByteRange> Range;
        Models::BlobRequestConditions Conditions;
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

    struct DownloadToOptions
    {
        std::optional<Models::BlobByteRange> Range;
        Models::BlobRequestConditions Conditions;
        std::size_t ChunkSize = DefaultChunkSize; // bytes; 0 selects the default (4 MiB); no upper bound
        // Count of concurrent chunk requests; 0 is treated as 1; no upper bound.
        std::size_t Concurrency = 1;
    };

    struct SetBlobMetadataOptions
    {
        Models::MetadataMap Metadata;
        Models::BlobRequestConditions Conditions;
    };

    struct AcquireLeaseOptions
    {
        Models::BlobRequestConditions Conditions;
        std::string ProposedLeaseId;
        // std::nullopt acquires an infinite lease; otherwise 15 to 60 seconds (validated client-side).
        std::optional<std::chrono::seconds> Duration;
    };

    // Conditions.LeaseId is ignored by every lease operation.
    struct RenewLeaseOptions
    {
        std::string LeaseId;
        Models::BlobRequestConditions Conditions;
    };

    struct ReleaseLeaseOptions
    {
        std::string LeaseId;
        Models::BlobRequestConditions Conditions;
    };

    // Set Blob Properties with x-ms-blob-content-length: only the access conditions apply.
    struct ResizePageBlobOptions
    {
        Models::BlobRequestConditions Conditions;
    };

    struct GetPageRangesOptions
    {
        std::optional<Models::BlobByteRange> Range;
        Models::BlobRequestConditions Conditions;
        // The service paginates large page lists. GetPageRangesAsync follows NextMarker until the list is
        // complete; Marker and MaxResults only select where to start and the size of each page requested.
        std::string Marker;
        std::optional<std::uint32_t> MaxResults;
    };

} // namespace AVEVA::AzureClient
