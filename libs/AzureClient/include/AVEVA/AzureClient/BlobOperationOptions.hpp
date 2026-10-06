#pragma once
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace AVEVA::AzureClient
{
    inline constexpr std::size_t DefaultUploadBlockSize = std::size_t{4} * 1024U * 1024U;

    struct CreateBlobContainerOptions
    {
        Models::MetadataMap Metadata;
        Models::BlobRequestConditions Conditions;
    };

    struct DeleteBlobContainerOptions
    {
        Models::BlobRequestConditions Conditions;
    };

    struct GetBlobContainerPropertiesOptions
    {
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
        // Used only by ListBlobsAllAsync, which buffers the whole listing in memory: fails with
        // std::errc::value_too_large (and stops enumerating) once more than this many blobs plus prefixes have
        // been collected. Unset (the default) is unlimited.
        std::optional<std::size_t> MaxItems;
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

    struct ListBlobContainersOptions
    {
        std::string Prefix;
        std::string Marker;
        // Count of containers per page; the service allows 1..5000; unset uses the service default (5000).
        std::optional<std::uint32_t> MaxResults;
        // Used only by ListBlobContainersAllAsync, which buffers every container in memory: fails with
        // std::errc::value_too_large once more than this many have been collected. Unset is unlimited.
        std::optional<std::size_t> MaxItems;
        bool IncludeMetadata = false;
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

    // Put Block From URL. SourceOffset/SourceLength select a range of the source (whole source when unset);
    // only Conditions.LeaseId applies to the destination.
    struct StageBlockFromUriOptions
    {
        // Bytes; offset into the source blob (0-based). Unset means the start.
        std::optional<std::uint64_t> SourceOffset;
        // Bytes; length of the source range. Unset means to the end of the source.
        std::optional<std::uint64_t> SourceLength;
        // Base64-encoded MD5 the service verifies against the source range.
        std::string SourceContentMd5;
        Models::BlobRequestConditions Conditions;
    };

    struct FindBlobsByTagsOptions
    {
        std::string Marker;
        // Count of blobs per page; the service allows 1..5000; unset uses the service default (5000).
        std::optional<std::uint32_t> MaxResults;
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
        Models::DeleteSnapshotsOption DeleteSnapshotsOption;
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

    // Lease options are shared by blob and container leases. Containers only honour the
    // IfModifiedSince/IfUnmodifiedSince conditions, and Conditions.LeaseId is ignored by every lease operation.
    struct RenewLeaseOptions
    {
        std::string LeaseId;
        Models::BlobRequestConditions Conditions;
    };

    struct ChangeLeaseOptions
    {
        std::string LeaseId;
        std::string ProposedLeaseId;
        Models::BlobRequestConditions Conditions;
    };

    struct ReleaseLeaseOptions
    {
        std::string LeaseId;
        Models::BlobRequestConditions Conditions;
    };

    struct BreakLeaseOptions
    {
        Models::BlobRequestConditions Conditions;
        // How long the lease stays in the Breaking state (0 to 60 seconds, validated client-side);
        // std::nullopt uses the remaining lease period (fixed leases) or breaks immediately (infinite).
        std::optional<std::chrono::seconds> BreakPeriod;
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

    // UploadFromAsync stages the source as blocks of BlockSize bytes with up to Concurrency Put Block
    // requests in flight (peak buffer memory is Concurrency * BlockSize), then commits them in order
    // with one Put Block List. A source that fits in a single block is sent as one Put Blob instead.
    // UploadOptions' access conditions apply to the final commit / Put Blob; stage requests carry
    // only the lease ID.
    struct UploadFromOptions
    {
        // Count of concurrent Put Block requests; 0 is treated as 1; no upper bound.
        std::size_t Concurrency = 1;
        // Bytes per block; must be 1 byte to 4000 MiB (anything else fails with std::errc::invalid_argument before
        // any request). The service allows at most 50,000 blocks per blob.
        std::size_t BlockSize = DefaultUploadBlockSize;
        // Bytes. File uploads only (the size must be known up front): files of at most this many bytes are
        // uploaded with a single Put Blob request, buffering the whole file (capped at 5000 MiB).
        // 0 (the default) means only sources that fit in one block use a single Put Blob.
        // For file uploads that would need more than 50,000 blocks, BlockSize is increased
        // automatically (up to 4000 MiB); stream uploads fail with invalid_argument instead.
        std::uint64_t SingleUploadThreshold = 0;
        UploadBlockBlobOptions UploadOptions;
    };
} // namespace AVEVA::AzureClient
