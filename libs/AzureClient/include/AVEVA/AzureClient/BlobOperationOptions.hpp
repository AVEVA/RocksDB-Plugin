// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/Detail/ProtocolConstants.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Outcomes.hpp>
#include <AVEVA/AzureClient/Types.hpp>

#include <boost/asio/thread_pool.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace AVEVA::AzureClient {
/// <summary>
/// Options for creating a blob container.
/// </summary>
struct CreateBlobContainerOptions {
    /// <summary>
    /// Metadata to set on the new container. Create Container takes no access conditions, which is what lets
    /// CreateIfNotExistsAsync read a conflict as "the container exists".
    /// </summary>
    Models::MetadataMap Metadata;
};

/// <summary>
/// Options for listing the blobs of a container.
/// </summary>
struct ListBlobsOptions {
    /// <summary>
    /// Restricts the listing to blobs whose names start with this prefix.
    /// </summary>
    std::string Prefix;

    /// <summary>
    /// Where to continue a listing, taken from the previous page's result.
    /// </summary>
    std::string Marker;

    /// <summary>
    /// Count of items per page (not a total cap); the service allows 1..5000 and rejects larger values
    /// with a service error; unset uses the service default (5000).
    /// </summary>
    std::optional<std::uint32_t> MaxResults;

    /// <summary>
    /// Adds the blobs' metadata to the listing (the <c>include=metadata</c> query parameter).
    /// </summary>
    MetadataListing Metadata = MetadataListing::Exclude;
};

/// <summary>
/// Options for creating a page blob.
/// </summary>
struct CreatePageBlobOptions {
    /// <summary>
    /// Metadata to set on the new blob.
    /// </summary>
    Models::MetadataMap Metadata;

    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Options for writing pages to a page blob.
/// </summary>
struct UploadPagesOptions {
    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Put Page From URL: the service copies a page range from another blob without the bytes passing through the
/// client.
/// </summary>
struct UploadPagesFromUrlOptions {
    /// <summary>
    /// Sent as x-ms-source-if-match: fails the copy with 412 when the source ETag has changed.
    /// </summary>
    std::string SourceIfMatch;

    /// <summary>
    /// Authorizes the source with the same bearer token the client uses for the destination (renewed on
    /// every retry) when set to <c>ClientCredential</c>. Needs a bearer-authorized client, with the source in a
    /// storage account that identity can read. A SAS or Shared Key client has no bearer token to forward:
    /// address the source with a SAS URL instead.
    /// </summary>
    SourceAuthorization SourceAuth = SourceAuthorization::None;

    /// <summary>
    /// Conditions on the destination blob.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Options for listing the valid page ranges of a page blob.
/// </summary>
struct GetPageRangesOptions {
    /// <summary>
    /// NextMarker from a previous truncated result.
    /// </summary>
    std::string Marker;

    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Options for deleting a single blob.
/// </summary>
struct DeleteBlobOptions {
    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Options for deleting several blobs at once.
/// </summary>
struct DeleteBlobsOptions {
    /// <summary>
    /// Blob Batch requests (up to 256 deletes each) allowed in flight at once when more than 256 names
    /// are given.
    /// </summary>
    AzureClient::Concurrency MaxConcurrentRequests{4};
};

/// <summary>
/// Options for fetching a blob's properties.
/// </summary>
struct GetBlobPropertiesOptions {
    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Runs a blocking task (a disk write, file open or rename) off the I/O thread. The scheduler must eventually run
/// every task posted to it, on any thread.
/// </summary>
using DownloadWriteScheduler = boost::asio::thread_pool::executor_type;

/// <summary>
/// Options for downloading a blob.
/// </summary>
struct DownloadBlobOptions {
    /// <summary>
    /// The byte range to download; unset downloads the whole blob.
    /// </summary>
    std::optional<Models::BlobByteRange> Range;

    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;

    /// <summary>
    /// Largest download accepted; a bigger blob (or range) fails with <c>std::errc::file_too_large</c>. The check
    /// runs once the first ranged response has arrived and revealed the size, so that response (at most one chunk)
    /// is already buffered by the transport; no later chunk is requested. Unset applies no limit.
    /// </summary>
    std::optional<std::uint64_t> MaxSize;
};

/// <summary>
/// Default per-request chunk size used by the chunked download/upload helpers.
/// </summary>
inline constexpr std::size_t DefaultChunkSize = Private::BlobTransferLimits::DefaultDownloadChunkSize; // 4 MiB

/// <summary>
/// Options for downloading a blob to a file. Chunked download options. The first request is a ranged GET of
/// <c>ChunkSize</c> bytes that also learns the blob's size and ETag; the remaining bytes are fetched in
/// <c>ChunkSize</c> ranges with up to <c>Concurrency</c> requests in flight and written to the destination strictly in
/// order (no seeking), so peak buffered memory is about <c>Concurrency * ChunkSize</c>. Every chunk after the first
/// sends If-Match: &lt;first ETag&gt; (unless <c>Conditions.IfMatch</c> is set), so a concurrent overwrite fails with
/// <c>ConditionNotMet</c> instead of producing a torn result. Each request's response-body limit is raised
/// to at least <c>ChunkSize</c> plus a small margin.
/// </summary>
struct DownloadToOptions {
    /// <summary>
    /// The byte range to download; unset downloads the whole blob.
    /// </summary>
    std::optional<Models::BlobByteRange> Range;

    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;

    /// <summary>
    /// The size of each ranged request, in bytes; no upper bound.
    /// </summary>
    AzureClient::ChunkSize ChunkSize{DefaultChunkSize};

    /// <summary>
    /// Count of concurrent chunk requests; no upper bound.
    /// </summary>
    AzureClient::Concurrency Concurrency{1};

    /// <summary>
    /// Largest output accepted; a bigger blob (or range) fails with <c>std::errc::file_too_large</c> before the
    /// destination is written. The first ranged response (at most <c>ChunkSize</c> bytes) is buffered before the
    /// size is known, so this does not bound transport buffering. Unset applies no limit.
    /// </summary>
    std::optional<std::uint64_t> MaxSize;

    /// <summary>
    /// Where file downloads run their blocking filesystem calls. Unset uses the transport's
    /// <c>GetDownloadWriter()</c> when it has one (::AVEVA::HttpClient does not, so callers supply a scheduler);
    /// otherwise the download fails with <c>operation_not_supported</c>.
    /// </summary>
    std::optional<DownloadWriteScheduler> WriteScheduler;
};

/// <summary>
/// Options for replacing a blob's metadata.
/// </summary>
struct SetBlobMetadataOptions {
    /// <summary>
    /// The metadata to store on the blob.
    /// </summary>
    Models::MetadataMap Metadata;

    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Options for acquiring a lease on a blob.
/// </summary>
struct AcquireLeaseOptions {
    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;

    /// <summary>
    /// The lease ID the caller asks the service to assign to the new lease.
    /// </summary>
    std::string ProposedLeaseId;

    /// <summary>
    /// <c>std::nullopt</c> acquires an infinite lease.
    /// </summary>
    std::optional<LeaseDuration> Duration;
};

/// <summary>
/// Options for renewing a lease. <c>Conditions.LeaseId</c> is ignored by every lease operation.
/// </summary>
struct RenewLeaseOptions {
    /// <summary>
    /// The ID of the lease to renew.
    /// </summary>
    std::string LeaseId;

    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Options for releasing a lease. <c>Conditions.LeaseId</c> is ignored by every lease operation.
/// </summary>
struct ReleaseLeaseOptions {
    /// <summary>
    /// The ID of the lease to release.
    /// </summary>
    std::string LeaseId;

    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

/// <summary>
/// Set Blob Properties with x-ms-blob-content-length: only the access conditions apply.
/// </summary>
struct ResizePageBlobOptions {
    /// <summary>
    /// Access conditions the request must satisfy.
    /// </summary>
    Models::BlobRequestConditions Conditions;
};

} // namespace AVEVA::AzureClient
