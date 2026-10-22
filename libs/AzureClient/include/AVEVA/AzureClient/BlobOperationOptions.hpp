// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>

#include <boost/asio/thread_pool.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

namespace AVEVA::AzureClient {
struct CreateBlobContainerOptions {
    Models::MetadataMap Metadata;
    Models::BlobRequestConditions Conditions;
};

struct ListBlobsOptions {
    std::string Prefix;
    std::string Marker;
    // Count of items per page (not a total cap); the service allows 1..5000 and rejects larger values
    // with a service error; unset uses the service default (5000).
    std::optional<std::uint32_t> MaxResults;
    // Adds the blobs' metadata to the listing (the `include=metadata` query parameter).
    bool IncludeMetadata = false;
};

struct CreatePageBlobOptions {
    Models::MetadataMap Metadata;
    Models::BlobRequestConditions Conditions;
};

struct UploadPagesOptions {
    Models::BlobRequestConditions Conditions;
};

// Put Page From URL: the service copies a page range from another blob without the bytes passing through the client.
struct UploadPagesFromUrlOptions {
    // Sent as x-ms-source-if-match: fails the copy with 412 when the source ETag has changed.
    std::string SourceIfMatch;
    // Authorizes the source with the same bearer token the client uses for the destination (renewed on
    // every retry). Needs a bearer-authorized client, with the source in a storage account that identity can
    // read. A SAS or Shared Key client has no bearer token to forward:
    // address the source with a SAS URL instead.
    bool AuthorizeSourceWithClientCredential = false;
    // Conditions on the destination blob.
    Models::BlobRequestConditions Conditions;
};

struct GetPageRangesOptions {
    // NextMarker from a previous truncated result.
    std::string Marker;
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

// Runs a blocking task (a disk write, file open or rename) off the I/O thread. The scheduler must eventually run
// every task posted to it, on any thread.
using DownloadWriteScheduler = boost::asio::thread_pool::executor_type;

struct DownloadBlobOptions {
    std::optional<Models::BlobByteRange> Range;
    Models::BlobRequestConditions Conditions;
    // Largest download accepted; a bigger blob (or range) fails with std::errc::file_too_large. The check runs
    // once the first ranged response has arrived and revealed the size, so that response (at most one chunk)
    // is already buffered by the transport; no later chunk is requested. Unset applies no limit.
    std::optional<std::uint64_t> MaxSize;
    // Where file downloads run their blocking filesystem calls. Unset uses the HTTP client's
    // `GetDownloadWriter()` when it has one (::AVEVA::HttpClient provides a bounded, client-owned pool), otherwise
    // a short-lived thread per task. Supply a scheduler to use your own pool.
    std::optional<DownloadWriteScheduler> WriteScheduler;
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
    // Largest output accepted; a bigger blob (or range) fails with std::errc::file_too_large before the
    // destination is written. The first ranged response (at most ChunkSize bytes) is buffered before the
    // size is known, so this does not bound transport buffering. Unset applies no limit.
    std::optional<std::uint64_t> MaxSize;
    // Where file downloads run their blocking filesystem calls. Unset uses the HTTP client's
    // `GetDownloadWriter()` when it has one (::AVEVA::HttpClient provides a bounded, client-owned pool), otherwise
    // a short-lived thread per task. Supply a scheduler to use your own pool.
    std::optional<DownloadWriteScheduler> WriteScheduler;
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
