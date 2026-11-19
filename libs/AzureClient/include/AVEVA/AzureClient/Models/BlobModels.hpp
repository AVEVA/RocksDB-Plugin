// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/Outcomes.hpp>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Models {
/// <summary>
/// Orders metadata names case-insensitively (ASCII), since the service treats metadata names that way.
/// </summary>
struct MetadataKeyCaseInsensitiveLess {
    using is_transparent = void;

    /// <summary>
    /// Folds an ASCII upper-case letter to lower case, leaving every other byte unchanged.
    /// </summary>
    /// <param name="value">The character to fold.</param>
    /// <returns>The lower-case form of <paramref name="value"/>.</returns>
    [[nodiscard]] static constexpr unsigned char ToLowerAscii(unsigned char value) noexcept {
        return (value >= 'A' && value <= 'Z') ? static_cast<unsigned char>(value + ('a' - 'A')) : value;
    }

    /// <summary>
    /// Compares two metadata names, ignoring ASCII case.
    /// </summary>
    /// <param name="lhs">The first name.</param>
    /// <param name="rhs">The second name.</param>
    /// <returns><c>true</c> when <paramref name="lhs"/> sorts before <paramref name="rhs"/>.</returns>
    [[nodiscard]] bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
        return std::ranges::lexicographical_compare(lhs,

                                                    rhs,

                                                    [](unsigned char left, unsigned char right) noexcept {
                                                        return MetadataKeyCaseInsensitiveLess::ToLowerAscii(left) <
                                                               MetadataKeyCaseInsensitiveLess::ToLowerAscii(right);
                                                    });
    }
};

/// <summary>
/// Blob metadata: name/value pairs whose names compare case-insensitively.
/// </summary>
using MetadataMap = std::map<std::string, std::string, MetadataKeyCaseInsensitiveLess>;

/// <summary>
/// The kind of a blob.
/// </summary>
enum class BlobType : std::uint8_t {
    /// <summary>The service did not report a type, or it is not one the client knows.</summary>
    Unknown,
    /// <summary>A block blob.</summary>
    BlockBlob,
    /// <summary>A page blob.</summary>
    PageBlob,
    /// <summary>An append blob.</summary>
    AppendBlob
};

/// <summary>
/// Whether a blob is locked by a lease.
/// </summary>
enum class LeaseStatus : std::uint8_t {
    /// <summary>The service did not report a status, or it is not one the client knows.</summary>
    Unknown,
    /// <summary>The blob is not locked.</summary>
    Unlocked,
    /// <summary>The blob is locked by a lease.</summary>
    Locked
};

/// <summary>
/// The lifecycle state of a blob's lease.
/// </summary>
enum class LeaseState : std::uint8_t {
    /// <summary>The service did not report a state, or it is not one the client knows.</summary>
    Unknown,
    /// <summary>The lease is available to be acquired.</summary>
    Available,
    /// <summary>The lease is held.</summary>
    Leased,
    /// <summary>The lease has expired.</summary>
    Expired,
    /// <summary>The lease is breaking.</summary>
    Breaking,
    /// <summary>The lease is broken.</summary>
    Broken
};

/// <summary>
/// Whether a lease has an expiry.
/// </summary>
enum class LeaseDurationType : std::uint8_t {
    /// <summary>The service did not report a duration, or it is not one the client knows.</summary>
    Unknown,
    /// <summary>The lease never expires.</summary>
    Infinite,
    /// <summary>The lease expires after a fixed duration.</summary>
    Fixed
};

/// <summary>
/// Conditions a request must satisfy for the service to carry it out; empty or unset members add no condition.
/// </summary>
struct BlobRequestConditions {
    /// <summary>
    /// The lease ID the blob must be leased under.
    /// </summary>
    std::string LeaseId;

    /// <summary>
    /// The blob's ETag must match this value.
    /// </summary>
    std::string IfMatch;

    /// <summary>
    /// The blob's ETag must not match this value.
    /// </summary>
    std::string IfNoneMatch;

    /// <summary>
    /// The blob must have been modified since this time.
    /// </summary>
    std::optional<std::chrono::system_clock::time_point> IfModifiedSince;

    /// <summary>
    /// The blob must not have been modified since this time.
    /// </summary>
    std::optional<std::chrono::system_clock::time_point> IfUnmodifiedSince;
};

/// <summary>
/// A range of bytes within a blob.
/// </summary>
struct BlobByteRange {
    /// <summary>
    /// The offset of the first byte of the range.
    /// </summary>
    std::uint64_t Offset = 0;

    /// <summary>
    /// Number of bytes from Offset (not an end offset). Unset means to the end of the blob; an explicit 0 is
    /// rejected as an invalid range rather than meaning "everything".
    /// </summary>
    std::optional<std::uint64_t> Length;
};

/// <summary>
/// The result of deleting a blob.
/// </summary>
struct DeleteBlobResult {
    /// <summary>
    /// The request ID the service assigned to the delete.
    /// </summary>
    std::string RequestId;

    /// <summary>
    /// Whether the delete was permanent or a soft delete; unset when the service did not say.
    /// </summary>
    std::optional<Permanence> DeleteTypePermanent;
};

/// <summary>
/// The properties of a blob, as reported by the service.
/// </summary>
struct BlobProperties {
    /// <summary>
    /// The blob's ETag.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;

    /// <summary>
    /// The size of the blob in bytes.
    /// </summary>
    std::uint64_t ContentLength = 0;

    /// <summary>
    /// The blob's Content-Type.
    /// </summary>
    std::string ContentType;

    /// <summary>
    /// The blob's Content-MD5 hash.
    /// </summary>
    std::string ContentMd5;

    /// <summary>
    /// The blob's Cache-Control.
    /// </summary>
    std::string CacheControl;

    /// <summary>
    /// The blob's Content-Encoding.
    /// </summary>
    std::string ContentEncoding;

    /// <summary>
    /// The blob's Content-Language.
    /// </summary>
    std::string ContentLanguage;

    /// <summary>
    /// The blob's Content-Disposition.
    /// </summary>
    std::string ContentDisposition;

    /// <summary>
    /// The kind of blob.
    /// </summary>
    BlobType Type = BlobType::Unknown;

    /// <summary>
    /// The blob's metadata.
    /// </summary>
    MetadataMap Metadata;

    /// <summary>
    /// When the blob was created; unset when the service did not report it.
    /// </summary>
    std::optional<std::chrono::system_clock::time_point> CreatedOn;

    /// <summary>
    /// Whether the blob is locked by a lease.
    /// </summary>
    Models::LeaseStatus LeaseStatus = Models::LeaseStatus::Unknown;

    /// <summary>
    /// The state of the blob's lease.
    /// </summary>
    Models::LeaseState LeaseState = Models::LeaseState::Unknown;

    /// <summary>
    /// Whether the blob's lease is infinite or fixed.
    /// </summary>
    Models::LeaseDurationType LeaseDuration = Models::LeaseDurationType::Unknown;

    /// <summary>
    /// Whether the service stores the blob's content encrypted; unset when the service did not say.
    /// </summary>
    std::optional<ServerEncryption> ServerEncrypted;

    /// <summary>
    /// Informational only: the client neither requests nor scopes operations by version or snapshot; these are
    /// filled in when the service returns them (for example when listing with snapshots or versions included).
    /// </summary>
    std::string VersionId;

    /// <summary>
    /// Whether this is the blob's current version; see <c>VersionId</c>.
    /// </summary>
    std::optional<VersionCurrency> IsCurrentVersion;

    /// <summary>
    /// The sequence number of a page blob; unset for other blobs.
    /// </summary>
    std::optional<std::uint64_t> SequenceNumber;
};

/// <summary>
/// The result of downloading a blob into memory.
/// </summary>
struct DownloadBlobResult {
    /// <summary>
    /// Raw response bytes; may be arbitrary binary data, not text.
    /// </summary>
    std::string Content;

    /// <summary>
    /// The blob's properties as reported with the content.
    /// </summary>
    BlobProperties Properties;

    /// <summary>
    /// The range the content covers, when the service answered with a Content-Range.
    /// </summary>
    std::optional<BlobByteRange> ContentRange;
};

/// <summary>
/// The result of downloading a blob to a file.
/// </summary>
struct DownloadBlobToResult {
    /// <summary>
    /// The blob's properties as reported with the content.
    /// </summary>
    BlobProperties Properties;

    /// <summary>
    /// The number of bytes written to the destination.
    /// </summary>
    std::uint64_t BytesWritten = 0;

    /// <summary>
    /// The range the content covers, when the service answered with a Content-Range.
    /// </summary>
    std::optional<BlobByteRange> ContentRange;
};

/// <summary>
/// One blob in a listing.
/// </summary>
struct BlobItem {
    /// <summary>
    /// The blob's name.
    /// </summary>
    std::string Name;

    /// <summary>
    /// The blob's properties as listed.
    /// </summary>
    BlobProperties Properties;
};

/// <summary>
/// One page of a blob listing.
/// </summary>
struct ListBlobsResult {
    /// <summary>
    /// The blobs on this page.
    /// </summary>
    std::vector<BlobItem> Blobs;

    /// <summary>
    /// The prefix the listing was restricted to.
    /// </summary>
    std::string Prefix;

    /// <summary>
    /// The marker this page started from.
    /// </summary>
    std::string Marker;

    /// <summary>
    /// Where to continue the listing; empty when there are no more pages.
    /// </summary>
    std::string NextMarker;
};

/// <summary>
/// The result of replacing a blob's metadata.
/// </summary>
struct SetBlobMetadataResult {
    /// <summary>
    /// The blob's ETag after the change.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// The result of acquiring a blob lease.
/// </summary>
struct AcquireBlobLeaseResult {
    /// <summary>
    /// The ID of the acquired lease.
    /// </summary>
    std::string LeaseId;

    /// <summary>
    /// The blob's ETag.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// The result of renewing a blob lease.
/// </summary>
struct RenewBlobLeaseResult {
    /// <summary>
    /// The ID of the renewed lease.
    /// </summary>
    std::string LeaseId;

    /// <summary>
    /// The blob's ETag.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// The result of releasing a blob lease.
/// </summary>
struct ReleaseBlobLeaseResult {
    /// <summary>
    /// The blob's ETag.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// The result of creating a page blob.
/// </summary>
struct CreatePageBlobResult {
    /// <summary>
    /// The blob's ETag.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// The result of writing pages to a page blob.
/// </summary>
struct UploadPagesResult {
    /// <summary>
    /// The blob's ETag after the write.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// The result of resizing a page blob.
/// </summary>
struct ResizePageBlobResult {
    /// <summary>
    /// The blob's ETag after the resize.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// A run of pages that hold data (not cleared).
/// </summary>
struct PageRange {
    /// <summary>
    /// The offset of the first byte of the run.
    /// </summary>
    std::uint64_t Offset = 0;

    /// <summary>
    /// The length of the run in bytes.
    /// </summary>
    std::uint64_t Length = 0;
};

/// <summary>
/// The result of listing a page blob's valid page ranges.
/// </summary>
struct GetPageRangesResult {
    /// <summary>
    /// Ranges that hold data, in ascending order; cleared ranges are omitted.
    /// </summary>
    std::vector<PageRange> Ranges;

    /// <summary>
    /// The blob's ETag.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the blob was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;

    /// <summary>
    /// Size of the whole blob (x-ms-blob-content-length), not of the queried range.
    /// </summary>
    std::uint64_t BlobContentLength = 0;

    /// <summary>
    /// Non-empty when the service truncated the list; pass it as <c>GetPageRangesOptions::Marker</c>.
    /// </summary>
    std::string NextMarker;
};

} // namespace AVEVA::AzureClient::Models
