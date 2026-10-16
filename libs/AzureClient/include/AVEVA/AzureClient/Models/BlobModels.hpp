// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

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
struct MetadataKeyCaseInsensitiveLess {
    using is_transparent = void;

    [[nodiscard]] static constexpr unsigned char ToLowerAscii(unsigned char value) noexcept {
        return (value >= 'A' && value <= 'Z') ? static_cast<unsigned char>(value + ('a' - 'A')) : value;
    }

    [[nodiscard]] bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
        return std::ranges::lexicographical_compare(lhs,

                                                    rhs,

                                                    [](unsigned char left, unsigned char right) noexcept {
                                                        return MetadataKeyCaseInsensitiveLess::ToLowerAscii(left) <
                                                               MetadataKeyCaseInsensitiveLess::ToLowerAscii(right);
                                                    });
    }
};

using MetadataMap = std::map<std::string, std::string, MetadataKeyCaseInsensitiveLess>;

enum class BlobType : std::uint8_t { Unknown, BlockBlob, PageBlob, AppendBlob };

enum class LeaseStatus : std::uint8_t { Unknown, Unlocked, Locked };

enum class LeaseState : std::uint8_t { Unknown, Available, Leased, Expired, Breaking, Broken };

enum class LeaseDurationType : std::uint8_t { Unknown, Infinite, Fixed };

struct BlobRequestConditions {
    std::string LeaseId;
    std::string IfMatch;
    std::string IfNoneMatch;
    std::optional<std::chrono::system_clock::time_point> IfModifiedSince;
    std::optional<std::chrono::system_clock::time_point> IfUnmodifiedSince;
};

struct BlobByteRange {
    std::uint64_t Offset = 0;
    // Number of bytes from Offset (not an end offset). Unset means to the end of the blob; an explicit 0 is
    // rejected as an invalid range rather than meaning "everything".
    std::optional<std::uint64_t> Length;
};

struct DeleteBlobResult {
    std::string RequestId;
    std::optional<bool> DeleteTypePermanent;
};

struct BlobProperties {
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
    std::uint64_t ContentLength = 0;
    std::string ContentType;
    std::string ContentMd5;
    std::string CacheControl;
    std::string ContentEncoding;
    std::string ContentLanguage;
    std::string ContentDisposition;
    BlobType Type = BlobType::Unknown;
    MetadataMap Metadata;
    std::optional<std::chrono::system_clock::time_point> CreatedOn;
    Models::LeaseStatus LeaseStatus = Models::LeaseStatus::Unknown;
    Models::LeaseState LeaseState = Models::LeaseState::Unknown;
    Models::LeaseDurationType LeaseDuration = Models::LeaseDurationType::Unknown;
    std::optional<bool> ServerEncrypted;
    // Informational only: the client neither requests nor scopes operations by version or snapshot; these are
    // filled in when the service returns them (for example when listing with snapshots or versions included).
    std::string VersionId;
    std::optional<bool> IsCurrentVersion;
    std::optional<std::uint64_t> SequenceNumber; // page blobs
};

struct DownloadBlobResult {
    // Raw response bytes; may be arbitrary binary data, not text.
    std::string Content;
    BlobProperties Properties;
    std::optional<BlobByteRange> ContentRange;
};

struct DownloadBlobToResult {
    BlobProperties Properties;
    std::uint64_t BytesWritten = 0;
    std::optional<BlobByteRange> ContentRange;
};

struct BlobItem {
    std::string Name;
    BlobProperties Properties;
};

struct ListBlobsResult {
    std::vector<BlobItem> Blobs;
    std::string Prefix;
    std::string Marker;
    std::string NextMarker;
};

struct SetBlobMetadataResult {
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

struct AcquireBlobLeaseResult {
    std::string LeaseId;
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

// Lease results for blob leases.
struct RenewBlobLeaseResult {
    std::string LeaseId;
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

struct ReleaseBlobLeaseResult {
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

struct CreatePageBlobResult {
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

struct UploadPagesResult {
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

struct ResizePageBlobResult {
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

// A run of pages that hold data (not cleared).
struct PageRange {
    std::uint64_t Offset = 0;
    std::uint64_t Length = 0;
};

struct GetPageRangesResult {
    // Ranges that hold data, in ascending order; cleared ranges are omitted.
    std::vector<PageRange> Ranges;
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
    // Size of the whole blob (x-ms-blob-content-length), not of the queried range.
    std::uint64_t BlobContentLength = 0;
    // Non-empty when the service truncated the list; pass it as GetPageRangesOptions::Marker.
    std::string NextMarker;
};

} // namespace AVEVA::AzureClient::Models
