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

namespace AVEVA::AzureClient::Models
{
    struct MetadataKeyCaseInsensitiveLess
    {
        using is_transparent = void;

        [[nodiscard]] static constexpr unsigned char ToLowerAscii(unsigned char value) noexcept
        {
            return (value >= 'A' && value <= 'Z') ? static_cast<unsigned char>(value + ('a' - 'A')) : value;
        }

        [[nodiscard]] bool operator()(std::string_view lhs, std::string_view rhs) const noexcept
        {
            return std::ranges::lexicographical_compare(lhs,

                rhs,

                [](unsigned char left, unsigned char right) noexcept
            {
                return MetadataKeyCaseInsensitiveLess::ToLowerAscii(left) <
                       MetadataKeyCaseInsensitiveLess::ToLowerAscii(right);
            });
        }
    };

    using MetadataMap = std::map<std::string, std::string, MetadataKeyCaseInsensitiveLess>;
    // Blob index tags (case-sensitive keys).
    using BlobTags = std::map<std::string, std::string>;

    enum class BlobType : std::uint8_t
    {
        Unknown,
        BlockBlob,
        PageBlob,
        AppendBlob
    };

    enum class LeaseStatus : std::uint8_t
    {
        Unknown,
        Unlocked,
        Locked
    };

    enum class LeaseState : std::uint8_t
    {
        Unknown,
        Available,
        Leased,
        Expired,
        Breaking,
        Broken
    };

    enum class LeaseDurationType : std::uint8_t
    {
        Unknown,
        Infinite,
        Fixed
    };

    // Base for string-valued service enumerations ("extensible enums"): the documented values are
    // named accessors on the derived type, but any string the service returns round-trips unchanged,
    // so new service values never get lost. An empty value means "not set". Derived types stay
    // implicitly constructible from a string so `options.AccessTier = "Hot"` keeps working.
    template <class Derived> class ExtensibleEnum
    {
      public:
        [[nodiscard]] const std::string& ToString() const noexcept
        {
            return m_value;
        }

        [[nodiscard]] bool empty() const noexcept
        {
            return m_value.empty();
        }

        friend bool operator==(const ExtensibleEnum&, const ExtensibleEnum&) = default;

        // Lets callers compare against a literal without materialising a temporary, and keeps
        // `tier == "Hot"` working now that the base constructors are protected.
        friend bool operator==(const ExtensibleEnum& lhs, std::string_view rhs) noexcept
        {
            return lhs.m_value == rhs;
        }

        friend bool operator==(std::string_view lhs, const ExtensibleEnum& rhs) noexcept
        {
            return rhs.m_value == lhs;
        }

      private:
        // Private (with `Derived` as the only friend) so an unrelated type cannot inherit from
        // someone else's instantiation of this CRTP base.
        ExtensibleEnum() = default;

        explicit ExtensibleEnum(std::string value) : m_value(std::move(value))
        {
        }

        friend Derived;

        std::string m_value;
    };

    class AccessTier : public ExtensibleEnum<AccessTier>
    {
      public:
        AccessTier() = default;

        template <class T>
            requires std::convertible_to<const T&, std::string_view>
        AccessTier(const T& value) : ExtensibleEnum(std::string{std::string_view{value}})
        {
        }

        // The documented tiers. These are accessors rather than static data members so that no
        // std::string is constructed before main(), where a failure could not be caught.
        [[nodiscard]] static const AccessTier& Hot();
        [[nodiscard]] static const AccessTier& Cool();
        [[nodiscard]] static const AccessTier& Cold();
        [[nodiscard]] static const AccessTier& Archive();
        [[nodiscard]] static const AccessTier& Premium();
        [[nodiscard]] static const AccessTier& P4();
        [[nodiscard]] static const AccessTier& P6();
        [[nodiscard]] static const AccessTier& P10();
        [[nodiscard]] static const AccessTier& P15();
        [[nodiscard]] static const AccessTier& P20();
        [[nodiscard]] static const AccessTier& P30();
        [[nodiscard]] static const AccessTier& P40();
        [[nodiscard]] static const AccessTier& P50();
        [[nodiscard]] static const AccessTier& P60();
        [[nodiscard]] static const AccessTier& P70();
        [[nodiscard]] static const AccessTier& P80();
    };

    inline const AccessTier& AccessTier::Hot()
    {
        static const AccessTier Value{"Hot"};
        return Value;
    }

    inline const AccessTier& AccessTier::Cool()
    {
        static const AccessTier Value{"Cool"};
        return Value;
    }

    inline const AccessTier& AccessTier::Cold()
    {
        static const AccessTier Value{"Cold"};
        return Value;
    }

    inline const AccessTier& AccessTier::Archive()
    {
        static const AccessTier Value{"Archive"};
        return Value;
    }

    inline const AccessTier& AccessTier::Premium()
    {
        static const AccessTier Value{"Premium"};
        return Value;
    }

    inline const AccessTier& AccessTier::P4()
    {
        static const AccessTier Value{"P4"};
        return Value;
    }

    inline const AccessTier& AccessTier::P6()
    {
        static const AccessTier Value{"P6"};
        return Value;
    }

    inline const AccessTier& AccessTier::P10()
    {
        static const AccessTier Value{"P10"};
        return Value;
    }

    inline const AccessTier& AccessTier::P15()
    {
        static const AccessTier Value{"P15"};
        return Value;
    }

    inline const AccessTier& AccessTier::P20()
    {
        static const AccessTier Value{"P20"};
        return Value;
    }

    inline const AccessTier& AccessTier::P30()
    {
        static const AccessTier Value{"P30"};
        return Value;
    }

    inline const AccessTier& AccessTier::P40()
    {
        static const AccessTier Value{"P40"};
        return Value;
    }

    inline const AccessTier& AccessTier::P50()
    {
        static const AccessTier Value{"P50"};
        return Value;
    }

    inline const AccessTier& AccessTier::P60()
    {
        static const AccessTier Value{"P60"};
        return Value;
    }

    inline const AccessTier& AccessTier::P70()
    {
        static const AccessTier Value{"P70"};
        return Value;
    }

    inline const AccessTier& AccessTier::P80()
    {
        static const AccessTier Value{"P80"};
        return Value;
    }

    class CopyStatus : public ExtensibleEnum<CopyStatus>
    {
      public:
        CopyStatus() = default;

        template <class T>
            requires std::convertible_to<const T&, std::string_view>
        CopyStatus(const T& value) : ExtensibleEnum(std::string{std::string_view{value}})
        {
        }

        [[nodiscard]] static const CopyStatus& Pending();
        [[nodiscard]] static const CopyStatus& Success();
        [[nodiscard]] static const CopyStatus& Aborted();
        [[nodiscard]] static const CopyStatus& Failed();
    };

    inline const CopyStatus& CopyStatus::Pending()
    {
        static const CopyStatus Value{"pending"};
        return Value;
    }

    inline const CopyStatus& CopyStatus::Success()
    {
        static const CopyStatus Value{"success"};
        return Value;
    }

    inline const CopyStatus& CopyStatus::Aborted()
    {
        static const CopyStatus Value{"aborted"};
        return Value;
    }

    inline const CopyStatus& CopyStatus::Failed()
    {
        static const CopyStatus Value{"failed"};
        return Value;
    }

    struct BlobRequestConditions
    {
        std::string LeaseId;
        std::string IfMatch;
        std::string IfNoneMatch;
        std::optional<std::chrono::system_clock::time_point> IfModifiedSince;
        std::optional<std::chrono::system_clock::time_point> IfUnmodifiedSince;
    };

    struct BlobHttpHeaders
    {
        std::string ContentType;
        std::string ContentMd5;
        std::string CacheControl;
        std::string ContentEncoding;
        std::string ContentLanguage;
        std::string ContentDisposition;
    };

    struct BlobByteRange
    {
        std::uint64_t Offset = 0;
        // Number of bytes from Offset (not an end offset). Unset means to the end of the blob; an explicit 0 is
        // rejected as an invalid range rather than meaning "everything".
        std::optional<std::uint64_t> Length;
    };

    struct DeleteBlobResult
    {
        std::string RequestId;
        std::optional<bool> DeleteTypePermanent;
    };

    struct BlobProperties
    {
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
        Models::AccessTier AccessTier;
        std::optional<bool> AccessTierInferred;
        Models::LeaseStatus LeaseStatus = Models::LeaseStatus::Unknown;
        Models::LeaseState LeaseState = Models::LeaseState::Unknown;
        Models::LeaseDurationType LeaseDuration = Models::LeaseDurationType::Unknown;
        std::string CopyId;
        Models::CopyStatus CopyStatus;
        std::string CopySource;
        std::string CopyProgress; // "<bytes copied>/<total bytes>"
        std::optional<bool> ServerEncrypted;
        std::string VersionId;
        std::optional<bool> IsCurrentVersion;
        std::optional<std::uint64_t> CommittedBlockCount; // append blobs
        std::optional<std::uint64_t> SequenceNumber;      // page blobs
    };

    struct DownloadBlobResult
    {
        // Raw response bytes; may be arbitrary binary data, not text.
        std::string Content;
        BlobProperties Properties;
        std::optional<BlobByteRange> ContentRange;
    };

    struct DownloadBlobToResult
    {
        BlobProperties Properties;
        std::uint64_t BytesWritten = 0;
        std::optional<BlobByteRange> ContentRange;
    };

    struct BlobItem
    {
        std::string Name;
        std::string Snapshot;
        BlobProperties Properties;
        // Set when listed with ListBlobsOptions::IncludeDeleted and the blob is soft-deleted.
        bool Deleted = false;
        // Populated when listed with ListBlobsOptions::IncludeTags.
        BlobTags Tags;
    };

    struct ListBlobsResult
    {
        std::vector<BlobItem> Blobs;
        std::vector<std::string> BlobPrefixes;
        std::string Prefix;
        std::string Delimiter;
        std::string Marker;
        std::string NextMarker;
    };

    struct UploadBlockBlobResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct StageBlockResult
    {
        std::string ContentMd5;
        std::string ContentCrc64;
        std::string RequestId;
    };

    struct CommitBlockListResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct BlockListBlock
    {
        std::string Name;
        std::uint64_t Size = 0;
    };

    struct SetBlobMetadataResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct AcquireBlobLeaseResult
    {
        std::string LeaseId;
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    // Lease results are shared by blob and container leases.
    struct RenewBlobLeaseResult
    {
        std::string LeaseId;
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct ReleaseBlobLeaseResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct CreatePageBlobResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct UploadPagesResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct ClearPagesResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct ResizePageBlobResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };

    struct PageRange
    {
        std::uint64_t Start = 0;
        std::uint64_t End = 0;
    };

    struct GetPageRangesResult
    {
        std::vector<PageRange> PageRanges;
        // Always empty on a completed GetPageRangesAsync result; every page has been merged.
        std::string NextMarker;
    };

    std::string EncodeBlockId(std::uint64_t index);
} // namespace AVEVA::AzureClient::Models
