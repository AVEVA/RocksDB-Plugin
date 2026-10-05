#include "BlobRequestHelpers.hpp"

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobServiceClient.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/ITokenCredential.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Models/BlobServiceModels.hpp"
#include "BlobStorageErrorCategory.hpp"
#include "BlobXmlParser.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/url/encode.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <boost/url/parse.hpp>
#include <boost/url/pct_string_view.hpp>
#include <boost/url/rfc/unreserved_chars.hpp>
#include <boost/url/url.hpp>
#include <boost/url/url_view.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <libxml/parser.h>
#include <libxml/tree.h>

#include <libxml/xmlstring.h>
#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstring>
#include <format>
#include <limits>
#include <locale>
#include <memory>
#include <mutex>
#include <openssl/params.h>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

// Response parsing: headers, properties, XML bodies and list/page/block payloads.
namespace AVEVA::AzureClient::Private
{
    namespace
    {

        [[nodiscard]] Models::BlobType ParseBlobTypeValue(std::string_view value)
        {
            if (IEquals(value, "BlockBlob"))
            {
                return Models::BlobType::BlockBlob;
            }
            if (IEquals(value, "PageBlob"))
            {
                return Models::BlobType::PageBlob;
            }
            if (IEquals(value, "AppendBlob"))
            {
                return Models::BlobType::AppendBlob;
            }
            return Models::BlobType::Unknown;
        }

        [[nodiscard]] std::optional<std::uint64_t> ParseUnsigned(std::string_view value) noexcept
        {
            if (value.empty())
            {
                return std::nullopt;
            }

            std::uint64_t parsed = 0;
            const auto result = std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
            if (result.ec != std::errc{} || result.ptr != std::to_address(value.end()))
            {
                return std::nullopt;
            }

            return parsed;
        }

        // Tags the error-message field name with its own type so it's never adjacent-and-same-type
        // with the raw value parameter it describes.
        struct FieldNameTag
        {
        };

        using FieldName = StringLabel<FieldNameTag>;

        [[nodiscard]] std::optional<bool> ParseOptionalBool(std::string_view value, FieldName what)
        {
            if (value.empty())
            {
                return std::nullopt;
            }
            if (IEquals(value, "true"))
            {
                return true;
            }
            if (IEquals(value, "false"))
            {
                return false;
            }
            throw std::invalid_argument(std::string{what.Value} + " must be true or false.");
        }

        [[nodiscard]] std::optional<std::uint64_t> ParseOptionalUnsigned(std::string_view value, FieldName what)
        {
            if (value.empty())
            {
                return std::nullopt;
            }
            const auto parsed = ParseUnsigned(value);
            if (!parsed.has_value())
            {
                throw std::invalid_argument(std::string{what.Value} + " must be an unsigned integer.");
            }
            return parsed;
        }

        [[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseOptionalHttpDate(std::string_view value,
            FieldName what)
        {
            if (value.empty())
            {
                return std::nullopt;
            }
            const auto parsed = ParseHttpDateHeader(value);
            if (!parsed.has_value())
            {
                throw std::invalid_argument(std::string{what.Value} + " must contain a valid HTTP-date.");
            }
            return parsed;
        }

        // Where each extended blob property lives in a Get Blob Properties response (headers) or a
        // List Blobs <Properties> element; the same parsing applies to both.
        struct BlobPropertyNames
        {
            std::string_view ContentEncoding;
            std::string_view ContentLanguage;
            std::string_view ContentDisposition;
            std::string_view CreatedOn;
            std::string_view AccessTier;
            std::string_view AccessTierInferred;
            std::string_view LeaseStatus;
            std::string_view LeaseState;
            std::string_view LeaseDuration;
            std::string_view CopyId;
            std::string_view CopyStatus;
            std::string_view CopySource;
            std::string_view CopyProgress;
            std::string_view ServerEncrypted;
            std::string_view CommittedBlockCount;
            std::string_view SequenceNumber;
        };

        constexpr BlobPropertyNames HeaderPropertyNames{.ContentEncoding = "Content-Encoding",
            .ContentLanguage = "Content-Language",
            .ContentDisposition = "Content-Disposition",
            .CreatedOn = "x-ms-creation-time",
            .AccessTier = "x-ms-access-tier",
            .AccessTierInferred = "x-ms-access-tier-inferred",
            .LeaseStatus = "x-ms-lease-status",
            .LeaseState = "x-ms-lease-state",
            .LeaseDuration = "x-ms-lease-duration",
            .CopyId = "x-ms-copy-id",
            .CopyStatus = "x-ms-copy-status",
            .CopySource = "x-ms-copy-source",
            .CopyProgress = "x-ms-copy-progress",
            .ServerEncrypted = "x-ms-server-encrypted",
            .CommittedBlockCount = "x-ms-blob-committed-block-count",
            .SequenceNumber = "x-ms-blob-sequence-number"};

        constexpr BlobPropertyNames XmlPropertyNames{.ContentEncoding = "Content-Encoding",
            .ContentLanguage = "Content-Language",
            .ContentDisposition = "Content-Disposition",
            .CreatedOn = "Creation-Time",
            .AccessTier = "AccessTier",
            .AccessTierInferred = "AccessTierInferred",
            .LeaseStatus = "LeaseStatus",
            .LeaseState = "LeaseState",
            .LeaseDuration = "LeaseDuration",
            .CopyId = "CopyId",
            .CopyStatus = "CopyStatus",
            .CopySource = "CopySource",
            .CopyProgress = "CopyProgress",
            .ServerEncrypted = "ServerEncrypted",
            .CommittedBlockCount = "CommittedBlockCount",
            .SequenceNumber = "x-ms-blob-sequence-number"};

        // `get(name)` returns the raw value (empty when absent). Throws std::invalid_argument on malformed values.
        template <class Getter>
        void ParseExtendedBlobProperties(Models::BlobProperties& properties,
            const BlobPropertyNames& names,
            const Getter& get)
        {
            properties.ContentEncoding = std::string{get(names.ContentEncoding)};
            properties.ContentLanguage = std::string{get(names.ContentLanguage)};
            properties.ContentDisposition = std::string{get(names.ContentDisposition)};
            properties.CreatedOn = ParseOptionalHttpDate(get(names.CreatedOn), names.CreatedOn);
            properties.AccessTier = std::string{get(names.AccessTier)};
            properties.AccessTierInferred = ParseOptionalBool(get(names.AccessTierInferred), names.AccessTierInferred);
            properties.LeaseStatus = ParseLeaseStatus(get(names.LeaseStatus));
            properties.LeaseState = ParseLeaseState(get(names.LeaseState));
            properties.LeaseDuration = ParseLeaseDurationType(get(names.LeaseDuration));
            properties.CopyId = std::string{get(names.CopyId)};
            properties.CopyStatus = std::string{get(names.CopyStatus)};
            properties.CopySource = std::string{get(names.CopySource)};
            properties.CopyProgress = std::string{get(names.CopyProgress)};
            properties.ServerEncrypted = ParseOptionalBool(get(names.ServerEncrypted), names.ServerEncrypted);
            properties.CommittedBlockCount =
                ParseOptionalUnsigned(get(names.CommittedBlockCount), names.CommittedBlockCount);
            properties.SequenceNumber = ParseOptionalUnsigned(get(names.SequenceNumber), names.SequenceNumber);
        }

        [[nodiscard]] std::optional<int> ParseInt(std::string_view value) noexcept
        {
            if (value.empty())
            {
                return std::nullopt;
            }

            int parsed = 0;
            const auto result = std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
            if (result.ec != std::errc{} || result.ptr != std::to_address(value.end()))
            {
                return std::nullopt;
            }
            return parsed;
        }

        [[nodiscard]] std::optional<unsigned int> ParseUnsignedComponent(std::string_view value) noexcept
        {
            if (value.empty())
            {
                return std::nullopt;
            }

            unsigned int parsed = 0;
            const auto result = std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
            if (result.ec != std::errc{} || result.ptr != std::to_address(value.end()))
            {
                return std::nullopt;
            }

            return parsed;
        }

        [[nodiscard]] std::optional<unsigned int> ParseMonth(std::string_view value) noexcept
        {
            for (std::size_t index = 0; index < MonthNames.size(); ++index)
            {
                if (IEquals(MonthNames.at(index), value))
                {
                    return static_cast<unsigned int>(index + 1U);
                }
            }

            return std::nullopt;
        }

        constexpr unsigned int MaxHourOfDay = 23U;
        constexpr unsigned int MaxMinuteOfHour = 59U;
        // 60 is accepted so that a leap second is not rejected outright.
        constexpr unsigned int MaxSecondOfMinute = 60U;

        // A fixed-width field within one of the fixed-layout timestamp formats below:
        // the `Offset` of its first character and its `Length` in characters.
        struct DateField
        {
            std::size_t Offset;
            std::size_t Length;
        };

        // Wraps every other time-of-day/calendar component so no two adjacent parameters of
        // BuildUtcTimePoint share a plain `unsigned int` type.
        struct UtcTimeComponentTag
        {
        };

        using UtcTimeComponent = NumericLabel<unsigned int, UtcTimeComponentTag>;

        [[nodiscard]] std::optional<std::chrono::system_clock::time_point> BuildUtcTimePoint(int yearValue,
            UtcTimeComponent monthValue,
            unsigned int dayValue,
            UtcTimeComponent hourValue,
            unsigned int minuteValue,
            UtcTimeComponent secondValue) noexcept
        {
            using namespace std::chrono;

            const year_month_day calendarDate{year{yearValue}, month{monthValue.Value}, day{dayValue}};
            if (!calendarDate.ok() || hourValue.Value > MaxHourOfDay || minuteValue > MaxMinuteOfHour ||
                secondValue.Value > MaxSecondOfMinute)
            {
                return std::nullopt;
            }

            const sys_days days{calendarDate};
            return system_clock::time_point{duration_cast<system_clock::duration>(
                days.time_since_epoch() + hours{hourValue.Value} + minutes{minuteValue} + seconds{secondValue.Value})};
        }

        // "Sun, 06 Nov 1994 08:49:37 GMT" - the preferred HTTP-date format (RFC 9110 5.6.7).
        namespace Rfc1123Layout
        {
            constexpr std::size_t TotalLength = 29U;
            constexpr DateField Weekday{.Offset = 0U, .Length = 3U};
            constexpr DateField WeekdaySeparator{.Offset = 3U, .Length = 2U}; // ", "
            constexpr DateField Day{.Offset = 5U, .Length = 2U};
            constexpr DateField DaySeparator{.Offset = 7U, .Length = 1U}; // " "
            constexpr DateField Month{.Offset = 8U, .Length = 3U};
            constexpr DateField MonthSeparator{.Offset = 11U, .Length = 1U}; // " "
            constexpr DateField Year{.Offset = 12U, .Length = 4U};
            constexpr DateField YearSeparator{.Offset = 16U, .Length = 1U}; // " "
            constexpr DateField Hour{.Offset = 17U, .Length = 2U};
            constexpr DateField HourSeparator{.Offset = 19U, .Length = 1U}; // ":"
            constexpr DateField Minute{.Offset = 20U, .Length = 2U};
            constexpr DateField MinuteSeparator{.Offset = 22U, .Length = 1U}; // ":"
            constexpr DateField Second{.Offset = 23U, .Length = 2U};
            constexpr DateField Zone{.Offset = 25U, .Length = 4U}; // " GMT"
        } // namespace Rfc1123Layout

        [[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseRfc1123HttpDate(
            std::string_view value) noexcept
        {
            using namespace Rfc1123Layout;

            if (value.size() != TotalLength || value.substr(WeekdaySeparator.Offset, WeekdaySeparator.Length) != ", " ||
                value.substr(DaySeparator.Offset, DaySeparator.Length) != " " ||
                value.substr(MonthSeparator.Offset, MonthSeparator.Length) != " " ||
                value.substr(YearSeparator.Offset, YearSeparator.Length) != " " ||
                value.substr(HourSeparator.Offset, HourSeparator.Length) != ":" ||
                value.substr(MinuteSeparator.Offset, MinuteSeparator.Length) != ":" ||
                !IEquals(value.substr(Zone.Offset, Zone.Length), " GMT"))
            {
                return std::nullopt;
            }

            const std::string_view weekday = value.substr(Weekday.Offset, Weekday.Length);
            if (std::ranges::find_if(ShortWeekdayNames,
                    [weekday](std::string_view expected)
            {
                return IEquals(expected, weekday);
            }) == ShortWeekdayNames.end())
            {
                return std::nullopt;
            }

            const auto day = ParseUnsignedComponent(value.substr(Day.Offset, Day.Length));
            const auto month = ParseMonth(value.substr(Month.Offset, Month.Length));
            const auto year = ParseInt(value.substr(Year.Offset, Year.Length));
            const auto hour = ParseUnsignedComponent(value.substr(Hour.Offset, Hour.Length));
            const auto minute = ParseUnsignedComponent(value.substr(Minute.Offset, Minute.Length));
            const auto second = ParseUnsignedComponent(value.substr(Second.Offset, Second.Length));
            if (!day.has_value() || !month.has_value() || !year.has_value() || !hour.has_value() ||
                !minute.has_value() || !second.has_value())
            {
                return std::nullopt;
            }

            return BuildUtcTimePoint(*year, *month, *day, *hour, *minute, *second);
        }

        // "Sunday, 06-Nov-94 08:49:37 GMT" - the obsolete RFC 850 format. Offsets are relative to the
        // remainder that follows "<weekday>, ".
        namespace Rfc850Layout
        {
            constexpr std::size_t RemainderLength = 22U;
            constexpr DateField Day{.Offset = 0U, .Length = 2U};
            constexpr DateField DaySeparator{.Offset = 2U, .Length = 1U}; // "-"
            constexpr DateField Month{.Offset = 3U, .Length = 3U};
            constexpr DateField MonthSeparator{.Offset = 6U, .Length = 1U}; // "-"
            constexpr DateField Year{.Offset = 7U, .Length = 2U};
            constexpr DateField YearSeparator{.Offset = 9U, .Length = 1U}; // " "
            constexpr DateField Hour{.Offset = 10U, .Length = 2U};
            constexpr DateField HourSeparator{.Offset = 12U, .Length = 1U}; // ":"
            constexpr DateField Minute{.Offset = 13U, .Length = 2U};
            constexpr DateField MinuteSeparator{.Offset = 15U, .Length = 1U}; // ":"
            constexpr DateField Second{.Offset = 16U, .Length = 2U};
            constexpr DateField Zone{.Offset = 18U, .Length = 4U}; // " GMT"

            // Two-digit years are read as 20xx, then pushed back a century if that would place them
            // more than 50 years in the future (RFC 9110 5.6.7).
            constexpr int CenturyBase = 2000;
            constexpr int MaxYearsAhead = 50;
            constexpr int YearsPerCentury = 100;
            constexpr std::size_t WeekdaySeparatorLength = 2U; // ", "
        } // namespace Rfc850Layout

        [[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseRfc850HttpDate(
            std::string_view value) noexcept
        {
            using namespace Rfc850Layout;

            const std::size_t comma = value.find(", ");
            if (comma == std::string_view::npos || comma == 0U)
            {
                return std::nullopt;
            }

            const std::string_view weekday = value.substr(0U, comma);
            if (std::ranges::find_if(LongWeekdayNames,
                    [weekday](std::string_view expected)
            {
                return IEquals(expected, weekday);
            }) == LongWeekdayNames.end())
            {
                return std::nullopt;
            }

            const std::string_view remainder = value.substr(comma + WeekdaySeparatorLength);
            if (remainder.size() != RemainderLength || remainder.at(DaySeparator.Offset) != '-' ||
                remainder.at(MonthSeparator.Offset) != '-' || remainder.at(YearSeparator.Offset) != ' ' ||
                remainder.at(HourSeparator.Offset) != ':' || remainder.at(MinuteSeparator.Offset) != ':' ||
                !IEquals(remainder.substr(Zone.Offset, Zone.Length), " GMT"))
            {
                return std::nullopt;
            }

            const auto day = ParseUnsignedComponent(remainder.substr(Day.Offset, Day.Length));
            const auto month = ParseMonth(remainder.substr(Month.Offset, Month.Length));
            const auto shortYear = ParseUnsignedComponent(remainder.substr(Year.Offset, Year.Length));
            const auto hour = ParseUnsignedComponent(remainder.substr(Hour.Offset, Hour.Length));
            const auto minute = ParseUnsignedComponent(remainder.substr(Minute.Offset, Minute.Length));
            const auto second = ParseUnsignedComponent(remainder.substr(Second.Offset, Second.Length));
            if (!day.has_value() || !month.has_value() || !shortYear.has_value() || !hour.has_value() ||
                !minute.has_value() || !second.has_value())
            {
                return std::nullopt;
            }

            const int currentYear = static_cast<int>(
                std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now())}
                    .year());
            int year = CenturyBase + static_cast<int>(*shortYear);
            if (year > (currentYear + MaxYearsAhead))
            {
                year -= YearsPerCentury;
            }

            return BuildUtcTimePoint(year, *month, *day, *hour, *minute, *second);
        }

        // "Sun Nov  6 08:49:37 1994" - the ANSI C asctime() format.
        namespace AsctimeLayout
        {
            constexpr std::size_t TotalLength = 24U;
            constexpr DateField Weekday{.Offset = 0U, .Length = 3U};
            constexpr DateField WeekdaySeparator{.Offset = 3U, .Length = 1U}; // " "
            constexpr DateField Month{.Offset = 4U, .Length = 3U};
            constexpr DateField MonthSeparator{.Offset = 7U, .Length = 1U}; // " "
            constexpr DateField Day{.Offset = 8U, .Length = 2U};            // space-padded
            constexpr DateField DaySeparator{.Offset = 10U, .Length = 1U};  // " "
            constexpr DateField Hour{.Offset = 11U, .Length = 2U};
            constexpr DateField HourSeparator{.Offset = 13U, .Length = 1U}; // ":"
            constexpr DateField Minute{.Offset = 14U, .Length = 2U};
            constexpr DateField MinuteSeparator{.Offset = 16U, .Length = 1U}; // ":"
            constexpr DateField Second{.Offset = 17U, .Length = 2U};
            constexpr DateField SecondSeparator{.Offset = 19U, .Length = 1U}; // " "
            constexpr DateField Year{.Offset = 20U, .Length = 4U};
        } // namespace AsctimeLayout

        [[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseAsctimeHttpDate(
            std::string_view value) noexcept
        {
            using namespace AsctimeLayout;

            if (value.size() != TotalLength || value.at(WeekdaySeparator.Offset) != ' ' ||
                value.at(MonthSeparator.Offset) != ' ' || value.at(DaySeparator.Offset) != ' ' ||
                value.at(HourSeparator.Offset) != ':' || value.at(MinuteSeparator.Offset) != ':' ||
                value.at(SecondSeparator.Offset) != ' ')
            {
                return std::nullopt;
            }

            const std::string_view weekday = value.substr(Weekday.Offset, Weekday.Length);
            if (std::ranges::find_if(ShortWeekdayNames,
                    [weekday](std::string_view expected)
            {
                return IEquals(expected, weekday);
            }) == ShortWeekdayNames.end())
            {
                return std::nullopt;
            }

            const auto month = ParseMonth(value.substr(Month.Offset, Month.Length));
            const auto day = ParseUnsignedComponent(TrimWhitespace(value.substr(Day.Offset, Day.Length)));
            const auto hour = ParseUnsignedComponent(value.substr(Hour.Offset, Hour.Length));
            const auto minute = ParseUnsignedComponent(value.substr(Minute.Offset, Minute.Length));
            const auto second = ParseUnsignedComponent(value.substr(Second.Offset, Second.Length));
            const auto year = ParseInt(value.substr(Year.Offset, Year.Length));
            if (!month.has_value() || !day.has_value() || !hour.has_value() || !minute.has_value() ||
                !second.has_value() || !year.has_value())
            {
                return std::nullopt;
            }

            return BuildUtcTimePoint(*year, *month, *day, *hour, *minute, *second);
        }

        [[nodiscard]] Models::MetadataMap ParseMetadataXml(const XmlNode& node)
        {
            Models::MetadataMap metadata;
            ForEachElement(node,
                [&](const XmlNode& child)
            {
                metadata.emplace(std::string{GetLocalName(NodeName(child))}, GetNodeText(child));
            });
            return metadata;
        }

        [[nodiscard]] Models::BlobProperties ParseBlobPropertiesXml(const XmlNode& node)
        {
            Models::BlobProperties properties;
            properties.ETag = GetChildTextOrEmpty(node, "Etag");
            if (const std::string lastModified = GetChildTextOrEmpty(node, "Last-Modified"); !lastModified.empty())
            {
                const auto parsed = ParseHttpDateHeader(lastModified);
                if (!parsed.has_value())
                {
                    throw std::invalid_argument("Blob Properties Last-Modified must contain a valid HTTP-date.");
                }

                properties.LastModified = *parsed;
            }

            if (const std::string contentLength = GetChildTextOrEmpty(node, "Content-Length"); !contentLength.empty())
            {
                const auto parsed = ParseUnsigned(contentLength);
                if (!parsed.has_value())
                {
                    throw std::invalid_argument("Blob Properties Content-Length must be an unsigned integer.");
                }

                properties.ContentLength = *parsed;
            }
            properties.ContentType = GetChildTextOrEmpty(node, "Content-Type");
            properties.ContentMd5 = GetChildTextOrEmpty(node, "Content-MD5");
            properties.CacheControl = GetChildTextOrEmpty(node, "Cache-Control");
            properties.Type = ParseBlobTypeValue(GetChildTextOrEmpty(node, "BlobType"));
            ParseExtendedBlobProperties(properties,
                XmlPropertyNames,
                [&node](std::string_view name)
            {
                return GetChildTextOrEmpty(node, name);
            });
            if (const XmlNode* metadata = FindChildByLocalName(node, "Metadata"); metadata != nullptr)
            {
                properties.Metadata = ParseMetadataXml(*metadata);
            }
            return properties;
        }
    } // namespace

    std::string_view FindHeaderValue(const HttpResponse& response, std::string_view headerName) noexcept
    {
        for (const auto& header : response.GetHeaders())
        {
            if (IEquals(header.GetName(), headerName))
            {
                return header.GetValue();
            }
        }
        return {};
    }

    Models::PublicAccessType ParsePublicAccessType(std::string_view value)
    {
        if (value.empty())
        {
            return Models::PublicAccessType::None;
        }
        if (IEquals(value, "container"))
        {
            return Models::PublicAccessType::BlobContainer;
        }

        if (IEquals(value, "blob"))
        {
            return Models::PublicAccessType::Blob;
        }

        return Models::PublicAccessType::Unknown;
    }

    Models::LeaseStatus ParseLeaseStatus(std::string_view value)
    {
        if (value.empty())
        {
            return Models::LeaseStatus::Unknown;
        }
        if (IEquals(value, "locked"))
        {
            return Models::LeaseStatus::Locked;
        }
        if (IEquals(value, "unlocked"))
        {
            return Models::LeaseStatus::Unlocked;
        }
        return Models::LeaseStatus::Unknown;
    }

    Models::LeaseState ParseLeaseState(std::string_view value)
    {
        if (value.empty())
        {
            return Models::LeaseState::Unknown;
        }
        if (IEquals(value, "leased"))
        {
            return Models::LeaseState::Leased;
        }
        if (IEquals(value, "expired"))
        {
            return Models::LeaseState::Expired;
        }
        if (IEquals(value, "breaking"))
        {
            return Models::LeaseState::Breaking;
        }
        if (IEquals(value, "broken"))
        {
            return Models::LeaseState::Broken;
        }
        if (IEquals(value, "available"))
        {
            return Models::LeaseState::Available;
        }
        return Models::LeaseState::Unknown;
    }

    Models::LeaseDurationType ParseLeaseDurationType(std::string_view value)
    {
        if (value.empty())
        {
            return Models::LeaseDurationType::Unknown;
        }
        if (IEquals(value, "fixed"))
        {
            return Models::LeaseDurationType::Fixed;
        }
        if (IEquals(value, "infinite"))
        {
            return Models::LeaseDurationType::Infinite;
        }
        return Models::LeaseDurationType::Unknown;
    }

    bool ParseBoolHeader(std::string_view value) noexcept
    {
        return IEquals(value, "true");
    }

    std::optional<std::chrono::system_clock::time_point> ParseHttpDateHeader(std::string_view value) noexcept
    {
        value = TrimWhitespace(value);
        if (value.empty())
        {
            return std::nullopt;
        }

        if (const auto rfc1123 = ParseRfc1123HttpDate(value); rfc1123.has_value())
        {
            return rfc1123;
        }
        if (const auto rfc850 = ParseRfc850HttpDate(value); rfc850.has_value())
        {
            return rfc850;
        }
        return ParseAsctimeHttpDate(value);
    }

    std::string FormatIso8601Utc(std::chrono::system_clock::time_point value)
    {
        const auto seconds = std::chrono::floor<std::chrono::seconds>(value);
        const auto days = std::chrono::floor<std::chrono::days>(seconds);
        const std::chrono::year_month_day calendarDate{days};
        const std::chrono::hh_mm_ss timeOfDay{seconds - days};
        return std::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            static_cast<int>(calendarDate.year()),
            static_cast<unsigned>(calendarDate.month()),
            static_cast<unsigned>(calendarDate.day()),
            timeOfDay.hours().count(),
            timeOfDay.minutes().count(),
            timeOfDay.seconds().count());
    }

    // "1994-11-06T08:49:37Z" with an optional fractional-second part before the 'Z'.
    namespace Iso8601Layout
    {
        constexpr std::size_t MinLength = 20U;
        constexpr DateField Year{.Offset = 0U, .Length = 4U};
        constexpr DateField YearSeparator{.Offset = 4U, .Length = 1U}; // "-"
        constexpr DateField Month{.Offset = 5U, .Length = 2U};
        constexpr DateField MonthSeparator{.Offset = 7U, .Length = 1U}; // "-"
        constexpr DateField Day{.Offset = 8U, .Length = 2U};
        constexpr DateField DaySeparator{.Offset = 10U, .Length = 1U}; // "T"
        constexpr DateField Hour{.Offset = 11U, .Length = 2U};
        constexpr DateField HourSeparator{.Offset = 13U, .Length = 1U}; // ":"
        constexpr DateField Minute{.Offset = 14U, .Length = 2U};
        constexpr DateField MinuteSeparator{.Offset = 16U, .Length = 1U}; // ":"
        constexpr DateField Second{.Offset = 17U, .Length = 2U};
        constexpr std::size_t FractionOffset = 19U;

        // The service reports 100 ns ticks, i.e. at most seven fractional digits.
        constexpr std::size_t MaxFractionDigits = 7U;
        constexpr std::int64_t NanosecondsPerTick = 100;
        constexpr std::int64_t DecimalRadix = 10;
        // A fractional part must be at least a '.' followed by one digit.
        constexpr std::size_t MinFractionLength = 2U;
    } // namespace Iso8601Layout

    std::optional<std::chrono::system_clock::time_point> ParseIso8601Utc(std::string_view value) noexcept
    {
        using namespace Iso8601Layout;

        if (value.size() < MinLength || value.back() != 'Z' || value.at(YearSeparator.Offset) != '-' ||
            value.at(MonthSeparator.Offset) != '-' || value.at(DaySeparator.Offset) != 'T' ||
            value.at(HourSeparator.Offset) != ':' || value.at(MinuteSeparator.Offset) != ':')
        {
            return std::nullopt;
        }
        const auto year = ParseInt(value.substr(Year.Offset, Year.Length));
        const auto month = ParseUnsignedComponent(value.substr(Month.Offset, Month.Length));
        const auto day = ParseUnsignedComponent(value.substr(Day.Offset, Day.Length));
        const auto hour = ParseUnsignedComponent(value.substr(Hour.Offset, Hour.Length));
        const auto minute = ParseUnsignedComponent(value.substr(Minute.Offset, Minute.Length));
        const auto second = ParseUnsignedComponent(value.substr(Second.Offset, Second.Length));
        if (!year || !month || !day || !hour || !minute || !second)
        {
            return std::nullopt;
        }
        auto result = BuildUtcTimePoint(*year, *month, *day, *hour, *minute, *second);
        std::string_view fraction = value.substr(FractionOffset, value.size() - MinLength);
        if (!result || (!fraction.empty() && (fraction.front() != '.' || fraction.size() < MinFractionLength)))
        {
            return std::nullopt;
        }
        if (!fraction.empty())
        {
            fraction.remove_prefix(1U);
            std::int64_t ticks = 0; // 100 ns units, up to 7 digits
            std::size_t digits = 0;
            for (const char c : fraction)
            {
                if (c < '0' || c > '9')
                {
                    return std::nullopt;
                }
                if (digits < MaxFractionDigits)
                {
                    ticks = (ticks * DecimalRadix) + (c - '0');
                    ++digits;
                }
            }
            for (; digits < MaxFractionDigits; ++digits)
            {
                ticks *= DecimalRadix;
            }
            *result += std::chrono::duration_cast<std::chrono::system_clock::duration>(
                std::chrono::nanoseconds{ticks * NanosecondsPerTick});
        }
        return result;
    }

    Models::AccountInfo ParseAccountInfo(const HttpResponse& response)
    {
        Models::AccountInfo info;
        info.SkuName = std::string{FindHeaderValue(response, "x-ms-sku-name")};
        info.AccountKind = std::string{FindHeaderValue(response, "x-ms-account-kind")};
        info.IsHierarchicalNamespaceEnabled =
            ParseOptionalBool(FindHeaderValue(response, "x-ms-is-hns-enabled"), "x-ms-is-hns-enabled").value_or(false);
        return info;
    }

    namespace
    {
        [[nodiscard]] bool ChildBool(const XmlNode& node, std::string_view tag)
        {
            return ParseOptionalBool(GetChildTextOrEmpty(node, tag), tag).value_or(false);
        }

        [[nodiscard]] std::optional<std::int32_t> ChildInt32(const XmlNode& node, std::string_view tag)
        {
            const auto value = ParseOptionalUnsigned(GetChildTextOrEmpty(node, tag), tag);
            if (value.has_value() && *value > static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max()))
            {
                throw std::invalid_argument(std::string{tag} + " is out of range.");
            }
            return value.has_value() ? std::optional<std::int32_t>{static_cast<std::int32_t>(*value)} : std::nullopt;
        }

        [[nodiscard]] Models::RetentionPolicy ParseRetentionPolicyXml(const XmlNode* node)
        {
            Models::RetentionPolicy policy;
            if (node != nullptr)
            {
                policy.Enabled = ChildBool(*node, "Enabled");
                policy.Days = ChildInt32(*node, "Days");
            }
            return policy;
        }

        [[nodiscard]] Models::Metrics ParseMetricsXml(const XmlNode* node)
        {
            Models::Metrics metrics;
            if (node != nullptr)
            {
                metrics.Version = GetChildTextOrEmpty(*node, "Version");
                metrics.Enabled = ChildBool(*node, "Enabled");
                metrics.IncludeApis = ParseOptionalBool(GetChildTextOrEmpty(*node, "IncludeAPIs"), "IncludeAPIs");
                metrics.RetentionPolicy = ParseRetentionPolicyXml(FindChildByLocalName(*node, "RetentionPolicy"));
            }
            return metrics;
        }
    } // namespace

    Models::BlobServiceProperties ParseBlobServicePropertiesXml(std::string_view xml)
    {
        Models::BlobServiceProperties properties;
        const auto tree = TryReadXmlOrThrow(xml, "Blob service properties response", "StorageServiceProperties");
        const XmlNode* root = tree ? tree->Root() : nullptr;
        if (root == nullptr)
        {
            return properties;
        }

        if (const XmlNode* logging = FindChildByLocalName(*root, "Logging"); logging != nullptr)
        {
            properties.Logging.Version = GetChildTextOrEmpty(*logging, "Version");
            properties.Logging.Delete = ChildBool(*logging, "Delete");
            properties.Logging.Read = ChildBool(*logging, "Read");
            properties.Logging.Write = ChildBool(*logging, "Write");
            properties.Logging.RetentionPolicy =
                ParseRetentionPolicyXml(FindChildByLocalName(*logging, "RetentionPolicy"));
        }
        properties.HourMetrics = ParseMetricsXml(FindChildByLocalName(*root, "HourMetrics"));
        properties.MinuteMetrics = ParseMetricsXml(FindChildByLocalName(*root, "MinuteMetrics"));
        if (const XmlNode* cors = FindChildByLocalName(*root, "Cors"); cors != nullptr)
        {
            for (const XmlNode* rule : GetChildrenByLocalName(*cors, "CorsRule"))
            {
                properties.Cors.push_back(Models::CorsRule{
                    .AllowedOrigins = GetChildTextOrEmpty(*rule, "AllowedOrigins"),
                    .AllowedMethods = GetChildTextOrEmpty(*rule, "AllowedMethods"),
                    .AllowedHeaders = GetChildTextOrEmpty(*rule, "AllowedHeaders"),
                    .ExposedHeaders = GetChildTextOrEmpty(*rule, "ExposedHeaders"),
                    .MaxAgeInSeconds = ChildInt32(*rule, "MaxAgeInSeconds").value_or(0),
                });
            }
        }
        properties.DefaultServiceVersion = GetChildTextOrEmpty(*root, "DefaultServiceVersion");
        properties.DeleteRetentionPolicy =
            ParseRetentionPolicyXml(FindChildByLocalName(*root, "DeleteRetentionPolicy"));
        if (const XmlNode* website = FindChildByLocalName(*root, "StaticWebsite"); website != nullptr)
        {
            properties.StaticWebsite.Enabled = ChildBool(*website, "Enabled");
            properties.StaticWebsite.IndexDocument = GetChildTextOrEmpty(*website, "IndexDocument");
            properties.StaticWebsite.ErrorDocument404Path = GetChildTextOrEmpty(*website, "ErrorDocument404Path");
            properties.StaticWebsite.DefaultIndexDocumentPath =
                GetChildTextOrEmpty(*website, "DefaultIndexDocumentPath");
        }
        return properties;
    }

    Models::UserDelegationKey ParseUserDelegationKeyXml(std::string_view xml)
    {
        Models::UserDelegationKey key;
        const auto tree = TryReadXmlOrThrow(xml, "User delegation key response", "UserDelegationKey");
        const XmlNode* root = tree ? tree->Root() : nullptr;
        if (root == nullptr)
        {
            throw std::invalid_argument("User delegation key response is empty.");
        }
        const auto parseTime = [root](std::string_view tag)
        {
            const auto parsed = ParseIso8601Utc(GetChildTextOrEmpty(*root, tag));
            if (!parsed.has_value())
            {
                throw std::invalid_argument(std::string{tag} + " must be an ISO 8601 UTC time.");
            }
            return *parsed;
        };
        key.SignedObjectId = GetChildTextOrEmpty(*root, "SignedOid");
        key.SignedTenantId = GetChildTextOrEmpty(*root, "SignedTid");
        key.SignedStartsOn = parseTime("SignedStart");
        key.SignedExpiresOn = parseTime("SignedExpiry");
        key.SignedService = GetChildTextOrEmpty(*root, "SignedService");
        key.SignedVersion = GetChildTextOrEmpty(*root, "SignedVersion");
        key.Value = GetChildTextOrEmpty(*root, "Value");
        if (key.Value.empty())
        {
            throw std::invalid_argument("User delegation key response has no Value.");
        }
        return key;
    }

    std::optional<ParsedContentRange> ParseContentRange(std::string_view header) noexcept
    {
        // Expected forms: "bytes start-end/total" or "bytes */total"
        header = TrimWhitespace(header);
        if (header.empty())
        {
            return std::nullopt;
        }
        const std::string_view prefix = "bytes ";
        if (!IStartsWith(header, prefix))
        {
            return std::nullopt;
        }
        header.remove_prefix(prefix.size());
        const std::size_t slash = header.find('/');
        if (slash == std::string_view::npos)
        {
            return std::nullopt;
        }
        const std::string_view rangePart = TrimWhitespace(header.substr(0, slash));
        const std::string_view totalPart = TrimWhitespace(header.substr(slash + 1));

        std::optional<std::uint64_t> total;
        if (!totalPart.empty() && totalPart != "*")
        {
            std::uint64_t t = 0;
            const auto r = std::from_chars(std::to_address(totalPart.begin()), std::to_address(totalPart.end()), t);
            if (r.ec != std::errc{} || r.ptr != std::to_address(totalPart.end()))
            {
                return std::nullopt;
            }
            total = t;
        }

        if (rangePart == "*")
        {
            // "bytes */*" carries no information and is invalid.
            if (!total.has_value())
            {
                return std::nullopt;
            }
            return ParsedContentRange{.Start = 0, .End = 0, .Total = total, .Unsatisfied = true};
        }

        const std::size_t dash = rangePart.find('-');
        if (dash == std::string_view::npos)
        {
            return std::nullopt;
        }

        const std::string_view startPart = TrimWhitespace(rangePart.substr(0, dash));
        const std::string_view endPart = TrimWhitespace(rangePart.substr(dash + 1));
        if (startPart.empty() || endPart.empty())
        {
            return std::nullopt;
        }

        std::uint64_t start = 0;
        std::uint64_t end = 0;
        const auto rs = std::from_chars(std::to_address(startPart.begin()), std::to_address(startPart.end()), start);
        const auto re = std::from_chars(std::to_address(endPart.begin()), std::to_address(endPart.end()), end);
        if (rs.ec != std::errc{} || rs.ptr != std::to_address(startPart.end()) || re.ec != std::errc{} ||
            re.ptr != std::to_address(endPart.end()))
        {
            return std::nullopt;
        }

        // RFC 9110 14.4: invalid if last < first, or if the complete length is not greater than last.
        if (end < start || (total.has_value() && *total <= end))
        {
            return std::nullopt;
        }

        return ParsedContentRange{.Start = start, .End = end, .Total = total};
    }

    Models::BlobProperties ParseBlobProperties(const HttpResponse& response)
    {
        Models::BlobProperties properties;
        ApplyETagAndLastModified(properties, response);

        // Prefer Content-Range total when present (for 206 responses)
        bool contentLengthPresent = false;
        const std::string_view contentRangeHeader = FindHeaderValue(response, "Content-Range");
        if (!contentRangeHeader.empty())
        {
            if (const auto parsed = ParseContentRange(contentRangeHeader);
                parsed.has_value() && parsed->Total.has_value())
            {
                properties.ContentLength = *parsed->Total;
                contentLengthPresent = true;
            }
        }

        if (!contentLengthPresent)
        {
            if (const std::string_view contentLength = FindHeaderValue(response, ContentLengthHeaderName);
                !contentLength.empty())
            {
                const auto parsed = ParseUnsigned(contentLength);
                if (!parsed.has_value())
                {
                    throw std::invalid_argument("Content-Length header must be an unsigned integer.");
                }

                properties.ContentLength = *parsed;
            }
        }
        properties.ContentType = std::string{FindHeaderValue(response, ContentTypeHeaderName)};
        properties.ContentMd5 = std::string{FindHeaderValue(response, ContentMd5HeaderName)};
        properties.CacheControl = std::string{FindHeaderValue(response, CacheControlHeaderName)};
        properties.Type = ParseBlobTypeValue(FindHeaderValue(response, XMsBlobTypeHeaderName));
        ParseExtendedBlobProperties(properties,
            HeaderPropertyNames,
            [&response](std::string_view name)
        {
            return FindHeaderValue(response, name);
        });
        properties.VersionId = std::string{FindHeaderValue(response, "x-ms-version-id")};
        properties.IsCurrentVersion =
            ParseOptionalBool(FindHeaderValue(response, "x-ms-is-current-version"), "x-ms-is-current-version");
        for (const auto& header : response.GetHeaders())
        {
            const std::string_view name = header.GetName();
            if (IStartsWith(name, XMsMetaHeaderPrefix))
            {
                properties.Metadata.emplace(std::string{name.substr(XMsMetaHeaderPrefix.size())}, header.GetValue());
            }
        }
        return properties;
    }

    Models::DeleteBlobResult ParseDeleteBlobResult(const HttpResponse& response)
    {
        Models::DeleteBlobResult result;
        result.RequestId = std::string{FindHeaderValue(response, XMsRequestIdHeaderName)};
        const std::string_view permanentDelete = FindHeaderValue(response, XMsDeleteTypePermanentHeaderName);
        if (!permanentDelete.empty())
        {
            result.DeleteTypePermanent = ParseBoolHeader(permanentDelete);
        }
        return result;
    }

    // Moves the body out of `response` into `result.Content` instead of copying it, so the HttpResponse
    // retained inside the caller's Response<T> does not also hold a duplicate of the blob. The transient
    // std::string -> std::vector<std::byte> conversion is still a copy (the element types differ), but it is the
    // only extra one and is freed when this function returns.
    Models::DownloadBlobResult ParseDownloadBlobResult(HttpResponse& response)
    {
        Models::DownloadBlobResult result;
        result.Properties = ParseBlobProperties(response);
        // Prefer to populate ContentRange when present on the response
        const std::string_view contentRangeHeader = FindHeaderValue(response, "Content-Range");
        if (!contentRangeHeader.empty())
        {
            if (const auto parsed = ParseContentRange(contentRangeHeader); parsed.has_value())
            {
                Models::BlobByteRange r;
                r.Offset = parsed->Start;
                r.Length = (parsed->End - parsed->Start) + 1;
                result.ContentRange = r;
            }
        }
        // Move the response body into the result to avoid an extra persistent duplicate copy.
        result.Content = std::move(response).GetBody();
        return result;
    }

    Models::StartBlobCopyFromUriResult ParseStartBlobCopyFromUriResult(const HttpResponse& response)
    {
        Models::StartBlobCopyFromUriResult result;
        result.CopyId = std::string{FindHeaderValue(response, XMsCopyIdHeaderName)};
        result.CopyStatus = std::string{FindHeaderValue(response, XMsCopyStatusHeaderName)};
        return result;
    }

    Models::CreateBlobSnapshotResult ParseCreateBlobSnapshotResult(const HttpResponse& response)
    {
        Models::CreateBlobSnapshotResult result;
        result.Snapshot = std::string{FindHeaderValue(response, XMsSnapshotHeaderName)};
        ApplyETagAndLastModified(result, response);
        return result;
    }

    Models::AcquireBlobLeaseResult ParseAcquireBlobLeaseResult(const HttpResponse& response)
    {
        Models::AcquireBlobLeaseResult result;
        result.LeaseId = std::string{FindHeaderValue(response, XMsLeaseIdHeaderName)};
        ApplyETagAndLastModified(result, response);
        return result;
    }

    Models::RenewBlobLeaseResult ParseRenewBlobLeaseResult(const HttpResponse& response)
    {
        auto result = ParseETagAndLastModified<Models::RenewBlobLeaseResult>(response);
        result.LeaseId = std::string{FindHeaderValue(response, XMsLeaseIdHeaderName)};
        return result;
    }

    Models::AppendBlockResult ParseAppendBlockResult(const HttpResponse& response)
    {
        auto result = ParseETagAndLastModified<Models::AppendBlockResult>(response);
        result.AppendOffset = ParseOptionalUnsigned(FindHeaderValue(response, XMsBlobAppendOffsetHeaderName),
            XMsBlobAppendOffsetHeaderName)
                                  .value_or(0);
        result.CommittedBlockCount =
            ParseOptionalUnsigned(FindHeaderValue(response, XMsBlobCommittedBlockCountHeaderName),
                XMsBlobCommittedBlockCountHeaderName)
                .value_or(0);
        return result;
    }

    Models::SealAppendBlobResult ParseSealAppendBlobResult(const HttpResponse& response)
    {
        auto result = ParseETagAndLastModified<Models::SealAppendBlobResult>(response);
        result.IsSealed = ParseOptionalBool(FindHeaderValue(response, XMsBlobSealedHeaderName), XMsBlobSealedHeaderName)
                              .value_or(true);
        return result;
    }

    Models::CopyBlobFromUriResult ParseCopyBlobFromUriResult(const HttpResponse& response)
    {
        auto result = ParseETagAndLastModified<Models::CopyBlobFromUriResult>(response);
        result.CopyId = std::string{FindHeaderValue(response, XMsCopyIdHeaderName)};
        result.CopyStatus = std::string{FindHeaderValue(response, XMsCopyStatusHeaderName)};
        result.ContentMd5 = std::string{FindHeaderValue(response, ContentMd5HeaderName)};
        result.ContentCrc64 = std::string{FindHeaderValue(response, XMsContentCrc64HeaderName)};
        return result;
    }

    Models::AbortCopyBlobFromUriResult ParseAbortCopyBlobFromUriResult(const HttpResponse& response)
    {
        return Models::AbortCopyBlobFromUriResult{
            .RequestId = std::string{FindHeaderValue(response, XMsRequestIdHeaderName)}};
    }

    Models::ChangeBlobLeaseResult ParseChangeBlobLeaseResult(const HttpResponse& response)
    {
        auto result = ParseETagAndLastModified<Models::ChangeBlobLeaseResult>(response);
        result.LeaseId = std::string{FindHeaderValue(response, XMsLeaseIdHeaderName)};
        return result;
    }

    Models::ReleaseBlobLeaseResult ParseReleaseBlobLeaseResult(const HttpResponse& response)
    {
        return ParseETagAndLastModified<Models::ReleaseBlobLeaseResult>(response);
    }

    Models::BreakBlobLeaseResult ParseBreakBlobLeaseResult(const HttpResponse& response)
    {
        Models::BreakBlobLeaseResult result;
        if (const std::string_view leaseTime = FindHeaderValue(response, XMsLeaseTimeHeaderName); !leaseTime.empty())
        {
            if (const auto parsed = ParseInt(leaseTime); parsed.has_value())
            {
                result.LeaseTimeSeconds = parsed;
            }
        }
        ApplyETagAndLastModified(result, response);
        return result;
    }

    namespace
    {
        // <Name Encoded="true"> carries a percent-encoded name (used for names with characters invalid in XML).
        [[nodiscard]] std::string GetBlobItemName(const XmlNode& item)
        {
            const XmlNode* name = FindChildByLocalName(item, "Name");
            if (name == nullptr)
            {
                return {};
            }
            std::string text = GetNodeText(*name);
            const auto encoded = GetAttribute(*name, "Encoded");
            if (!encoded.has_value() || !IEquals(*encoded, "true"))
            {
                return text;
            }
            const auto decoded = boost::urls::make_pct_string_view(text);
            if (!decoded)
            {
                throw std::invalid_argument("Encoded blob name is not valid percent-encoding.");
            }
            return decoded->decode();
        }

        [[nodiscard]] Models::BlobTags ParseBlobTagsXml(const XmlNode& tags)
        {
            Models::BlobTags result;
            const XmlNode* tagSet = FindChildByLocalName(tags, "TagSet");
            for (const XmlNode* tag : GetChildrenByLocalName(tagSet != nullptr ? *tagSet : tags, "Tag"))
            {
                result.insert_or_assign(GetChildTextOrEmpty(*tag, "Key"), GetChildTextOrEmpty(*tag, "Value"));
            }
            return result;
        }
    } // namespace

    Models::ListBlobsResult ParseListBlobsResultXml(std::string_view xml)
    {
        Models::ListBlobsResult result;
        const auto tree = TryReadXmlOrThrow(xml, "List blobs response", "EnumerationResults");
        const XmlNode* root = tree ? tree->Root() : nullptr;
        if (root == nullptr)
        {
            return result;
        }

        result.Prefix = GetChildTextOrEmpty(*root, "Prefix");
        result.Delimiter = GetChildTextOrEmpty(*root, "Delimiter");
        result.Marker = GetChildTextOrEmpty(*root, "Marker");
        result.NextMarker = GetChildTextOrEmpty(*root, "NextMarker");

        if (const XmlNode* blobs = FindChildByLocalName(*root, "Blobs"); blobs != nullptr)
        {
            for (const XmlNode* block : GetChildrenByLocalName(*blobs, "Blob"))
            {
                Models::BlobItem item;
                item.Name = GetBlobItemName(*block);
                item.Deleted = ParseOptionalBool(GetChildTextOrEmpty(*block, "Deleted"), "Deleted").value_or(false);
                if (const XmlNode* tags = FindChildByLocalName(*block, "Tags"); tags != nullptr)
                {
                    item.Tags = ParseBlobTagsXml(*tags);
                }
                item.Snapshot = GetChildTextOrEmpty(*block, "Snapshot");
                if (const XmlNode* properties = FindChildByLocalName(*block, "Properties"); properties != nullptr)
                {
                    item.Properties = ParseBlobPropertiesXml(*properties);
                }
                // VersionId and IsCurrentVersion are siblings of <Properties>.
                item.Properties.VersionId = GetChildTextOrEmpty(*block, "VersionId");
                item.Properties.IsCurrentVersion =
                    ParseOptionalBool(GetChildTextOrEmpty(*block, "IsCurrentVersion"), "IsCurrentVersion");
                // The service returns <Metadata> as a sibling of <Properties>.
                if (const XmlNode* metadata = FindChildByLocalName(*block, "Metadata"); metadata != nullptr)
                {
                    item.Properties.Metadata = ParseMetadataXml(*metadata);
                }
                result.Blobs.push_back(std::move(item));
            }

            for (const XmlNode* block : GetChildrenByLocalName(*blobs, "BlobPrefix"))
            {
                result.BlobPrefixes.push_back(GetBlobItemName(*block));
            }
        }

        return result;
    }

    Models::GetBlobTagsResult ParseGetBlobTagsResultXml(std::string_view xml)
    {
        Models::GetBlobTagsResult result;
        const auto tree = TryReadXmlOrThrow(xml, "Get blob tags response", "Tags");
        if (const XmlNode* root = tree ? tree->Root() : nullptr; root != nullptr)
        {
            result.Tags = ParseBlobTagsXml(*root);
        }
        return result;
    }

    Models::FindBlobsByTagsResult ParseFindBlobsByTagsResultXml(std::string_view xml)
    {
        Models::FindBlobsByTagsResult result;
        const auto tree = TryReadXmlOrThrow(xml, "Find blobs by tags response", "EnumerationResults");
        const XmlNode* root = tree ? tree->Root() : nullptr;
        if (root == nullptr)
        {
            return result;
        }
        result.Where = GetChildTextOrEmpty(*root, "Where");
        result.NextMarker = GetChildTextOrEmpty(*root, "NextMarker");
        if (const XmlNode* blobs = FindChildByLocalName(*root, "Blobs"); blobs != nullptr)
        {
            for (const XmlNode* blob : GetChildrenByLocalName(*blobs, "Blob"))
            {
                Models::TaggedBlobItem item;
                item.BlobName = GetChildTextOrEmpty(*blob, "Name");
                item.ContainerName = GetChildTextOrEmpty(*blob, "ContainerName");
                if (const XmlNode* tags = FindChildByLocalName(*blob, "Tags"); tags != nullptr)
                {
                    item.Tags = ParseBlobTagsXml(*tags);
                }
                result.Blobs.push_back(std::move(item));
            }
        }
        return result;
    }

    Models::ListBlobContainersResult ParseListBlobContainersResultXml(std::string_view xml)
    {
        Models::ListBlobContainersResult result;
        const auto tree = TryReadXmlOrThrow(xml, "List blob containers response", "EnumerationResults");
        const XmlNode* root = tree ? tree->Root() : nullptr;
        if (root == nullptr)
        {
            return result;
        }

        result.Prefix = GetChildTextOrEmpty(*root, "Prefix");
        result.Marker = GetChildTextOrEmpty(*root, "Marker");
        result.NextMarker = GetChildTextOrEmpty(*root, "NextMarker");

        if (const XmlNode* containers = FindChildByLocalName(*root, "Containers"); containers != nullptr)
        {
            for (const XmlNode* block : GetChildrenByLocalName(*containers, "Container"))
            {
                Models::BlobContainerItem item;
                item.Name = GetChildTextOrEmpty(*block, "Name");
                if (const XmlNode* properties = FindChildByLocalName(*block, "Properties"); properties != nullptr)
                {
                    item.Properties.ETag = GetChildTextOrEmpty(*properties, "Etag");
                    if (const std::string lastModified = GetChildTextOrEmpty(*properties, "Last-Modified");
                        !lastModified.empty())
                    {
                        const auto parsed = ParseHttpDateHeader(lastModified);
                        if (!parsed.has_value())
                        {
                            throw std::invalid_argument(
                                "Container Properties Last-Modified must contain a valid HTTP-date.");
                        }

                        item.Properties.LastModified = *parsed;
                    }
                    item.Properties.AccessType =
                        ParsePublicAccessType(GetChildTextOrEmpty(*properties, "PublicAccess"));
                    item.Properties.HasImmutabilityPolicy =
                        ParseBoolHeader(GetChildTextOrEmpty(*properties, "HasImmutabilityPolicy"));
                    item.Properties.HasLegalHold = ParseBoolHeader(GetChildTextOrEmpty(*properties, "HasLegalHold"));
                    item.Properties.Status = ParseLeaseStatus(GetChildTextOrEmpty(*properties, "LeaseStatus"));
                    item.Properties.State = ParseLeaseState(GetChildTextOrEmpty(*properties, "LeaseState"));
                    item.Properties.DurationType =
                        ParseLeaseDurationType(GetChildTextOrEmpty(*properties, "LeaseDuration"));
                    item.Properties.DefaultEncryptionScope = GetChildTextOrEmpty(*properties, "DefaultEncryptionScope");
                    item.Properties.PreventEncryptionScopeOverride =
                        ParseBoolHeader(GetChildTextOrEmpty(*properties, "DenyEncryptionScopeOverride"));
                }
                if (const XmlNode* metadata = FindChildByLocalName(*block, "Metadata"); metadata != nullptr)
                {
                    item.Properties.Metadata = ParseMetadataXml(*metadata);
                }
                result.Containers.push_back(std::move(item));
            }
        }

        return result;
    }

    Models::GetBlockListResult ParseGetBlockListResultXml(std::string_view xml)
    {
        auto parseBlocks = [](const XmlNode& blockList)
        {
            std::vector<Models::BlockListBlock> blocks;
            for (const XmlNode* block : GetChildrenByLocalName(blockList, "Block"))
            {
                Models::BlockListBlock item;
                item.Name = GetChildTextOrEmpty(*block, "Name");
                if (const std::string size = GetChildTextOrEmpty(*block, "Size"); !size.empty())
                {
                    const auto parsed = ParseUnsigned(size);
                    if (!parsed.has_value())
                    {
                        throw std::invalid_argument("Block Size must be an unsigned integer.");
                    }

                    item.Size = *parsed;
                }
                blocks.push_back(std::move(item));
            }
            return blocks;
        };

        Models::GetBlockListResult result;
        const auto tree = TryReadXmlOrThrow(xml, "Get block list response", "BlockList");
        const XmlNode* root = tree ? tree->Root() : nullptr;
        if (root == nullptr)
        {
            return result;
        }

        if (const XmlNode* committed = FindChildByLocalName(*root, "CommittedBlocks"); committed != nullptr)
        {
            result.CommittedBlocks = parseBlocks(*committed);
        }
        if (const XmlNode* uncommitted = FindChildByLocalName(*root, "UncommittedBlocks"); uncommitted != nullptr)
        {
            result.UncommittedBlocks = parseBlocks(*uncommitted);
        }

        return result;
    }

    Models::GetPageRangesResult ParseGetPageRangesResultXml(std::string_view xml)
    {
        Models::GetPageRangesResult result;
        const auto tree = TryReadXmlOrThrow(xml, "Get page ranges response", "PageList");
        const XmlNode* root = tree ? tree->Root() : nullptr;
        if (root == nullptr)
        {
            return result;
        }

        for (const XmlNode* block : GetChildrenByLocalName(*root, "PageRange"))
        {
            Models::PageRange range;
            if (const std::string start = GetChildTextOrEmpty(*block, "Start"); !start.empty())
            {
                const auto parsed = ParseUnsigned(start);
                if (!parsed.has_value())
                {
                    throw std::invalid_argument("PageRange Start must be an unsigned integer.");
                }

                range.Start = *parsed;
            }
            if (const std::string end = GetChildTextOrEmpty(*block, "End"); !end.empty())
            {
                const auto parsed = ParseUnsigned(end);
                if (!parsed.has_value())
                {
                    throw std::invalid_argument("PageRange End must be an unsigned integer.");
                }

                range.End = *parsed;
            }
            result.PageRanges.push_back(range);
        }
        result.NextMarker = GetChildTextOrEmpty(*root, "NextMarker");
        return result;
    }
} // namespace AVEVA::AzureClient::Private
