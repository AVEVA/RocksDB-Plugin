// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobServiceClient.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/TokenCredential.hpp"
#include "BlobStorageErrorCategory.hpp"
#include "BlobXmlParser.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
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
#include <bitset>
#include <charconv>
#include <chrono>
#include <cstring>
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
namespace AVEVA::AzureClient::Private {
namespace {

[[nodiscard]] Models::BlobType ParseBlobTypeValue(std::string_view value) {
    if (IEquals(value, "BlockBlob")) {
        return Models::BlobType::BlockBlob;
    }
    if (IEquals(value, "PageBlob")) {
        return Models::BlobType::PageBlob;
    }
    if (IEquals(value, "AppendBlob")) {
        return Models::BlobType::AppendBlob;
    }
    return Models::BlobType::Unknown;
}

[[nodiscard]] std::optional<std::uint64_t> ParseUnsigned(std::string_view value) noexcept {
    if (value.empty()) {
        return std::nullopt;
    }

    std::uint64_t parsed = 0;
    const auto result = std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
    if (result.ec != std::errc{} || result.ptr != std::to_address(value.end())) {
        return std::nullopt;
    }

    return parsed;
}

// Tags the error-message field name with its own type so it's never adjacent-and-same-type
// with the raw value parameter it describes.
struct FieldNameTag {};

using FieldName = StringLabel<FieldNameTag>;

[[nodiscard]] std::optional<bool> ParseOptionalBool(std::string_view value, FieldName what) {
    if (value.empty()) {
        return std::nullopt;
    }
    if (IEquals(value, "true")) {
        return true;
    }
    if (IEquals(value, "false")) {
        return false;
    }
    throw std::invalid_argument(std::string{what.Value} + " must be true or false.");
}

[[nodiscard]] std::optional<std::uint64_t> ParseOptionalUnsigned(std::string_view value, FieldName what) {
    if (value.empty()) {
        return std::nullopt;
    }
    const auto parsed = ParseUnsigned(value);
    if (!parsed.has_value()) {
        throw std::invalid_argument(std::string{what.Value} + " must be an unsigned integer.");
    }
    return parsed;
}

[[nodiscard]] std::optional<std::chrono::system_clock::time_point> ParseOptionalHttpDate(std::string_view value,
                                                                                         FieldName what) {
    if (value.empty()) {
        return std::nullopt;
    }
    const auto parsed = ParseHttpDateHeader(value);
    if (!parsed.has_value()) {
        throw std::invalid_argument(std::string{what.Value} + " must contain a valid HTTP-date.");
    }
    return parsed;
}

// Where each extended blob property lives in a Get Blob Properties response (headers) or a
// List Blobs <Properties> element; the same parsing applies to both.
struct BlobPropertyNames {
    std::string_view ContentEncoding;
    std::string_view ContentLanguage;
    std::string_view ContentDisposition;
    std::string_view CreatedOn;
    std::string_view LeaseStatus;
    std::string_view LeaseState;
    std::string_view LeaseDuration;
    std::string_view ServerEncrypted;
    std::string_view SequenceNumber;
};

constexpr BlobPropertyNames HeaderPropertyNames{.ContentEncoding = "Content-Encoding",
                                                .ContentLanguage = "Content-Language",
                                                .ContentDisposition = "Content-Disposition",
                                                .CreatedOn = "x-ms-creation-time",
                                                .LeaseStatus = "x-ms-lease-status",
                                                .LeaseState = "x-ms-lease-state",
                                                .LeaseDuration = "x-ms-lease-duration",
                                                .ServerEncrypted = "x-ms-server-encrypted",
                                                .SequenceNumber = "x-ms-blob-sequence-number"};

constexpr BlobPropertyNames XmlPropertyNames{.ContentEncoding = "Content-Encoding",
                                             .ContentLanguage = "Content-Language",
                                             .ContentDisposition = "Content-Disposition",
                                             .CreatedOn = "Creation-Time",
                                             .LeaseStatus = "LeaseStatus",
                                             .LeaseState = "LeaseState",
                                             .LeaseDuration = "LeaseDuration",
                                             .ServerEncrypted = "ServerEncrypted",
                                             .SequenceNumber = "x-ms-blob-sequence-number"};

// Raw (unparsed) values for the extended properties; empty when absent.
struct ExtendedBlobValues {
    std::string_view ContentEncoding;
    std::string_view ContentLanguage;
    std::string_view ContentDisposition;
    std::string_view CreatedOn;
    std::string_view LeaseStatus;
    std::string_view LeaseState;
    std::string_view LeaseDuration;
    std::string_view ServerEncrypted;
    std::string_view SequenceNumber;
};

// Throws std::invalid_argument on malformed values.
void ParseExtendedBlobProperties(Models::BlobProperties& properties, const BlobPropertyNames& names,
                                 const ExtendedBlobValues& values) {
    properties.ContentEncoding = std::string{values.ContentEncoding};
    properties.ContentLanguage = std::string{values.ContentLanguage};
    properties.ContentDisposition = std::string{values.ContentDisposition};
    properties.CreatedOn = ParseOptionalHttpDate(values.CreatedOn, names.CreatedOn);
    properties.LeaseStatus = ParseLeaseStatus(values.LeaseStatus);
    properties.LeaseState = ParseLeaseState(values.LeaseState);
    properties.LeaseDuration = ParseLeaseDurationType(values.LeaseDuration);
    properties.ServerEncrypted = ParseOptionalBool(values.ServerEncrypted, names.ServerEncrypted);
    properties.SequenceNumber = ParseOptionalUnsigned(values.SequenceNumber, names.SequenceNumber);
}

[[nodiscard]] std::optional<int> ParseInt(std::string_view value) noexcept {
    if (value.empty()) {
        return std::nullopt;
    }

    int parsed = 0;
    const auto result = std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
    if (result.ec != std::errc{} || result.ptr != std::to_address(value.end())) {
        return std::nullopt;
    }
    return parsed;
}

[[nodiscard]] std::optional<unsigned int> ParseUnsignedComponent(std::string_view value) noexcept {
    if (value.empty()) {
        return std::nullopt;
    }

    unsigned int parsed = 0;
    const auto result = std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
    if (result.ec != std::errc{} || result.ptr != std::to_address(value.end())) {
        return std::nullopt;
    }

    return parsed;
}

[[nodiscard]] std::optional<unsigned int> ParseMonth(std::string_view value) noexcept {
    for (std::size_t index = 0; index < MonthNames.size(); ++index) {
        if (IEquals(MonthNames.at(index), value)) {
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
struct DateField {
    std::size_t Offset;
    std::size_t Length;
};

// Wraps every other time-of-day/calendar component so no two adjacent parameters of
// BuildUtcTimePoint share a plain `unsigned int` type.
struct UtcTimeComponentTag {};

using UtcTimeComponent = NumericLabel<unsigned int, UtcTimeComponentTag>;

[[nodiscard]] std::optional<std::chrono::system_clock::time_point>
BuildUtcTimePoint(int yearValue, UtcTimeComponent monthValue, unsigned int dayValue, UtcTimeComponent hourValue,
                  unsigned int minuteValue, UtcTimeComponent secondValue) noexcept {
    using namespace std::chrono;

    const year_month_day calendarDate{year{yearValue}, month{monthValue.Value}, day{dayValue}};
    if (!calendarDate.ok() || hourValue.Value > MaxHourOfDay || minuteValue > MaxMinuteOfHour ||
        secondValue.Value > MaxSecondOfMinute) {
        return std::nullopt;
    }

    const sys_days days{calendarDate};
    return system_clock::time_point{duration_cast<system_clock::duration>(
        days.time_since_epoch() + hours{hourValue.Value} + minutes{minuteValue} + seconds{secondValue.Value})};
}

// "Sun, 06 Nov 1994 08:49:37 GMT" - the preferred HTTP-date format (RFC 9110 5.6.7).
namespace Rfc1123Layout {
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

[[nodiscard]] std::optional<std::chrono::system_clock::time_point>
ParseRfc1123HttpDate(std::string_view value) noexcept {
    using namespace Rfc1123Layout;

    if (value.size() != TotalLength || value.substr(WeekdaySeparator.Offset, WeekdaySeparator.Length) != ", " ||
        value.substr(DaySeparator.Offset, DaySeparator.Length) != " " ||
        value.substr(MonthSeparator.Offset, MonthSeparator.Length) != " " ||
        value.substr(YearSeparator.Offset, YearSeparator.Length) != " " ||
        value.substr(HourSeparator.Offset, HourSeparator.Length) != ":" ||
        value.substr(MinuteSeparator.Offset, MinuteSeparator.Length) != ":" ||
        !IEquals(value.substr(Zone.Offset, Zone.Length), " GMT")) {
        return std::nullopt;
    }

    const std::string_view weekday = value.substr(Weekday.Offset, Weekday.Length);
    if (std::ranges::find_if(ShortWeekdayNames, [weekday](std::string_view expected) {
            return IEquals(expected, weekday);
        }) == ShortWeekdayNames.end()) {
        return std::nullopt;
    }

    const auto day = ParseUnsignedComponent(value.substr(Day.Offset, Day.Length));
    const auto month = ParseMonth(value.substr(Month.Offset, Month.Length));
    const auto year = ParseInt(value.substr(Year.Offset, Year.Length));
    const auto hour = ParseUnsignedComponent(value.substr(Hour.Offset, Hour.Length));
    const auto minute = ParseUnsignedComponent(value.substr(Minute.Offset, Minute.Length));
    const auto second = ParseUnsignedComponent(value.substr(Second.Offset, Second.Length));
    if (!day.has_value() || !month.has_value() || !year.has_value() || !hour.has_value() || !minute.has_value() ||
        !second.has_value()) {
        return std::nullopt;
    }

    return BuildUtcTimePoint(*year, *month, *day, *hour, *minute, *second);
}

// "Sunday, 06-Nov-94 08:49:37 GMT" - the obsolete RFC 850 format. Offsets are relative to the
// remainder that follows "<weekday>, ".
namespace Rfc850Layout {
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

[[nodiscard]] std::optional<std::chrono::system_clock::time_point>
ParseRfc850HttpDate(std::string_view value) noexcept {
    using namespace Rfc850Layout;

    const std::size_t comma = value.find(", ");
    if (comma == std::string_view::npos || comma == 0U) {
        return std::nullopt;
    }

    const std::string_view weekday = value.substr(0U, comma);
    if (std::ranges::find_if(LongWeekdayNames, [weekday](std::string_view expected) {
            return IEquals(expected, weekday);
        }) == LongWeekdayNames.end()) {
        return std::nullopt;
    }

    const std::string_view remainder = value.substr(comma + WeekdaySeparatorLength);
    if (remainder.size() != RemainderLength || remainder.at(DaySeparator.Offset) != '-' ||
        remainder.at(MonthSeparator.Offset) != '-' || remainder.at(YearSeparator.Offset) != ' ' ||
        remainder.at(HourSeparator.Offset) != ':' || remainder.at(MinuteSeparator.Offset) != ':' ||
        !IEquals(remainder.substr(Zone.Offset, Zone.Length), " GMT")) {
        return std::nullopt;
    }

    const auto day = ParseUnsignedComponent(remainder.substr(Day.Offset, Day.Length));
    const auto month = ParseMonth(remainder.substr(Month.Offset, Month.Length));
    const auto shortYear = ParseUnsignedComponent(remainder.substr(Year.Offset, Year.Length));
    const auto hour = ParseUnsignedComponent(remainder.substr(Hour.Offset, Hour.Length));
    const auto minute = ParseUnsignedComponent(remainder.substr(Minute.Offset, Minute.Length));
    const auto second = ParseUnsignedComponent(remainder.substr(Second.Offset, Second.Length));
    if (!day.has_value() || !month.has_value() || !shortYear.has_value() || !hour.has_value() || !minute.has_value() ||
        !second.has_value()) {
        return std::nullopt;
    }

    const int currentYear = static_cast<int>(
        std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now())}.year());
    int year = CenturyBase + static_cast<int>(*shortYear);
    if (year > (currentYear + MaxYearsAhead)) {
        year -= YearsPerCentury;
    }

    return BuildUtcTimePoint(year, *month, *day, *hour, *minute, *second);
}

// "Sun Nov  6 08:49:37 1994" - the ANSI C asctime() format.
namespace AsctimeLayout {
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

[[nodiscard]] std::optional<std::chrono::system_clock::time_point>
ParseAsctimeHttpDate(std::string_view value) noexcept {
    using namespace AsctimeLayout;

    if (value.size() != TotalLength || value.at(WeekdaySeparator.Offset) != ' ' ||
        value.at(MonthSeparator.Offset) != ' ' || value.at(DaySeparator.Offset) != ' ' ||
        value.at(HourSeparator.Offset) != ':' || value.at(MinuteSeparator.Offset) != ':' ||
        value.at(SecondSeparator.Offset) != ' ') {
        return std::nullopt;
    }

    const std::string_view weekday = value.substr(Weekday.Offset, Weekday.Length);
    if (std::ranges::find_if(ShortWeekdayNames, [weekday](std::string_view expected) {
            return IEquals(expected, weekday);
        }) == ShortWeekdayNames.end()) {
        return std::nullopt;
    }

    const auto month = ParseMonth(value.substr(Month.Offset, Month.Length));
    const auto day = ParseUnsignedComponent(TrimWhitespace(value.substr(Day.Offset, Day.Length)));
    const auto hour = ParseUnsignedComponent(value.substr(Hour.Offset, Hour.Length));
    const auto minute = ParseUnsignedComponent(value.substr(Minute.Offset, Minute.Length));
    const auto second = ParseUnsignedComponent(value.substr(Second.Offset, Second.Length));
    const auto year = ParseInt(value.substr(Year.Offset, Year.Length));
    if (!month.has_value() || !day.has_value() || !hour.has_value() || !minute.has_value() || !second.has_value() ||
        !year.has_value()) {
        return std::nullopt;
    }

    return BuildUtcTimePoint(*year, *month, *day, *hour, *minute, *second);
}

[[nodiscard]] Models::MetadataMap ParseMetadataXml(const XmlNode& node) {
    Models::MetadataMap metadata;
    ForEachElement(node, [&](const XmlNode& child) {
        metadata.emplace(std::string{GetLocalName(NodeName(child))}, GetNodeText(child));
    });
    return metadata;
}

[[nodiscard]] Models::BlobProperties ParseBlobPropertiesXml(const XmlNode& node) {
    Models::BlobProperties properties;
    properties.ETag = GetChildTextOrEmpty(node, "Etag");
    if (const std::string lastModified = GetChildTextOrEmpty(node, "Last-Modified"); !lastModified.empty()) {
        const auto parsed = ParseHttpDateHeader(lastModified);
        if (!parsed.has_value()) {
            throw std::invalid_argument("Blob Properties Last-Modified must contain a valid HTTP-date.");
        }

        properties.LastModified = *parsed;
    }

    if (const std::string contentLength = GetChildTextOrEmpty(node, "Content-Length"); !contentLength.empty()) {
        const auto parsed = ParseUnsigned(contentLength);
        if (!parsed.has_value()) {
            throw std::invalid_argument("Blob Properties Content-Length must be an unsigned integer.");
        }

        properties.ContentLength = *parsed;
    }
    properties.ContentType = GetChildTextOrEmpty(node, "Content-Type");
    properties.ContentMd5 = GetChildTextOrEmpty(node, "Content-MD5");
    properties.CacheControl = GetChildTextOrEmpty(node, "Cache-Control");
    properties.Type = ParseBlobTypeValue(GetChildTextOrEmpty(node, "BlobType"));
    ParseExtendedBlobProperties(
        properties, XmlPropertyNames,
        ExtendedBlobValues{.ContentEncoding = GetChildTextOrEmpty(node, XmlPropertyNames.ContentEncoding),
                           .ContentLanguage = GetChildTextOrEmpty(node, XmlPropertyNames.ContentLanguage),
                           .ContentDisposition = GetChildTextOrEmpty(node, XmlPropertyNames.ContentDisposition),
                           .CreatedOn = GetChildTextOrEmpty(node, XmlPropertyNames.CreatedOn),
                           .LeaseStatus = GetChildTextOrEmpty(node, XmlPropertyNames.LeaseStatus),
                           .LeaseState = GetChildTextOrEmpty(node, XmlPropertyNames.LeaseState),
                           .LeaseDuration = GetChildTextOrEmpty(node, XmlPropertyNames.LeaseDuration),
                           .ServerEncrypted = GetChildTextOrEmpty(node, XmlPropertyNames.ServerEncrypted),
                           .SequenceNumber = GetChildTextOrEmpty(node, XmlPropertyNames.SequenceNumber)});
    if (const XmlNode* metadata = FindChildByLocalName(node, "Metadata"); metadata != nullptr) {
        properties.Metadata = ParseMetadataXml(*metadata);
    }
    return properties;
}
} // namespace

std::string_view FindHeaderValue(const HttpResponse& response, std::string_view headerName) noexcept {
    for (const auto& header : response.GetHeaders()) {
        if (IEquals(header.GetName(), headerName)) {
            return header.GetValue();
        }
    }
    return {};
}

Models::LeaseStatus ParseLeaseStatus(std::string_view value) {
    if (value.empty()) {
        return Models::LeaseStatus::Unknown;
    }
    if (IEquals(value, "locked")) {
        return Models::LeaseStatus::Locked;
    }
    if (IEquals(value, "unlocked")) {
        return Models::LeaseStatus::Unlocked;
    }
    return Models::LeaseStatus::Unknown;
}

Models::LeaseState ParseLeaseState(std::string_view value) {
    if (value.empty()) {
        return Models::LeaseState::Unknown;
    }
    if (IEquals(value, "leased")) {
        return Models::LeaseState::Leased;
    }
    if (IEquals(value, "expired")) {
        return Models::LeaseState::Expired;
    }
    if (IEquals(value, "breaking")) {
        return Models::LeaseState::Breaking;
    }
    if (IEquals(value, "broken")) {
        return Models::LeaseState::Broken;
    }
    if (IEquals(value, "available")) {
        return Models::LeaseState::Available;
    }
    return Models::LeaseState::Unknown;
}

Models::LeaseDurationType ParseLeaseDurationType(std::string_view value) {
    if (value.empty()) {
        return Models::LeaseDurationType::Unknown;
    }
    if (IEquals(value, "fixed")) {
        return Models::LeaseDurationType::Fixed;
    }
    if (IEquals(value, "infinite")) {
        return Models::LeaseDurationType::Infinite;
    }
    return Models::LeaseDurationType::Unknown;
}

bool ParseBoolHeader(std::string_view value) noexcept { return IEquals(value, "true"); }

std::optional<std::chrono::system_clock::time_point> ParseHttpDateHeader(std::string_view value) noexcept {
    value = TrimWhitespace(value);
    if (value.empty()) {
        return std::nullopt;
    }

    if (const auto rfc1123 = ParseRfc1123HttpDate(value); rfc1123.has_value()) {
        return rfc1123;
    }
    if (const auto rfc850 = ParseRfc850HttpDate(value); rfc850.has_value()) {
        return rfc850;
    }
    return ParseAsctimeHttpDate(value);
}

// "1994-11-06T08:49:37Z" with an optional fractional-second part before the 'Z'.
namespace Iso8601Layout {
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

std::optional<std::chrono::system_clock::time_point> ParseIso8601Utc(std::string_view value) noexcept {
    using namespace Iso8601Layout;

    if (value.size() < MinLength || value.back() != 'Z' || value.at(YearSeparator.Offset) != '-' ||
        value.at(MonthSeparator.Offset) != '-' || value.at(DaySeparator.Offset) != 'T' ||
        value.at(HourSeparator.Offset) != ':' || value.at(MinuteSeparator.Offset) != ':') {
        return std::nullopt;
    }
    const auto year = ParseInt(value.substr(Year.Offset, Year.Length));
    const auto month = ParseUnsignedComponent(value.substr(Month.Offset, Month.Length));
    const auto day = ParseUnsignedComponent(value.substr(Day.Offset, Day.Length));
    const auto hour = ParseUnsignedComponent(value.substr(Hour.Offset, Hour.Length));
    const auto minute = ParseUnsignedComponent(value.substr(Minute.Offset, Minute.Length));
    const auto second = ParseUnsignedComponent(value.substr(Second.Offset, Second.Length));
    if (!year || !month || !day || !hour || !minute || !second) {
        return std::nullopt;
    }
    auto result = BuildUtcTimePoint(*year, *month, *day, *hour, *minute, *second);
    // Digits beyond the seventh are dropped (truncated, not rounded): the service emits 100 ns ticks, so nothing real
    // is lost.
    std::string_view fraction = value.substr(FractionOffset, value.size() - MinLength);
    if (!result || (!fraction.empty() && (fraction.front() != '.' || fraction.size() < MinFractionLength))) {
        return std::nullopt;
    }
    if (!fraction.empty()) {
        fraction.remove_prefix(1U);
        std::int64_t ticks = 0; // 100 ns units, up to 7 digits
        std::size_t digits = 0;
        for (const char c : fraction) {
            if (c < '0' || c > '9') {
                return std::nullopt;
            }
            if (digits < MaxFractionDigits) {
                ticks = (ticks * DecimalRadix) + (c - '0');
                ++digits;
            }
        }
        for (; digits < MaxFractionDigits; ++digits) {
            ticks *= DecimalRadix;
        }
        *result += std::chrono::duration_cast<std::chrono::system_clock::duration>(
            std::chrono::nanoseconds{ticks * NanosecondsPerTick});
    }
    return result;
}

std::optional<ParsedContentRange> ParseContentRange(std::string_view header) noexcept {
    // Expected forms: "bytes start-end/total" or "bytes */total"
    header = TrimWhitespace(header);
    if (header.empty()) {
        return std::nullopt;
    }
    const std::string_view prefix = "bytes ";
    if (!IStartsWith(header, prefix)) {
        return std::nullopt;
    }
    header.remove_prefix(prefix.size());
    const std::size_t slash = header.find('/');
    if (slash == std::string_view::npos) {
        return std::nullopt;
    }
    const std::string_view rangePart = TrimWhitespace(header.substr(0, slash));
    const std::string_view totalPart = TrimWhitespace(header.substr(slash + 1));

    std::optional<std::uint64_t> total;
    if (!totalPart.empty() && totalPart != "*") {
        std::uint64_t t = 0;
        const auto r = std::from_chars(std::to_address(totalPart.begin()), std::to_address(totalPart.end()), t);
        if (r.ec != std::errc{} || r.ptr != std::to_address(totalPart.end())) {
            return std::nullopt;
        }
        total = t;
    }

    if (rangePart == "*") {
        // "bytes */*" carries no information and is invalid.
        if (!total.has_value()) {
            return std::nullopt;
        }
        return ParsedContentRange{.Start = 0, .End = 0, .Total = total, .Unsatisfied = true};
    }

    const std::size_t dash = rangePart.find('-');
    if (dash == std::string_view::npos) {
        return std::nullopt;
    }

    const std::string_view startPart = TrimWhitespace(rangePart.substr(0, dash));
    const std::string_view endPart = TrimWhitespace(rangePart.substr(dash + 1));
    if (startPart.empty() || endPart.empty()) {
        return std::nullopt;
    }

    std::uint64_t start = 0;
    std::uint64_t end = 0;
    const auto rs = std::from_chars(std::to_address(startPart.begin()), std::to_address(startPart.end()), start);
    const auto re = std::from_chars(std::to_address(endPart.begin()), std::to_address(endPart.end()), end);
    if (rs.ec != std::errc{} || rs.ptr != std::to_address(startPart.end()) || re.ec != std::errc{} ||
        re.ptr != std::to_address(endPart.end())) {
        return std::nullopt;
    }

    // RFC 9110 14.4: invalid if last < first, or if the complete length is not greater than last.
    if (end < start || (total.has_value() && *total <= end)) {
        return std::nullopt;
    }

    return ParsedContentRange{.Start = start, .End = end, .Total = total};
}

namespace {
enum class BlobHeader : std::uint8_t {
    ETag,
    LastModified,
    ContentRange,
    ContentLength,
    ContentType,
    ContentMd5,
    CacheControl,
    BlobType,
    VersionId,
    IsCurrentVersion,
    ContentEncoding,
    ContentLanguage,
    ContentDisposition,
    CreatedOn,
    LeaseStatus,
    LeaseState,
    LeaseDuration,
    ServerEncrypted,
    SequenceNumber,
    Count
};

// Dispatches on the name length first so most headers (and every header we do not care about) cost one or two
// case-insensitive compares instead of one full scan per property.
[[nodiscard]] BlobHeader ClassifyBlobHeader(std::string_view name) noexcept {
    switch (name.size()) {
    case 4:
        if (IEquals(name, ETagHeaderName)) {
            return BlobHeader::ETag;
        }
        break;
    case 11:
        if (IEquals(name, ContentMd5HeaderName)) {
            return BlobHeader::ContentMd5;
        }
        break;
    case 12:
        if (IEquals(name, ContentTypeHeaderName)) {
            return BlobHeader::ContentType;
        }
        break;
    case 13:
        if (IEquals(name, CacheControlHeaderName)) {
            return BlobHeader::CacheControl;
        }
        if (IEquals(name, "Content-Range")) {
            return BlobHeader::ContentRange;
        }
        if (IEquals(name, LastModifiedHeaderName)) {
            return BlobHeader::LastModified;
        }
        break;
    case 14:
        if (IEquals(name, ContentLengthHeaderName)) {
            return BlobHeader::ContentLength;
        }
        if (IEquals(name, XMsBlobTypeHeaderName)) {
            return BlobHeader::BlobType;
        }
        break;
    case 15:
        if (IEquals(name, "x-ms-version-id")) {
            return BlobHeader::VersionId;
        }
        break;
    case 16:
        if (IEquals(name, HeaderPropertyNames.ContentEncoding)) {
            return BlobHeader::ContentEncoding;
        }
        if (IEquals(name, HeaderPropertyNames.ContentLanguage)) {
            return BlobHeader::ContentLanguage;
        }
        if (IEquals(name, HeaderPropertyNames.LeaseState)) {
            return BlobHeader::LeaseState;
        }
        break;
    case 17:
        if (IEquals(name, HeaderPropertyNames.LeaseStatus)) {
            return BlobHeader::LeaseStatus;
        }
        break;
    case 18:
        if (IEquals(name, HeaderPropertyNames.CreatedOn)) {
            return BlobHeader::CreatedOn;
        }
        break;
    case 19:
        if (IEquals(name, HeaderPropertyNames.LeaseDuration)) {
            return BlobHeader::LeaseDuration;
        }
        if (IEquals(name, HeaderPropertyNames.ContentDisposition)) {
            return BlobHeader::ContentDisposition;
        }
        break;
    case 21:
        if (IEquals(name, HeaderPropertyNames.ServerEncrypted)) {
            return BlobHeader::ServerEncrypted;
        }
        break;
    case 23:
        if (IEquals(name, "x-ms-is-current-version")) {
            return BlobHeader::IsCurrentVersion;
        }
        break;
    case 25:
        if (IEquals(name, HeaderPropertyNames.SequenceNumber)) {
            return BlobHeader::SequenceNumber;
        }
        break;
    default:
        break;
    }
    return BlobHeader::Count;
}
} // namespace

Models::BlobProperties ParseBlobProperties(const HttpResponse& response) {
    Models::BlobProperties properties;

    // One pass over the headers; as with FindHeaderValue, the first occurrence of a header wins.
    constexpr std::size_t HeaderCount = static_cast<std::size_t>(BlobHeader::Count);
    std::array<std::string_view, HeaderCount> values{};
    std::bitset<HeaderCount> seen;
    for (const auto& header : response.GetHeaders()) {
        const std::string_view name = header.GetName();
        if (IStartsWith(name, XMsMetaHeaderPrefix)) {
            properties.Metadata.emplace(std::string{name.substr(XMsMetaHeaderPrefix.size())}, header.GetValue());
            continue;
        }
        const BlobHeader slot = ClassifyBlobHeader(name);
        if (slot != BlobHeader::Count && !seen.test(static_cast<std::size_t>(slot))) {
            seen.set(static_cast<std::size_t>(slot));
            values.at(static_cast<std::size_t>(slot)) = header.GetValue();
        }
    }
    const auto value = [&values](BlobHeader slot) { return values.at(static_cast<std::size_t>(slot)); };

    properties.ETag = std::string{value(BlobHeader::ETag)};
    if (const std::string_view lastModified = value(BlobHeader::LastModified); !lastModified.empty()) {
        const auto parsed = ParseHttpDateHeader(lastModified);
        if (!parsed.has_value()) {
            throw std::invalid_argument("Last-Modified header must contain a valid HTTP-date.");
        }
        properties.LastModified = *parsed;
    }

    // Prefer the Content-Range total when present (206 responses). A header that is present but malformed is an
    // error rather than a silent fallback to Content-Length, which would describe only the partial body.
    bool contentLengthPresent = false;
    const std::string_view contentRangeHeader = value(BlobHeader::ContentRange);
    if (!contentRangeHeader.empty()) {
        const auto parsed = ParseContentRange(contentRangeHeader);
        if (!parsed.has_value()) {
            throw std::invalid_argument("Content-Range header is malformed.");
        }
        if (parsed->Total.has_value()) {
            properties.ContentLength = *parsed->Total;
            contentLengthPresent = true;
        }
    }

    if (!contentLengthPresent) {
        if (const std::string_view contentLength = value(BlobHeader::ContentLength); !contentLength.empty()) {
            const auto parsed = ParseUnsigned(contentLength);
            if (!parsed.has_value()) {
                throw std::invalid_argument("Content-Length header must be an unsigned integer.");
            }

            properties.ContentLength = *parsed;
        }
    }
    properties.ContentType = std::string{value(BlobHeader::ContentType)};
    properties.ContentMd5 = std::string{value(BlobHeader::ContentMd5)};
    properties.CacheControl = std::string{value(BlobHeader::CacheControl)};
    properties.Type = ParseBlobTypeValue(value(BlobHeader::BlobType));
    ParseExtendedBlobProperties(properties, HeaderPropertyNames,
                                ExtendedBlobValues{.ContentEncoding = value(BlobHeader::ContentEncoding),
                                                   .ContentLanguage = value(BlobHeader::ContentLanguage),
                                                   .ContentDisposition = value(BlobHeader::ContentDisposition),
                                                   .CreatedOn = value(BlobHeader::CreatedOn),
                                                   .LeaseStatus = value(BlobHeader::LeaseStatus),
                                                   .LeaseState = value(BlobHeader::LeaseState),
                                                   .LeaseDuration = value(BlobHeader::LeaseDuration),
                                                   .ServerEncrypted = value(BlobHeader::ServerEncrypted),
                                                   .SequenceNumber = value(BlobHeader::SequenceNumber)});
    properties.VersionId = std::string{value(BlobHeader::VersionId)};
    properties.IsCurrentVersion = ParseOptionalBool(value(BlobHeader::IsCurrentVersion), "x-ms-is-current-version");
    return properties;
}

Models::DeleteBlobResult ParseDeleteBlobResult(const HttpResponse& response) {
    Models::DeleteBlobResult result;
    result.RequestId = std::string{FindHeaderValue(response, XMsRequestIdHeaderName)};
    const std::string_view permanentDelete = FindHeaderValue(response, XMsDeleteTypePermanentHeaderName);
    if (!permanentDelete.empty()) {
        result.DeleteTypePermanent = ParseBoolHeader(permanentDelete);
    }
    return result;
}

Models::AcquireBlobLeaseResult ParseAcquireBlobLeaseResult(const HttpResponse& response) {
    Models::AcquireBlobLeaseResult result;
    result.LeaseId = std::string{FindHeaderValue(response, XMsLeaseIdHeaderName)};
    ApplyETagAndLastModified(result, response);
    return result;
}

Models::RenewBlobLeaseResult ParseRenewBlobLeaseResult(const HttpResponse& response) {
    auto result = ParseETagAndLastModified<Models::RenewBlobLeaseResult>(response);
    result.LeaseId = std::string{FindHeaderValue(response, XMsLeaseIdHeaderName)};
    return result;
}

Models::ReleaseBlobLeaseResult ParseReleaseBlobLeaseResult(const HttpResponse& response) {
    return ParseETagAndLastModified<Models::ReleaseBlobLeaseResult>(response);
}

Models::GetPageRangesResult ParseGetPageRangesResult(const HttpResponse& response) {
    auto result = ParseETagAndLastModified<Models::GetPageRangesResult>(response);
    result.BlobContentLength =
        ParseOptionalUnsigned(FindHeaderValue(response, XMsBlobContentLengthHeaderName), "x-ms-blob-content-length")
            .value_or(0U);

    const auto tree = TryReadXmlOrThrow(response.GetBody(), "Get page ranges response", "PageList");
    const XmlNode* root = tree ? tree->Root() : nullptr;
    if (root == nullptr) {
        return result;
    }

    result.NextMarker = GetChildTextOrEmpty(*root, "NextMarker");
    for (const XmlNode* range : GetChildrenByLocalName(*root, "PageRange")) {
        const auto start = ParseUnsigned(GetChildTextOrEmpty(*range, "Start"));
        const auto end = ParseUnsigned(GetChildTextOrEmpty(*range, "End"));
        if (!start.has_value() || !end.has_value() || *end < *start) {
            throw std::invalid_argument("PageRange must have unsigned Start and End with End >= Start.");
        }
        result.Ranges.push_back({.Offset = *start, .Length = *end - *start + 1U});
    }
    return result;
}

namespace {
// The Name element's Encoded="true" attribute marks a percent-encoded name (used for names with characters invalid in
// XML).
[[nodiscard]] std::string GetBlobItemName(const XmlNode& item) {
    const XmlNode* name = FindChildByLocalName(item, "Name");
    if (name == nullptr) {
        return {};
    }
    std::string text = GetNodeText(*name);
    const auto encoded = GetAttribute(*name, "Encoded");
    if (!encoded.has_value() || !IEquals(*encoded, "true")) {
        return text;
    }
    const auto decoded = boost::urls::make_pct_string_view(text);
    if (!decoded) {
        throw std::invalid_argument("Encoded blob name is not valid percent-encoding.");
    }
    return decoded->decode();
}

} // namespace

Models::ListBlobsResult ParseListBlobsResultXml(std::string_view xml) {
    Models::ListBlobsResult result;
    const auto tree = TryReadXmlOrThrow(xml, "List blobs response", "EnumerationResults");
    const XmlNode* root = tree ? tree->Root() : nullptr;
    if (root == nullptr) {
        return result;
    }

    result.Prefix = GetChildTextOrEmpty(*root, "Prefix");
    result.Marker = GetChildTextOrEmpty(*root, "Marker");
    result.NextMarker = GetChildTextOrEmpty(*root, "NextMarker");

    if (const XmlNode* blobs = FindChildByLocalName(*root, "Blobs"); blobs != nullptr) {
        for (const XmlNode* block : GetChildrenByLocalName(*blobs, "Blob")) {
            Models::BlobItem item;
            item.Name = GetBlobItemName(*block);
            if (const XmlNode* properties = FindChildByLocalName(*block, "Properties"); properties != nullptr) {
                item.Properties = ParseBlobPropertiesXml(*properties);
            }
            // VersionId and IsCurrentVersion are siblings of <Properties>.
            item.Properties.VersionId = GetChildTextOrEmpty(*block, "VersionId");
            item.Properties.IsCurrentVersion =
                ParseOptionalBool(GetChildTextOrEmpty(*block, "IsCurrentVersion"), "IsCurrentVersion");
            // The service returns <Metadata> as a sibling of <Properties>.
            if (const XmlNode* metadata = FindChildByLocalName(*block, "Metadata"); metadata != nullptr) {
                item.Properties.Metadata = ParseMetadataXml(*metadata);
            }
            result.Blobs.push_back(std::move(item));
        }
    }

    return result;
}
} // namespace AVEVA::AzureClient::Private
