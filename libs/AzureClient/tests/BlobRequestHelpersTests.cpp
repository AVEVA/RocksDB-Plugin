// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobServiceClient.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "ValueOrFail.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <gtest/gtest.h>

#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <limits>
#include <locale>
#include <optional>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace {
using AVEVA::HttpHeader;
using AVEVA::HttpMethod;
using AVEVA::HttpRequest;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobClientOptions;
using AVEVA::AzureClient::BlobContainerClientOptions;
using AVEVA::AzureClient::BlobServiceClientOptions;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::CredentialPresence;
using AVEVA::AzureClient::StringMatch;
using AVEVA::AzureClient::Models::BlobType;
using AVEVA::AzureClient::Private::AuthorizeRequest;
using AVEVA::AzureClient::Private::BuildBlobUrl;
using AVEVA::AzureClient::Private::BuildContainerUrl;
using AVEVA::AzureClient::Private::BuildDateHeaderValue;
using AVEVA::AzureClient::Private::BuildQueryString;
using AVEVA::AzureClient::Private::BuildServiceUrl;
using AVEVA::AzureClient::Private::DetermineBlobStorageFailure;
using AVEVA::AzureClient::Private::FindHeaderValue;
using AVEVA::AzureClient::Private::IEquals;
using AVEVA::AzureClient::Private::IStartsWith;
using AVEVA::AzureClient::Private::ParseBlobProperties;
using AVEVA::AzureClient::Private::ParseHttpDateHeader;
using AVEVA::AzureClient::Private::ParseListBlobsResultXml;
using AVEVA::AzureClient::Private::TrimLeadingQuestionMark;
using AVEVA::AzureClient::Private::TrimTrailingSlashes;
using AVEVA::AzureClient::Private::UrlEncode;

class ScopedGlobalLocale {
  public:
    explicit ScopedGlobalLocale(const std::locale& replacement) : m_original(std::locale()) {
        std::locale::global(replacement);
    }

    ~ScopedGlobalLocale() { std::locale::global(m_original); }

    ScopedGlobalLocale(const ScopedGlobalLocale&) = delete;
    ScopedGlobalLocale& operator=(const ScopedGlobalLocale&) = delete;
    ScopedGlobalLocale(ScopedGlobalLocale&&) = delete;
    ScopedGlobalLocale& operator=(ScopedGlobalLocale&&) = delete;

  private:
    std::locale m_original;
};

// Digit grouping and a comma decimal point, as in many European locales.
class GroupingNumpunct final : public std::numpunct<char> {
  protected:
    char do_decimal_point() const override { return ','; }

    char do_thousands_sep() const override { return '.'; }

    std::string do_grouping() const override { return "\1"; }
};

// Built from a custom facet so the test runs identically on every machine instead of
// depending on an installed host locale.
[[nodiscard]] std::optional<std::locale> TryCreateNonEnglishLocale() {
    return std::locale{std::locale::classic(), new GroupingNumpunct};
}

// Wraps every other calendar/time component so no two adjacent parameters of
// MakeUtcTimePoint share a plain `int` type (see bugprone-easily-swappable-parameters).
// Implicitly constructible from `int`, so call sites are unaffected.
struct OddTimeComponent {
    int Value;

    constexpr OddTimeComponent(int value) noexcept : Value(value) {}
};

// std::tm counts years from 1900.
constexpr int TmEpochYear = 1900;
constexpr int MaxByteValue = 255;
// Fuzz shape: iteration counts, maximum generated lengths and fixed seeds that keep the tests deterministic.
constexpr int FuzzIterations = 256;
constexpr int HeaderFuzzIterations = 128;
constexpr int MaxFuzzHeaders = 8;
constexpr int MaxFuzzHeaderLength = 16;
constexpr int MaxFuzzValueLength = 64;
constexpr unsigned int HttpDateFuzzSeed = 12345U;
constexpr unsigned int UrlEncodeFuzzSeed = 54321U;
constexpr unsigned int HeaderFuzzSeed = 9999U;
// Concurrent signing: threads and signatures per thread.
constexpr std::size_t SigningThreads = 8;
constexpr int SignaturesPerThread = 200;
[[nodiscard]] std::chrono::system_clock::time_point MakeUtcTimePoint(int year, OddTimeComponent month, int day,
                                                                     OddTimeComponent hour, int minute,
                                                                     OddTimeComponent second) {
    std::tm utcTime{};
    utcTime.tm_year = year - TmEpochYear;
    utcTime.tm_mon = month.Value - 1;
    utcTime.tm_mday = day;
    utcTime.tm_hour = hour.Value;
    utcTime.tm_min = minute;
    utcTime.tm_sec = second.Value;

#ifdef _WIN32
    return std::chrono::system_clock::from_time_t(_mkgmtime(&utcTime));
#else
    return std::chrono::system_clock::from_time_t(timegm(&utcTime));
#endif
}

[[nodiscard]] std::string RandomBytes(std::mt19937& generator, std::size_t length) {
    std::uniform_int_distribution<int> byteDistribution(0, MaxByteValue);
    std::string value(length, '\0');
    for (char& ch : value) {
        ch = static_cast<char>(byteDistribution(generator));
    }
    return value;
}

[[nodiscard]] std::string FindRequestHeaderValue(const HttpRequest& request, std::string_view name) {
    for (const auto& header : request.GetHeaders()) {
        if (IEquals(header.GetName(), name) == StringMatch::Match) {
            return header.GetValue();
        }
    }

    return {};
}

[[nodiscard]] bool IsWellFormedPercentEncodedText(std::string_view encoded) {
    for (std::size_t index = 0; index < encoded.size(); ++index) {
        const auto ch = static_cast<unsigned char>(encoded.at(index));
        if (ch == '%') {
            if (encoded.size() < index + 3U) {
                return false;
            }
            if (std::isxdigit(static_cast<unsigned char>(encoded.at(index + 1U))) == 0) {
                return false;
            }
            if (std::isxdigit(static_cast<unsigned char>(encoded.at(index + 2U))) == 0) {
                return false;
            }
            index += 2U;
            continue;
        }

        if (std::isalnum(ch) == 0 && ch != '-' && ch != '_' && ch != '.' && ch != '~' && ch != '/') {
            return false;
        }
    }
    return true;
}

void RunUrlEncodeFuzzIterations(std::mt19937& generator, std::uniform_int_distribution<int>& lengthDistribution) {
    for (int iteration = 0; iteration < FuzzIterations; ++iteration) {
        const std::string value = RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)));
        EXPECT_TRUE(IsWellFormedPercentEncodedText(UrlEncode(value, "/")));
    }
}

[[nodiscard]] std::vector<HttpHeader> MakeRandomHeaders(std::mt19937& generator) {
    std::uniform_int_distribution<int> headerCountDistribution(0, MaxFuzzHeaders);
    std::uniform_int_distribution<int> lengthDistribution(0, MaxFuzzHeaderLength);
    std::vector<HttpHeader> headers;
    const int headerCount = headerCountDistribution(generator);
    headers.reserve(static_cast<std::size_t>(headerCount));
    for (int headerIndex = 0; headerIndex < headerCount; ++headerIndex) {
        headers.emplace_back(
            AVEVA::HeaderName{RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)))},
            AVEVA::HeaderValue{RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)))});
    }
    return headers;
}

[[nodiscard]] std::string ExpectedHeaderValueForQuery(const std::vector<HttpHeader>& headers, std::string_view query) {
    for (const auto& header : headers) {
        if (IEquals(header.GetName(), query) == StringMatch::Match) {
            return header.GetValue();
        }
    }
    return {};
}

void RunFindHeaderValueFuzzIterations(std::mt19937& generator) {
    std::uniform_int_distribution<int> lengthDistribution(0, MaxFuzzHeaderLength);
    for (int iteration = 0; iteration < HeaderFuzzIterations; ++iteration) {
        const std::vector<HttpHeader> headers = MakeRandomHeaders(generator);
        const std::string query = RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)));
        const HttpResponse response{AVEVA::HttpStatus::Ok, headers, ""};
        const std::string expected = ExpectedHeaderValueForQuery(headers, query);
        static_assert(noexcept(FindHeaderValue(response, query)));
        EXPECT_EQ(FindHeaderValue(response, query), expected);
    }
}

[[nodiscard]] std::vector<int>
CountSharedKeySignerMismatchesAcrossThreads(const AVEVA::AzureClient::Private::SharedKeySigner& signer,
                                            const AVEVA::HttpRequest& request, std::string_view expected) {
    std::vector<std::thread> threads;
    std::vector<int> mismatches(SigningThreads, 0);
    threads.reserve(mismatches.size());
    for (int& mismatchCount : mismatches) {
        threads.emplace_back([&signer, &request, expected, &mismatchCount] {
            for (int i = 0; i < SignaturesPerThread; ++i) {
                if (signer.Authorize(request) != expected) {
                    ++mismatchCount;
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    return mismatches;
}
} // namespace

TEST(BlobRequestHelpersTests, SharedKeyAuthorization_MatchesForOwnedAndViewBodies) {
    using AVEVA::AzureClient::Private::AuthorizeRequest;

    const AVEVA::AzureClient::Private::SharedKeySigner signer{"testaccount", "MTIzNDU2"}; // base64 of "123456"

    AVEVA::HttpRequest reqOwned;
    reqOwned.SetMethod(AVEVA::HttpMethod::Put);
    reqOwned.SetUrl(AVEVA::HttpUrl{"https://example.com/container/blob"});
    reqOwned.AddHeader(
        AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"}, AVEVA::HeaderValue{"Mon, 01 Jan 2020 00:00:00 GMT"}));
    reqOwned.SetBody(AVEVA::HttpBody{"abc"});
    AuthorizeRequest(signer, reqOwned);
    const std::string authOwned = FindRequestHeaderValue(reqOwned, "Authorization");

    AVEVA::HttpRequest reqView;
    reqView.SetMethod(AVEVA::HttpMethod::Put);
    reqView.SetUrl(AVEVA::HttpUrl{"https://example.com/container/blob"});
    reqView.AddHeader(
        AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"}, AVEVA::HeaderValue{"Mon, 01 Jan 2020 00:00:00 GMT"}));
    const auto buffer =
        std::make_shared<const std::vector<std::byte>>(std::vector{std::byte('a'), std::byte('b'), std::byte('c')});
    reqView.SetBody(AVEVA::HttpBody(std::span<const std::byte>(*buffer), buffer));
    AuthorizeRequest(signer, reqView);
    const std::string authView = FindRequestHeaderValue(reqView, "Authorization");

    EXPECT_EQ(authOwned, authView);
}

TEST(BlobRequestHelpersTests, UrlEncode_EncodesReservedCharactersUtf8AndExtraSafeCharacters) {
    EXPECT_EQ(UrlEncode("folder name/file?.txt", {}), "folder%20name%2Ffile%3F.txt");
    EXPECT_EQ(UrlEncode("folder name/file?.txt", "/"), "folder%20name/file%3F.txt");
    EXPECT_EQ(UrlEncode("\xF0\x9F\x98\x80", {}), "%F0%9F%98%80");
    EXPECT_EQ(UrlEncode("a+b=c", "+"), "a+b%3Dc");
}

TEST(BlobRequestHelpersTests, BuildAndParseHttpDateHeader_RoundTripsAndIgnoreGlobalLocale) {
    const auto expectedTime = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);
    const std::string expectedText = "Fri, 26 Jun 2015 18:59:17 GMT";

    const auto nonEnglishLocale = TryCreateNonEnglishLocale();
    ASSERT_TRUE(nonEnglishLocale.has_value());

    ScopedGlobalLocale const guard(*nonEnglishLocale);
    EXPECT_EQ(BuildDateHeaderValue(expectedTime), expectedText);
    EXPECT_EQ(ParseHttpDateHeader(expectedText), std::optional{expectedTime});
}

TEST(BlobRequestHelpersTests, ParseHttpDateHeader_ReturnsNulloptForMalformedInput) {
    EXPECT_EQ(ParseHttpDateHeader(""), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("not a date"), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("Fri, 99 Jun 2015 18:59:17 GMT"), std::nullopt);
}

TEST(BlobRequestHelpersTests, ParseHttpDateHeader_AcceptsAllRfc7231Formats) {
    const auto expectedTime = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);

    EXPECT_EQ(ParseHttpDateHeader("Fri, 26 Jun 2015 18:59:17 GMT"), std::optional{expectedTime});
    EXPECT_EQ(ParseHttpDateHeader("Friday, 26-Jun-15 18:59:17 GMT"), std::optional{expectedTime});
    EXPECT_EQ(ParseHttpDateHeader("Fri Jun 26 18:59:17 2015"), std::optional{expectedTime});
}

TEST(BlobRequestHelpersTests, AsciiHelpers_FoldOnlyAsciiLettersAndPassOtherBytesThrough) {
    static_assert(AVEVA::AzureClient::Private::IEquals("Content-LENGTH", "content-length") == StringMatch::Match);
    static_assert(AVEVA::AzureClient::Private::IEquals("abc", "abcd") == StringMatch::Mismatch);
    static_assert(AVEVA::AzureClient::Private::IStartsWith("X-MS-Meta", "x-ms-") == StringMatch::Match);
    static_assert(AVEVA::AzureClient::Private::IStartsWith("x-m", "x-ms-") == StringMatch::Mismatch);

    // 0xC9 and 0xE9 differ only by bit 0x20 but are not ASCII letters, so they must stay distinct.
    EXPECT_EQ(AVEVA::AzureClient::Private::IEquals("\xC9", "\xE9"), StringMatch::Mismatch);
    EXPECT_EQ(AVEVA::AzureClient::Private::IEquals("@", "`"), StringMatch::Mismatch);
    EXPECT_EQ(AVEVA::AzureClient::Private::IEquals("[", "{"), StringMatch::Mismatch);
    EXPECT_EQ(AVEVA::AzureClient::Private::ToLowerAscii("AbC-\xC9Z[@"), "abc-\xC9z[@");
}

TEST(BlobRequestHelpersTests, StringHelpers_AreCaseInsensitiveAndTrimAsExpected) {
    EXPECT_EQ(IEquals("x-ms-meta-project", "X-Ms-MeTa-PrOjEcT"), StringMatch::Match);
    EXPECT_EQ(IEquals("abc", "abcd"), StringMatch::Mismatch);

    EXPECT_EQ(IStartsWith("X-Ms-Meta-Project", "x-ms-meta-"), StringMatch::Match);
    EXPECT_EQ(IStartsWith("metadata", "x-ms-meta-"), StringMatch::Mismatch);

    EXPECT_EQ(TrimTrailingSlashes("https://example.com///"), "https://example.com");
    EXPECT_EQ(TrimTrailingSlashes("plain"), "plain");
    EXPECT_EQ(TrimTrailingSlashes(""), "");
    EXPECT_EQ(TrimTrailingSlashes("///"), "");
    EXPECT_EQ(TrimTrailingSlashes("/a/b/"), "/a/b");

    EXPECT_EQ(TrimLeadingQuestionMark("?sv=1&sig=2"), "sv=1&sig=2");
    EXPECT_EQ(TrimLeadingQuestionMark("sv=1&sig=2"), "sv=1&sig=2");

    const auto nonEnglishLocale = TryCreateNonEnglishLocale();
    if (nonEnglishLocale.has_value()) {
        ScopedGlobalLocale const guard(*nonEnglishLocale);
        EXPECT_EQ(IEquals("CONTENT-TYPE", "content-type"), StringMatch::Match);
        EXPECT_EQ(IStartsWith("X-Ms-Meta-Owner", "x-ms-meta-"), StringMatch::Match);
    }
}

TEST(BlobRequestHelpersTests, ParseBlobProperties_ExtractsMetadataAndHandlesUnknownValues) {
    const auto expectedTime = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);
    const HttpResponse response{
        AVEVA::HttpStatus::Ok,
        {
            AVEVA::HttpHeader(AVEVA::HeaderName{"ETag"}, AVEVA::HeaderValue{"\"etag\""}),
            AVEVA::HttpHeader(AVEVA::HeaderName{"Last-Modified"}, AVEVA::HeaderValue{"Fri, 26 Jun 2015 18:59:17 GMT"}),
            AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Type"}, AVEVA::HeaderValue{"application/octet-stream"}),
            AVEVA::HttpHeader(AVEVA::HeaderName{"Cache-Control"}, AVEVA::HeaderValue{"max-age=60"}),
            AVEVA::HttpHeader(AVEVA::HeaderName{"Content-MD5"}, AVEVA::HeaderValue{"abcd"}),
            AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-blob-type"}, AVEVA::HeaderValue{"NotARealBlobType"}),
            AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-Project"}, AVEVA::HeaderValue{"aveva"}),
            AVEVA::HttpHeader(AVEVA::HeaderName{"X-Ms-MeTa-OWNER"}, AVEVA::HeaderValue{"storage"}),
        },
        ""};

    const auto properties = ParseBlobProperties(response);

    EXPECT_EQ(properties.ETag, "\"etag\"");
    EXPECT_EQ(properties.LastModified, expectedTime);
    EXPECT_EQ(properties.ContentLength, 0U);
    EXPECT_EQ(properties.ContentType, "application/octet-stream");
    EXPECT_EQ(properties.ContentMd5, "abcd");
    EXPECT_EQ(properties.CacheControl, "max-age=60");
    EXPECT_EQ(properties.Type, BlobType::Unknown);
    EXPECT_EQ(properties.Metadata.at("project"), "aveva");
    EXPECT_EQ(properties.Metadata.at("OWNER"), "storage");
}

TEST(BlobRequestHelpersTests, ParseBlobProperties_ZeroContentLengthIsPresentAndNotTreatedAsMissing) {
    const HttpResponse zero{
        AVEVA::HttpStatus::Ok, {AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Length"}, AVEVA::HeaderValue{"0"})}, ""};
    EXPECT_EQ(ParseBlobProperties(zero).ContentLength, 0U);

    const HttpResponse ranged{
        AVEVA::HttpStatus::PartialContent,
        {AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Range"}, AVEVA::HeaderValue{"bytes 0-0/10"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Length"}, AVEVA::HeaderValue{"1"})},
        ""};
    EXPECT_EQ(ParseBlobProperties(ranged).ContentLength, 10U);

    const HttpResponse emptyTotal{
        AVEVA::HttpStatus::PartialContent,
        {AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Range"}, AVEVA::HeaderValue{"bytes 0-0/0"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Length"}, AVEVA::HeaderValue{"1"})},
        ""};
    EXPECT_THROW(static_cast<void>(ParseBlobProperties(emptyTotal)), std::invalid_argument);
}

TEST(BlobRequestHelpersTests, BoostUrlHelpers_PreserveExistingUrlShapes) {
    EXPECT_EQ(BuildQueryString({{"comp", "list"}, {"prefix", "folder name"}}), "comp=list&prefix=folder%20name");

    BlobClientOptions blobOptions;
    blobOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    blobOptions.ContainerName = "container";
    blobOptions.BlobName = "folder name/file?.txt";
    blobOptions.SasToken = "sv=1&sig=a%2Bb";
    EXPECT_EQ(BuildBlobUrl(AVEVA::AzureClient::Private::MakeBlobTarget(blobOptions, CredentialPresence::Absent),
                           "comp=blocklist"),
              "https://account.blob.core.windows.net/container/folder%20name/"
              "file%3F.txt?comp=blocklist&sv=1&sig=a%2Bb");

    BlobContainerClientOptions containerOptions;
    containerOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    containerOptions.ContainerName = "container";
    containerOptions.SasToken = "sv=1&sig=a%2Bb";
    EXPECT_EQ(
        BuildContainerUrl(
            AVEVA::AzureClient::Private::MakeContainerTarget(containerOptions, CredentialPresence::Absent),
            "comp=list&include=metadata"),
        "https://account.blob.core.windows.net/container?restype=container&comp=list&include=metadata&sv=1&sig=a%2Bb");
}

TEST(BlobRequestHelpersTests, ParseHttpDateHeader_FuzzDoesNotThrow) {
    std::seed_seq seed{HttpDateFuzzSeed}; // A fixed seed keeps this test deterministic.
    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> lengthDistribution(0, MaxFuzzValueLength);

    for (int iteration = 0; iteration < FuzzIterations; ++iteration) {
        const std::string value = RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)));
        EXPECT_NO_THROW({
            const auto parsed = ParseHttpDateHeader(value);
            static_cast<void>(parsed);
        });
    }
}

TEST(BlobRequestHelpersTests, UrlEncode_FuzzProducesWellFormedPercentEscapes) {
    std::seed_seq seed{UrlEncodeFuzzSeed}; // A fixed seed keeps this test deterministic.
    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> lengthDistribution(0, MaxFuzzValueLength);

    RunUrlEncodeFuzzIterations(generator, lengthDistribution);
}

TEST(BlobRequestHelpersTests, FindHeaderValue_FuzzDoesNotThrowAndReturnsMatchingValue) {
    std::seed_seq seed{HeaderFuzzSeed}; // A fixed seed keeps this test deterministic.
    std::mt19937 generator(seed);
    RunFindHeaderValueFuzzIterations(generator);
}

TEST(BlobRequestHelpersTests, SharedKeyAuthorization_KnownAnswerVectorsMatchIndependentSignatures) {
    const AVEVA::AzureClient::Private::SharedKeySigner sharedKey{"storageaccount", "MDEyMzQ1Njc4OWFiY2RlZg=="};

    struct KnownAnswerCase {
        const char* Name;
        HttpMethod Method;
        const char* Url;
        std::vector<HttpHeader> Headers;
        const char* Body;
        const char* ExpectedAuthorization;
    };

    const std::vector<KnownAnswerCase> cases{
        {
            .Name = "GET",
            .Method = HttpMethod::Get,
            .Url = "https://storageaccount.blob.core.windows.net/photos/"
                   "cat%20photo.jpg?zeta=last&comp=metadata&Alpha=first&timeout=30",
            .Headers =
                {
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"},
                                      AVEVA::HeaderValue{"Sun, 06 Nov 1994 08:49:37 GMT"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-version"}, AVEVA::HeaderValue{"2023-11-03"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"X-Ms-Meta-Owner"}, AVEVA::HeaderValue{"  Alice  "}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"If-None-Match"}, AVEVA::HeaderValue{"\"etag-123\""}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"Range"}, AVEVA::HeaderValue{"bytes=1-3"}),
                },
            .Body = "",
            .ExpectedAuthorization = "SharedKey storageaccount:RRz8HIx1QCyRXmSRoclBHAGSviMjbDbPzYyxnnl72VA=",
        },
        {
            .Name = "PUT",
            .Method = HttpMethod::Put,
            .Url = "https://storageaccount.blob.core.windows.net/images/photo.png?timeout=30&comp=blocklist",
            .Headers =
                {
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"},
                                      AVEVA::HeaderValue{"Sun, 06 Nov 1994 08:49:37 GMT"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-version"}, AVEVA::HeaderValue{"2023-11-03"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"X-Ms-Blob-Type"}, AVEVA::HeaderValue{"BlockBlob"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-Project"}, AVEVA::HeaderValue{" AzureClient "}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"Content-MD5"}, AVEVA::HeaderValue{"sQqNsWTgdUEFt6mb5y4/5Q=="}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Type"},
                                      AVEVA::HeaderValue{"application/xml; charset=utf-8"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"If-Match"}, AVEVA::HeaderValue{"\"match-me\""}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"If-Unmodified-Since"},
                                      AVEVA::HeaderValue{"Sat, 05 Nov 1994 08:49:37 GMT"}),
                },
            .Body = R"(<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>YmxvY2swMQ==</Latest></BlockList>)",
            .ExpectedAuthorization = "SharedKey storageaccount:DOD+foEtDUy9DET5eSjm+6OSomlOr0Gnxufdaj5g4I8=",
        },
        {
            .Name = "HEAD",
            .Method = HttpMethod::Head,
            .Url = "https://storageaccount.blob.core.windows.net/logs/app.log?timeout=10&comp=metadata&Alpha=first",
            .Headers =
                {
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"},
                                      AVEVA::HeaderValue{"Sun, 06 Nov 1994 08:49:37 GMT"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"X-Ms-Version"}, AVEVA::HeaderValue{"2023-11-03"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"If-Modified-Since"},
                                      AVEVA::HeaderValue{"Sat, 05 Nov 1994 08:49:37 GMT"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-Zebra"}, AVEVA::HeaderValue{" z "}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-alpha"}, AVEVA::HeaderValue{" a "}),
                },
            .Body = "",
            .ExpectedAuthorization = "SharedKey storageaccount:Pbn+Boe3r45lo3l+F0ymWu5xY+PPFCynbQ8nfUJi2AU=",
        },
        {
            .Name = "DELETE",
            .Method = HttpMethod::Delete,
            .Url = "https://storageaccount.blob.core.windows.net/photos/"
                   "cat%20photo.jpg?timeout=45&snapshot=2024-01-02T03%3A04%3A05.0000000Z",
            .Headers =
                {
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"},
                                      AVEVA::HeaderValue{"Sun, 06 Nov 1994 08:49:37 GMT"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-version"}, AVEVA::HeaderValue{"2023-11-03"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-delete-snapshots"}, AVEVA::HeaderValue{"include"}),
                    AVEVA::HttpHeader(AVEVA::HeaderName{"If-Modified-Since"},
                                      AVEVA::HeaderValue{"Sat, 05 Nov 1994 08:49:37 GMT"}),
                },
            .Body = "",
            .ExpectedAuthorization = "SharedKey storageaccount:/jcIn141mdTeJVNSo/mDdDy0DesOllUXRHbgKkBWAZA=",
        },
    };

    for (const auto& testCase : cases) {
        HttpRequest request;
        request.SetMethod(testCase.Method);
        request.SetUrl(AVEVA::HttpUrl{testCase.Url});
        request.SetBody(AVEVA::HttpBody{testCase.Body});
        for (const auto& header : testCase.Headers) {
            request.AddHeader(header);
        }

        AuthorizeRequest(sharedKey, request);
        EXPECT_EQ(FindRequestHeaderValue(request, "Authorization"), testCase.ExpectedAuthorization) << testCase.Name;
    }
}

TEST(BlobRequestHelpersTests, XmlParsers_HandleNestedSameNameElementsAndNumericEntities) {
    const std::string xml = R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults>
  <Marker>&#38;&#x26;</Marker>
  <Blobs>
    <Blob>
      <Name>outer<Name>inner</Name></Name>
      <Properties>
        <Metadata>
          <Project>alpha&#38;&#x26;beta</Project>
        </Metadata>
      </Properties>
    </Blob>
  </Blobs>
</EnumerationResults>)";

    const auto result = ParseListBlobsResultXml(xml);
    ASSERT_EQ(result.Blobs.size(), 1U);
    EXPECT_EQ(result.Marker, "&&");
    EXPECT_EQ(result.Blobs.at(0).Name, "outer");
    EXPECT_EQ(result.Blobs.at(0).Properties.Metadata.at("project"), "alpha&&beta");
}

TEST(BlobRequestHelpersTests, UrlBuilders_HandlePortsUnicodeLiteralPercentsAndDuplicateQueryKeys) {
    BlobClientOptions blobOptions;
    blobOptions.ServiceEndpoint = "HTTPS://StorageAccount.blob.core.windows.net:8443";
    blobOptions.ContainerName = "container";
    blobOptions.BlobName = "emoji-\xF0\x9F\x98\x80-%2F.txt";
    blobOptions.SasToken = "sv=1&sig=a%2Bb";

    const std::string blobUrl = BuildBlobUrl(
        AVEVA::AzureClient::Private::MakeBlobTarget(blobOptions, CredentialPresence::Absent), "comp=metadata");
    EXPECT_EQ(IStartsWith(blobUrl, "https://"), StringMatch::Match);
    EXPECT_NE(blobUrl.find(":8443/container/emoji-%F0%9F%98%80-%252F.txt"), std::string::npos);
    EXPECT_NE(blobUrl.find("?comp=metadata&sv=1&sig=a%2Bb"), std::string::npos);

    EXPECT_EQ(
        BuildQueryString({{"comp", "list"}, {"include", "metadata"}, {"include", "snapshots"}, {"marker", "A+B&C=%"}}),
        "comp=list&include=metadata&include=snapshots&marker=A%2BB%26C%3D%25");

    BlobServiceClientOptions serviceOptions;
    serviceOptions.ServiceEndpoint = "https://account.blob.core.windows.net:9443";
    serviceOptions.SasToken = "sv=1&sig=s%2Bv";
    EXPECT_EQ(
        BuildServiceUrl(*AVEVA::AzureClient::Private::MakeConnectionState(serviceOptions, CredentialPresence::Absent),
                        BuildQueryString({{"comp", "list"}, {"marker", "na\xC3\xAFve"}})),
        "https://account.blob.core.windows.net:9443?comp=list&marker=na%C3%AFve&sv=1&sig=s%2Bv");

    BlobContainerClientOptions containerOptions;
    containerOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    containerOptions.ContainerName = "images";
    containerOptions.SasToken = "sv=1&sig=abc%2B123%26x%3D1";
    EXPECT_EQ(BuildContainerUrl(
                  AVEVA::AzureClient::Private::MakeContainerTarget(containerOptions, CredentialPresence::Absent),
                  BuildQueryString({{"comp", "list"}, {"marker", "next+page"}, {"include", "metadata"}})),
              "https://account.blob.core.windows.net/"
              "images?restype=container&comp=list&marker=next%2Bpage&include=metadata&sv=1&sig=abc%2B123%26x%3D1");
}

TEST(BlobRequestHelpersTests, DateHelpers_HandleLeapDaysPreEpochWhitespaceCaseAndBoundaries) {
    EXPECT_EQ(ParseHttpDateHeader("Sat, 29 Feb 2020 00:00:00 GMT"),
              std::optional{MakeUtcTimePoint(2020, 2, 29, 0, 0, 0)});
    EXPECT_EQ(ParseHttpDateHeader("Sun, 31 Dec 2023 23:59:59 GMT"),
              std::optional{MakeUtcTimePoint(2023, 12, 31, 23, 59, 59)});
    EXPECT_EQ(ParseHttpDateHeader("Mon, 01 Jan 2024 00:00:00 GMT"),
              std::optional{MakeUtcTimePoint(2024, 1, 1, 0, 0, 0)});

    const auto preEpoch = MakeUtcTimePoint(1969, 12, 31, 23, 59, 59);
    EXPECT_EQ(ParseHttpDateHeader("Wed, 31 Dec 1969 23:59:59 GMT"), std::optional{preEpoch});
    EXPECT_EQ(BuildDateHeaderValue(preEpoch), "Wed, 31 Dec 1969 23:59:59 GMT");

    const auto expected = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);
    EXPECT_EQ(ParseHttpDateHeader("  fri, 26 jun 2015 18:59:17 gmt\t"), std::optional{expected});
    EXPECT_EQ(ParseHttpDateHeader(" friday, 26-jun-15 18:59:17 gmt "), std::optional{expected});
    EXPECT_EQ(ParseHttpDateHeader("\tfri jun 26 18:59:17 2015 "), std::optional{expected});
}

TEST(BlobRequestHelpersTests, DateHelpers_RejectInvalidWeekdayAndMonthNames) {
    EXPECT_EQ(ParseHttpDateHeader("Fry, 26 Jun 2015 18:59:17 GMT"), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("Fri, 26 Jnn 2015 18:59:17 GMT"), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("Funday, 26-Jun-15 18:59:17 GMT"), std::nullopt);
}

TEST(BlobRequestHelpersTests, BuildRangeHeaderValue_RejectsZeroLengthAndOverflow) {
    EXPECT_THROW(static_cast<void>(AVEVA::AzureClient::Private::BuildRangeHeaderValue(
                     AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 0U})),
                 std::invalid_argument);
    EXPECT_THROW(
        static_cast<void>(AVEVA::AzureClient::Private::BuildRangeHeaderValue(AVEVA::AzureClient::Models::BlobByteRange{
            .Offset = std::numeric_limits<std::uint64_t>::max() - 3U, .Length = 8U})),
        std::invalid_argument);
}

namespace {
[[nodiscard]] AVEVA::HttpRequest MakeGoldenGetRequest() {
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://storageaccount.blob.core.windows.net/photos/"
                                  "cat%20photo.jpg?zeta=last&comp=metadata&Alpha=first&timeout=30"});
    for (const auto& [name, value] :
         std::vector<std::pair<std::string, std::string>>{{"x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT"},
                                                          {"x-ms-version", "2023-11-03"},
                                                          {"X-Ms-Meta-Owner", "  Alice  "},
                                                          {"If-None-Match", "\"etag-123\""},
                                                          {"Range", "bytes=1-3"}}) {
        request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{name}, AVEVA::HeaderValue{value}));
    }
    return request;
}
} // namespace

// T12: the per-connection signer is keyed once and reused; it must give the golden signature on
// every call, including concurrent calls from several threads.
TEST(BlobRequestHelpersTests, SharedKeySigner_IsReusableAndThreadSafe) {
    using AVEVA::AzureClient::Private::SharedKeySigner;
    constexpr std::string_view Expected = "SharedKey storageaccount:RRz8HIx1QCyRXmSRoclBHAGSviMjbDbPzYyxnnl72VA=";

    const SharedKeySigner signer{"storageaccount", "MDEyMzQ1Njc4OWFiY2RlZg=="};
    const AVEVA::HttpRequest request = MakeGoldenGetRequest();
    EXPECT_EQ(signer.AccountName(), "storageaccount");
    EXPECT_EQ(signer.Authorize(request), Expected);
    EXPECT_EQ(signer.Authorize(request), Expected);

    const std::vector<int> mismatches = CountSharedKeySignerMismatchesAcrossThreads(signer, request, Expected);
    for (std::size_t t = 0; t < mismatches.size(); ++t) {
        EXPECT_EQ(mismatches.at(t), 0) << "thread " << t;
    }
}

TEST(BlobRequestHelpersTests, SharedKeySigner_RejectsMalformedKey) {
    using AVEVA::AzureClient::Private::SharedKeySigner;
    EXPECT_THROW((SharedKeySigner{"account", "not base64!"}), std::invalid_argument);
}

// T14: options are normalised once into an immutable ConnectionState shared by child clients.
TEST(BlobRequestHelpersTests, MakeConnectionState_NormalisesOptionsOnce) {
    using AVEVA::AzureClient::BlobServiceClientOptions;
    using AVEVA::AzureClient::Private::MakeBlobTarget;
    using AVEVA::AzureClient::Private::MakeConnectionState;

    BlobServiceClientOptions sharedKeyOptions;
    sharedKeyOptions.ServiceEndpoint = "http://127.0.0.1:10000/devstoreaccount1/";
    sharedKeyOptions.SharedKey = {.AccountName = "devstoreaccount1", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    sharedKeyOptions.InsecureHttp = AVEVA::AzureClient::InsecureHttpPolicy::Allow;
    const auto sharedKey = MakeConnectionState(std::move(sharedKeyOptions), CredentialPresence::Absent);
    EXPECT_EQ(sharedKey->Signer->AccountName(), "devstoreaccount1");
    EXPECT_EQ(sharedKey->BasePrefix, "http://127.0.0.1:10000");
    EXPECT_EQ(sharedKey->BasePath, "/devstoreaccount1");

    const auto blob = MakeBlobTarget(sharedKey, AVEVA::AzureClient::Private::ContainerNameText{"container"},
                                     AVEVA::AzureClient::Private::BlobNameText{"dir/blob.txt"});
    EXPECT_EQ(blob.Connection, sharedKey) << "child targets share, not copy, the connection";
    EXPECT_EQ(BuildBlobUrl(blob), "http://127.0.0.1:10000/devstoreaccount1/container/dir/blob.txt");

    BlobServiceClientOptions unsignedOptions;
    unsignedOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    const auto unsigned_ = MakeConnectionState(std::move(unsignedOptions), CredentialPresence::Absent);
    EXPECT_EQ(unsigned_->Signer, nullptr);
    EXPECT_EQ(unsigned_->BasePath, "");

    BlobServiceClientOptions sasOptions;
    sasOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    sasOptions.SasToken = "?sv=1&sig=x";
    const auto sas = MakeConnectionState(std::move(sasOptions), CredentialPresence::Absent);
    EXPECT_EQ(sas->SasToken, "sv=1&sig=x");

    BlobServiceClientOptions badKeyOptions;
    badKeyOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    badKeyOptions.SharedKey = {.AccountName = "a", .AccountKey = "not base64!"};
    EXPECT_THROW(static_cast<void>(MakeConnectionState(std::move(badKeyOptions), CredentialPresence::Absent)),
                 std::invalid_argument);
    EXPECT_THROW(static_cast<void>(MakeBlobTarget(sharedKey, AVEVA::AzureClient::Private::ContainerNameText{"Bad_Name"},
                                                  AVEVA::AzureClient::Private::BlobNameText{"blob"})),
                 std::invalid_argument);
    EXPECT_THROW(
        static_cast<void>(MakeBlobTarget(sharedKey, AVEVA::AzureClient::Private::ContainerNameText{"container"},
                                         AVEVA::AzureClient::Private::BlobNameText{""})),
        std::invalid_argument);
}

TEST(BlobRequestHelpersTests, BlobTarget_CachesTheEncodedResourceAndAppendsOnlyTheQuery) {
    using AVEVA::AzureClient::BlobServiceClientOptions;
    using AVEVA::AzureClient::Private::MakeBlobTarget;
    using AVEVA::AzureClient::Private::MakeConnectionState;

    BlobServiceClientOptions connectionOptions;
    connectionOptions.ServiceEndpoint = "https://account.blob.core.windows.net/x";
    connectionOptions.SasToken = "sv=1&sig=s";
    const auto connection = MakeConnectionState(std::move(connectionOptions), CredentialPresence::Absent);
    const auto blob = MakeBlobTarget(connection, AVEVA::AzureClient::Private::ContainerNameText{"container"},
                                     AVEVA::AzureClient::Private::BlobNameText{"folder name/file?.txt"});

    EXPECT_EQ(blob.EncodedResource, "https://account.blob.core.windows.net/x/container/folder%20name/file%3F.txt");
    EXPECT_EQ(BuildBlobUrl(blob, "comp=metadata"), blob.EncodedResource + "?comp=metadata&sv=1&sig=s");
    EXPECT_EQ(BuildBlobUrl(blob, ""), blob.EncodedResource + "?sv=1&sig=s");
}

TEST(BlobRequestHelpersTests, ParseListBlobsResultXml_HandlesEntitiesCdataCommentsAndNamespaces) {
    const std::string xml = R"(<?xml version="1.0" encoding="utf-8"?>
<!-- leading comment -->
<x:EnumerationResults xmlns:x="urn:test" ServiceEndpoint="https://a/">
  <x:Prefix>a&amp;b</x:Prefix>
  <x:Blobs>
    <x:Blob>
      <x:Name>part1<!-- split -->part2<![CDATA[<raw>&]]></x:Name>
      <x:Metadata><Key1>v&lt;1&gt;</Key1><x:Key2 attr="ignored">v2</x:Key2></x:Metadata>
    </x:Blob>
    <x:BlobPrefix><x:Name>dir/</x:Name></x:BlobPrefix>
  </x:Blobs>
  <x:NextMarker></x:NextMarker>
</x:EnumerationResults>)";

    const auto result = ParseListBlobsResultXml(xml);
    EXPECT_EQ(result.Prefix, "a&b");
    ASSERT_EQ(result.Blobs.size(), 1U);
    EXPECT_EQ(result.Blobs.at(0).Name, "part1part2<raw>&");
    EXPECT_EQ(result.Blobs.at(0).Properties.Metadata.at("Key1"), "v<1>");
    EXPECT_EQ(result.Blobs.at(0).Properties.Metadata.at("Key2"), "v2");
    EXPECT_TRUE(result.NextMarker.empty());

    EXPECT_THROW(static_cast<void>(ParseListBlobsResultXml("<EnumerationResults><Blobs></EnumerationResults>")),
                 std::invalid_argument);
}

TEST(BlobRequestHelpersTests, SharedKeyStringToSign_SupportsEveryHttpMethod) {
    using AVEVA::HttpMethod;
    for (const auto& [method, verb] :
         std::vector<std::pair<HttpMethod, std::string>>{{HttpMethod::Get, "GET"},
                                                         {HttpMethod::Post, "POST"},
                                                         {HttpMethod::Put, "PUT"},
                                                         {HttpMethod::Patch, "PATCH"},
                                                         {HttpMethod::Delete, "DELETE"},
                                                         {HttpMethod::Head, "HEAD"},
                                                         {HttpMethod::Options, "OPTIONS"},
                                                         {HttpMethod::Trace, "TRACE"},
                                                         {HttpMethod::Connect, "CONNECT"}}) {
        AVEVA::HttpRequest request;
        request.SetMethod(method);
        request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/container/blob"});
        EXPECT_TRUE(
            AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request).starts_with(verb + "\n"))
            << verb;
    }
}

TEST(SharedKeyCanonicalizationTests, ReplacedHeadersAndRepeatedQueryParametersAreCanonicalized) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/container/blob?b=2&a=z&a=y&comp=list"});
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-k"}, AVEVA::HeaderValue{"v1"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-a"}, AVEVA::HeaderValue{"first"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"X-MS-Meta-K"}, AVEVA::HeaderValue{" v2 "}));

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_NE(toSign.find("x-ms-a:first\nx-ms-meta-k:v2\n"), std::string::npos) << toSign;
    EXPECT_NE(toSign.find("/account/container/blob\na:y,z\nb:2\ncomp:list"), std::string::npos) << toSign;
}

TEST(AS_R12_SharedKeyGoldenTests, EncodedPathPutWithEmptyBodyAndReplacedHeader) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Put);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/cont/my%20dir%2Fblob?comp=metadata&Z=2&z=1"});
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-version"}, AVEVA::HeaderValue{"2024-11-04"}));
    request.AddHeader(
        AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"}, AVEVA::HeaderValue{"Sun, 06 Nov 1994 08:49:37 GMT"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-a"}, AVEVA::HeaderValue{"1"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-a"}, AVEVA::HeaderValue{"2"}));

    // The path is signed in its encoded form; an empty body yields an empty Content-Length field.
    EXPECT_EQ(AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request),
              "PUT\n\n\n\n\n\n\n\n\n\n\n\n"
              "x-ms-date:Sun, 06 Nov 1994 08:49:37 GMT\nx-ms-meta-a:2\nx-ms-version:2024-11-04\n"
              "/account/cont/my%20dir%2Fblob\ncomp:metadata\nz:1,2");
}

TEST(AS_R06_SharedKeySignerValidationTests, RejectsXMsHeadersThatCouldBeSignedDifferentlyFromTheirWireForm) {
    for (const auto& [name, value] : std::vector<std::pair<std::string, std::string>>{
             {"x-ms-a", "v\r\nx-ms-b: 1"}, {"x-ms-a\n", "v"}, {"x-ms-a:b", "v"}, {"x-ms-a", std::string("v\0w", 3)}}) {
        AVEVA::HttpRequest request;
        request.SetMethod(HttpMethod::Get);
        request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/container/blob"});
        request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{name}, AVEVA::HeaderValue{value}));
        EXPECT_THROW(static_cast<void>(AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request)),
                     std::invalid_argument);
    }
}

TEST(SharedKeyCanonicalizationTests, UnparseableUrlIsRejectedInsteadOfSigningTheRootPath) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"not a url"});

    EXPECT_THROW((void)AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request),
                 std::invalid_argument);
}

TEST(SharedKeyCanonicalizationTests, InternalWhitespaceRunsInHeaderValuesAreCollapsed) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/container/blob"});
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-meta-k"}, AVEVA::HeaderValue{"  a  \t b   c "}));

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_NE(toSign.find("x-ms-meta-k:a b c\n"), std::string::npos) << toSign;
}

TEST(SharedKeyCanonicalizationTests, StandardHeadersFillTheirSlotsCaseInsensitivelyAndLastOneWins) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Put);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/container/blob"});
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"range"}, AVEVA::HeaderValue{"bytes=0-1"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"IF-MATCH"}, AVEVA::HeaderValue{"\"etag\""}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Type"}, AVEVA::HeaderValue{"text/plain"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"content-type"}, AVEVA::HeaderValue{"ignored"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"If-Unmodified-Since"}, AVEVA::HeaderValue{"yesterday"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-a"}, AVEVA::HeaderValue{"1"}));

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_TRUE(toSign.starts_with("PUT\n\n\n\n\nignored\n\n\n\"etag\"\n\nyesterday\nbytes=0-1\nx-ms-a:1\n"))
        << toSign;
}

TEST(SharedKeyCanonicalizationTests, HeadersAreOrderedWithCultureAwareRules) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/container/blob"});
    for (const char* name : {"x-ms-meta-a1", "x-ms-meta-b-c", "x-ms-meta-a_", "x-ms-meta-bc"}) {
        request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{name}, AVEVA::HeaderValue{"v"}));
    }

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_NE(toSign.find("x-ms-meta-a_:v\nx-ms-meta-a1:v\nx-ms-meta-bc:v\nx-ms-meta-b-c:v\n"), std::string::npos)
        << toSign;
}

TEST(SharedKeyCanonicalizationTests, ResourcePathIsUsedAsSentOnTheWire) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/c/a%2Bb%20c.txt?x=1%202"});

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_NE(toSign.find("/account/c/a%2Bb%20c.txt\nx:1 2"), std::string::npos) << toSign;
}

TEST(SharedKeyCanonicalizationTests, QueryKeysAreLowercasedMergedAndValuesAreDecodedAndSorted) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/c/b?Comp=list&COMP=Meta&a=%26b%3Dc"});

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_TRUE(toSign.ends_with("/account/c/b\na:&b=c\ncomp:Meta,list")) << toSign;
}

TEST(SharedKeyCanonicalizationTests, QueryParametersWithoutValuesOrWithEmptyValuesAreSignedWithEmptyValues) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/c/b?flag&a="});

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_TRUE(toSign.ends_with("/account/c/b\na:\nflag:")) << toSign;
}

TEST(SharedKeyCanonicalizationTests, EmptyPathIsSignedAsTheRootResource) {
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net?comp=list"});

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_TRUE(toSign.ends_with("/account/\ncomp:list")) << toSign;
}

TEST(SharedKeyCanonicalizationTests, MalformedUrlsAreRejected) {
    for (const char* url : {"", "/container/blob", "http://[::1/c", "https://account.blob.core.windows.net/c%zz"}) {
        AVEVA::HttpRequest request;
        request.SetMethod(HttpMethod::Get);
        request.SetUrl(AVEVA::HttpUrl{url});

        EXPECT_THROW((void)AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request),
                     std::invalid_argument)
            << url;
    }
}

TEST(SharedKeyCanonicalizationTests, AuthorizationSupportsRootContainerUnicodeAndAzuritePathStyleEndpoints) {
    HttpRequest pathStyleRequest;
    pathStyleRequest.SetMethod(HttpMethod::Get);
    pathStyleRequest.SetUrl(
        AVEVA::HttpUrl{"http://127.0.0.1:10000/devstoreaccount1/$root/"
                       "caf%C3%A9%25%2Bplus%20space.txt?comp=metadata&include=snapshots&include=metadata"});
    pathStyleRequest.AddHeader(
        AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"}, AVEVA::HeaderValue{"Sun, 06 Nov 1994 08:49:37 GMT"}));
    pathStyleRequest.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-version"}, AVEVA::HeaderValue{"2023-11-03"}));

    EXPECT_EQ(AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("devstoreaccount1", pathStyleRequest),
              "GET\n\n\n\n\n\n\n\n\n\n\n\n"
              "x-ms-date:Sun, 06 Nov 1994 08:49:37 GMT\n"
              "x-ms-version:2023-11-03\n"
              "/devstoreaccount1/devstoreaccount1/$root/caf%C3%A9%25%2Bplus%20space.txt\n"
              "comp:metadata\n"
              "include:metadata,snapshots");

    HttpRequest rootContainerRequest;
    rootContainerRequest.SetMethod(HttpMethod::Get);
    rootContainerRequest.SetUrl(AVEVA::HttpUrl{
        "https://storageaccount.blob.core.windows.net/$root/%E6%96%87%E4%BB%B6%25%2Bname%20space.txt?comp=metadata"});
    rootContainerRequest.AddHeader(
        AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-date"}, AVEVA::HeaderValue{"Sun, 06 Nov 1994 08:49:37 GMT"}));
    rootContainerRequest.AddHeader(
        AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-version"}, AVEVA::HeaderValue{"2023-11-03"}));

    EXPECT_EQ(AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("storageaccount", rootContainerRequest),
              "GET\n\n\n\n\n\n\n\n\n\n\n\n"
              "x-ms-date:Sun, 06 Nov 1994 08:49:37 GMT\n"
              "x-ms-version:2023-11-03\n"
              "/storageaccount/$root/%E6%96%87%E4%BB%B6%25%2Bname%20space.txt\n"
              "comp:metadata");
}
