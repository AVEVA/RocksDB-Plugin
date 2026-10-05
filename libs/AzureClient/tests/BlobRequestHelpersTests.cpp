#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobServiceClient.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "BlobRequestHelpers.hpp"
#include "ValueOrFail.hpp"

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

namespace
{
    using AVEVA::HttpHeader;
    using AVEVA::HttpMethod;
    using AVEVA::HttpRequest;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobContainerClientOptions;
    using AVEVA::AzureClient::BlobServiceClientOptions;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::SharedKeyCredentialOptions;
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
    using AVEVA::AzureClient::Private::ParseGetBlockListResultXml;
    using AVEVA::AzureClient::Private::ParseGetPageRangesResultXml;
    using AVEVA::AzureClient::Private::ParseHttpDateHeader;
    using AVEVA::AzureClient::Private::ParseListBlobContainersResultXml;
    using AVEVA::AzureClient::Private::ParseListBlobsResultXml;
    using AVEVA::AzureClient::Private::TrimLeadingQuestionMark;
    using AVEVA::AzureClient::Private::TrimTrailingSlashes;
    using AVEVA::AzureClient::Private::UrlEncode;

    class ScopedGlobalLocale
    {
      public:
        explicit ScopedGlobalLocale(const std::locale& replacement) : m_original(std::locale())
        {
            std::locale::global(replacement);
        }

        ~ScopedGlobalLocale()
        {
            std::locale::global(m_original);
        }

        ScopedGlobalLocale(const ScopedGlobalLocale&) = delete;
        ScopedGlobalLocale& operator=(const ScopedGlobalLocale&) = delete;
        ScopedGlobalLocale(ScopedGlobalLocale&&) = delete;
        ScopedGlobalLocale& operator=(ScopedGlobalLocale&&) = delete;

      private:
        std::locale m_original;
    };

    // Digit grouping and a comma decimal point, as in many European locales.
    class GroupingNumpunct final : public std::numpunct<char>
    {
      protected:
        char do_decimal_point() const override
        {
            return ',';
        }

        char do_thousands_sep() const override
        {
            return '.';
        }

        std::string do_grouping() const override
        {
            return "\1";
        }
    };

    // Built from a custom facet so the test runs identically on every machine instead of
    // depending on an installed host locale.
    [[nodiscard]] std::optional<std::locale> TryCreateNonEnglishLocale()
    {
        return std::locale{std::locale::classic(), new GroupingNumpunct};
    }

    // Wraps every other calendar/time component so no two adjacent parameters of
    // MakeUtcTimePoint share a plain `int` type (see bugprone-easily-swappable-parameters).
    // Implicitly constructible from `int`, so call sites are unaffected.
    struct OddTimeComponent
    {
        int Value;

        constexpr OddTimeComponent(int value) noexcept : Value(value)
        {
        }
    };

    [[nodiscard]] std::chrono::system_clock::time_point MakeUtcTimePoint(int year,
        OddTimeComponent month,
        int day,
        OddTimeComponent hour,
        int minute,
        OddTimeComponent second)
    {
        std::tm utcTime{};
        utcTime.tm_year = year - 1900;
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

    [[nodiscard]] std::string RandomBytes(std::mt19937& generator, std::size_t length)
    {
        std::uniform_int_distribution<int> byteDistribution(0, 255);
        std::string value(length, '\0');
        for (char& ch : value)
        {
            ch = static_cast<char>(byteDistribution(generator));
        }
        return value;
    }

    [[nodiscard]] std::string FindRequestHeaderValue(const HttpRequest& request, std::string_view name)
    {
        for (const auto& header : request.GetHeaders())
        {
            if (IEquals(header.GetName(), name))
            {
                return header.GetValue();
            }
        }

        return {};
    }

    struct CompletedDownloadBlobResult
    {
        std::optional<std::error_code> ObservedError;
        std::optional<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::DownloadBlobResult>> ObservedResponse;
    };

    [[nodiscard]] CompletedDownloadBlobResult CompleteDownloadBlobResultForNonSuccess(HttpResponse response)
    {
        using AVEVA::AzureClient::Models::DownloadBlobResult;
        using AVEVA::AzureClient::Private::CompleteParsed;
        using AVEVA::AzureClient::Private::ParseDownloadBlobResult;

        CompletedDownloadBlobResult completed;
        CompleteParsed<DownloadBlobResult>({},
            std::move(response),
            ParseDownloadBlobResult,
            [&](std::error_code error, AVEVA::AzureClient::Response<DownloadBlobResult> parsedResponse)
        {
            completed.ObservedError = error;
            completed.ObservedResponse = std::move(parsedResponse);
        });
        return completed;
    }

    [[nodiscard]] bool IsWellFormedPercentEncodedText(std::string_view encoded)
    {
        for (std::size_t index = 0; index < encoded.size(); ++index)
        {
            const auto ch = static_cast<unsigned char>(encoded.at(index));
            if (ch == '%')
            {
                if (encoded.size() < index + 3U)
                {
                    return false;
                }
                if (std::isxdigit(static_cast<unsigned char>(encoded.at(index + 1U))) == 0)
                {
                    return false;
                }
                if (std::isxdigit(static_cast<unsigned char>(encoded.at(index + 2U))) == 0)
                {
                    return false;
                }
                index += 2U;
                continue;
            }

            if (std::isalnum(ch) == 0 && ch != '-' && ch != '_' && ch != '.' && ch != '~' && ch != '/')
            {
                return false;
            }
        }
        return true;
    }

    void RunUrlEncodeFuzzIterations(std::mt19937& generator, std::uniform_int_distribution<int>& lengthDistribution)
    {
        for (int iteration = 0; iteration < 256; ++iteration)
        {
            const std::string value = RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)));
            EXPECT_TRUE(IsWellFormedPercentEncodedText(UrlEncode(value, "/")));
        }
    }

    [[nodiscard]] std::vector<HttpHeader> MakeRandomHeaders(std::mt19937& generator)
    {
        std::uniform_int_distribution<int> headerCountDistribution(0, 8);
        std::uniform_int_distribution<int> lengthDistribution(0, 16);
        std::vector<HttpHeader> headers;
        const int headerCount = headerCountDistribution(generator);
        headers.reserve(static_cast<std::size_t>(headerCount));
        for (int headerIndex = 0; headerIndex < headerCount; ++headerIndex)
        {
            headers.emplace_back(RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator))),
                RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator))));
        }
        return headers;
    }

    [[nodiscard]] std::string ExpectedHeaderValueForQuery(const std::vector<HttpHeader>& headers,
        std::string_view query)
    {
        for (const auto& header : headers)
        {
            if (IEquals(header.GetName(), query))
            {
                return header.GetValue();
            }
        }
        return {};
    }

    struct FindHeaderValueAttempt
    {
        bool Threw = false;
        std::string Value;
    };

    [[nodiscard]] FindHeaderValueAttempt TryFindHeaderValue(const HttpResponse& response, std::string_view query)
    {
        try
        {
            return {.Value = std::string{FindHeaderValue(response, query)}};
        }
        catch (...)
        {
            return {.Threw = true};
        }
    }

    void RunFindHeaderValueFuzzIterations(std::mt19937& generator)
    {
        std::uniform_int_distribution<int> lengthDistribution(0, 16);
        for (int iteration = 0; iteration < 128; ++iteration)
        {
            const std::vector<HttpHeader> headers = MakeRandomHeaders(generator);
            const std::string query = RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)));
            const HttpResponse response{200, headers, ""};
            const std::string expected = ExpectedHeaderValueForQuery(headers, query);
            const FindHeaderValueAttempt attempt = TryFindHeaderValue(response, query);
            EXPECT_FALSE(attempt.Threw);
            EXPECT_EQ(attempt.Value, expected);
        }
    }

    [[nodiscard]] std::vector<int> CountSharedKeySignerMismatchesAcrossThreads(
        const AVEVA::AzureClient::Private::SharedKeySigner& signer,
        const AVEVA::HttpRequest& request,
        std::string_view expected)
    {
        std::vector<std::thread> threads;
        std::vector<int> mismatches(8, 0);
        threads.reserve(mismatches.size());
        for (int& mismatchCount : mismatches)
        {
            threads.emplace_back([&signer, &request, expected, &mismatchCount]
            {
                for (int i = 0; i < 200; ++i)
                {
                    if (signer.Authorize(request) != expected)
                    {
                        ++mismatchCount;
                    }
                }
            });
        }
        for (std::thread& thread : threads)
        {
            thread.join();
        }
        return mismatches;
    }
} // namespace

TEST(BlobRequestHelpersTests, SharedKeyAuthorization_MatchesForOwnedAndViewBodies)
{
    using AVEVA::AzureClient::Private::AuthorizeRequest;

    SharedKeyCredentialOptions options{.AccountName = std::string("testaccount"),
        .AccountKey = std::string("MTIzNDU2")}; // base64 of "123456"

    AVEVA::HttpRequest reqOwned;
    reqOwned.SetMethod(AVEVA::HttpMethod::Put);
    reqOwned.SetUrl("https://example.com/container/blob");
    reqOwned.AddHeader(AVEVA::HttpHeader("x-ms-date", "Mon, 01 Jan 2020 00:00:00 GMT"));
    reqOwned.SetBody("abc");
    AuthorizeRequest(options, reqOwned);
    const std::string authOwned = FindRequestHeaderValue(reqOwned, "Authorization");

    AVEVA::HttpRequest reqView;
    reqView.SetMethod(AVEVA::HttpMethod::Put);
    reqView.SetUrl("https://example.com/container/blob");
    reqView.AddHeader(AVEVA::HttpHeader("x-ms-date", "Mon, 01 Jan 2020 00:00:00 GMT"));
    std::vector<std::byte> buffer{std::byte('a'), std::byte('b'), std::byte('c')};
    reqView.SetBodyView(std::span<const std::byte>(buffer.data(), buffer.size()));
    AuthorizeRequest(options, reqView);
    const std::string authView = FindRequestHeaderValue(reqView, "Authorization");

    EXPECT_EQ(authOwned, authView);
}

TEST(BlobRequestHelpersTests, UrlEncode_EncodesReservedCharactersUtf8AndExtraSafeCharacters)
{
    EXPECT_EQ(UrlEncode("folder name/file?.txt", {}), "folder%20name%2Ffile%3F.txt");
    EXPECT_EQ(UrlEncode("folder name/file?.txt", "/"), "folder%20name/file%3F.txt");
    EXPECT_EQ(UrlEncode("\xF0\x9F\x98\x80", {}), "%F0%9F%98%80");
    EXPECT_EQ(UrlEncode("a+b=c", "+"), "a+b%3Dc");
}

// Task 3 (Option A): ParseDownloadBlobResult must move the body out of the response into
// Content rather than copying it, so the response no longer also retains a full duplicate of
// the blob after parsing.
TEST(BlobRequestHelpersTests, ParseDownloadBlobResult_MovesBodyOutOfResponseInsteadOfCopyingIt)
{
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::Private::AsChars;
    using AVEVA::AzureClient::Private::ParseDownloadBlobResult;

    HttpResponse response{200, {{"Content-Length", "5"}}, "hello"};

    const auto result = ParseDownloadBlobResult(response);

    EXPECT_EQ(result.Content, "hello");
    // The response's body was moved out, not copied: it must now be empty.
    EXPECT_TRUE(response.GetBody().empty());
}

// Task 16 (Round 1 loose end): ParseDownloadBlobResult now takes HttpResponse& instead of
// const HttpResponse&, since it moves the body out on success. CompleteParsed only ever invokes
// the parser on the success path (!failure.Error) — on a non-2xx response the parser must not run
// at all, and the resulting BlobStorageError must still be populated correctly from
// DetermineBlobStorageFailure, exactly as it was before ParseDownloadBlobResult's signature
// changed.
TEST(BlobRequestHelpersTests, CompleteParsed_DownloadBlobResult_NonSuccessResponseSkipsParserAndReportsBlobStorageError)
{
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::DownloadBlobResult;
    using AVEVA::AzureClient::Private::CompleteParsed;
    using AVEVA::AzureClient::Private::ParseDownloadBlobResult;

    HttpResponse response{403,
        {{"x-ms-request-id", "request-id"}},
        R"(<Error><Code>AuthenticationFailed</Code><Message>Auth failed</Message></Error>)"};

    const CompletedDownloadBlobResult completed = CompleteDownloadBlobResultForNonSuccess(std::move(response));

    ASSERT_TRUE(completed.ObservedError.has_value());
    EXPECT_EQ(*completed.ObservedError, make_error_code(BlobStorageErrorCode::AuthenticationFailed));
    ASSERT_TRUE(completed.ObservedResponse.has_value());
    // The parser must not have run: Content stays default-constructed (empty) rather than
    // reflecting the error response's XML body.
    EXPECT_TRUE(ValueOrFail(completed.ObservedResponse).Value().Content.empty());
    ASSERT_TRUE(ValueOrFail(completed.ObservedResponse).Error().has_value());
    EXPECT_EQ(ValueOrFail(ValueOrFail(completed.ObservedResponse).Error()).ErrorCode, "AuthenticationFailed");
    EXPECT_EQ(ValueOrFail(ValueOrFail(completed.ObservedResponse).Error()).RequestId, "request-id");
}

TEST(BlobRequestHelpersTests, BuildAndParseHttpDateHeader_RoundTripsAndIgnoreGlobalLocale)
{
    const auto expectedTime = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);
    const std::string expectedText = "Fri, 26 Jun 2015 18:59:17 GMT";

    const auto nonEnglishLocale = TryCreateNonEnglishLocale();
    ASSERT_TRUE(nonEnglishLocale.has_value());

    ScopedGlobalLocale const guard(*nonEnglishLocale);
    EXPECT_EQ(BuildDateHeaderValue(expectedTime), expectedText);
    EXPECT_EQ(ParseHttpDateHeader(expectedText), std::optional{expectedTime});
}

TEST(BlobRequestHelpersTests, ParseHttpDateHeader_ReturnsNulloptForMalformedInput)
{
    EXPECT_EQ(ParseHttpDateHeader(""), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("not a date"), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("Fri, 99 Jun 2015 18:59:17 GMT"), std::nullopt);
}

TEST(BlobRequestHelpersTests, ParseHttpDateHeader_AcceptsAllRfc7231Formats)
{
    const auto expectedTime = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);

    EXPECT_EQ(ParseHttpDateHeader("Fri, 26 Jun 2015 18:59:17 GMT"), std::optional{expectedTime});
    EXPECT_EQ(ParseHttpDateHeader("Friday, 26-Jun-15 18:59:17 GMT"), std::optional{expectedTime});
    EXPECT_EQ(ParseHttpDateHeader("Fri Jun 26 18:59:17 2015"), std::optional{expectedTime});
}

TEST(BlobRequestHelpersTests, StringHelpers_AreCaseInsensitiveAndTrimAsExpected)
{
    EXPECT_TRUE(IEquals("x-ms-meta-project", "X-Ms-MeTa-PrOjEcT"));
    EXPECT_FALSE(IEquals("abc", "abcd"));

    EXPECT_TRUE(IStartsWith("X-Ms-Meta-Project", "x-ms-meta-"));
    EXPECT_FALSE(IStartsWith("metadata", "x-ms-meta-"));

    EXPECT_EQ(TrimTrailingSlashes("https://example.com///"), "https://example.com");
    EXPECT_EQ(TrimTrailingSlashes("plain"), "plain");

    EXPECT_EQ(TrimLeadingQuestionMark("?sv=1&sig=2"), "sv=1&sig=2");
    EXPECT_EQ(TrimLeadingQuestionMark("sv=1&sig=2"), "sv=1&sig=2");

    const auto nonEnglishLocale = TryCreateNonEnglishLocale();
    if (nonEnglishLocale.has_value())
    {
        ScopedGlobalLocale const guard(*nonEnglishLocale);
        EXPECT_TRUE(IEquals("CONTENT-TYPE", "content-type"));
        EXPECT_TRUE(IStartsWith("X-Ms-Meta-Owner", "x-ms-meta-"));
    }
}

TEST(BlobRequestHelpersTests, ParseBlobProperties_ExtractsMetadataAndHandlesUnknownValues)
{
    const auto expectedTime = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);
    const HttpResponse response{200,
        {
            {"ETag", "\"etag\""},
            {"Last-Modified", "Fri, 26 Jun 2015 18:59:17 GMT"},
            {"Content-Type", "application/octet-stream"},
            {"Cache-Control", "max-age=60"},
            {"Content-MD5", "abcd"},
            {"x-ms-blob-type", "NotARealBlobType"},
            {"x-ms-meta-Project", "aveva"},
            {"X-Ms-MeTa-OWNER", "storage"},
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

TEST(BlobRequestHelpersTests, ParseBlobProperties_ZeroContentLengthIsPresentAndNotTreatedAsMissing)
{
    const HttpResponse zero{200, {{"Content-Length", "0"}}, ""};
    EXPECT_EQ(ParseBlobProperties(zero).ContentLength, 0U);

    const HttpResponse ranged{206, {{"Content-Range", "bytes 0-0/10"}, {"Content-Length", "1"}}, ""};
    EXPECT_EQ(ParseBlobProperties(ranged).ContentLength, 10U);

    const HttpResponse emptyTotal{206, {{"Content-Range", "bytes 0-0/0"}, {"Content-Length", "1"}}, ""};
    EXPECT_EQ(ParseBlobProperties(emptyTotal).ContentLength, 1U);
}

TEST(BlobRequestHelpersTests, XmlParsers_HandleNamespacesAttributesEntitiesAndSelfClosingElements)
{
    const auto expectedTime = MakeUtcTimePoint(2015, 6, 26, 18, 59, 17);
    const std::string listBlobsXml = R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults xmlns="urn:test" xmlns:a="urn:a">
  <a:Prefix />
  <Delimiter>/</Delimiter>
  <Marker>ma&#x72;ker</Marker>
  <NextMarker><![CDATA[next<marker>]]></NextMarker>
  <Blobs>
    <a:Blob attr="1">
      <a:Name><![CDATA[folder/<blob>.txt]]></a:Name>
      <Snapshot>snap&#38;1</Snapshot>
      <Properties>
        <Etag>"etag"</Etag>
        <Last-Modified>Fri, 26 Jun 2015 18:59:17 GMT</Last-Modified>
        <Content-Length>123</Content-Length>
        <Content-Type>text/plain</Content-Type>
        <Content-MD5>abcd</Content-MD5>
        <Cache-Control>max-age=60</Cache-Control>
        <BlobType>BlockBlob</BlobType>
        <Metadata>
          <Project><![CDATA[aveva&cloud]]></Project>
          <Owner />
        </Metadata>
      </Properties>
    </a:Blob>
    <BlobPrefix attr="2">
      <Name>nested&#x2F;path&#x2F;</Name>
    </BlobPrefix>
  </Blobs>
</EnumerationResults>)";

    const auto blobs = ParseListBlobsResultXml(listBlobsXml);
    ASSERT_EQ(blobs.Blobs.size(), 1U);
    EXPECT_EQ(blobs.Prefix, "");
    EXPECT_EQ(blobs.Delimiter, "/");
    EXPECT_EQ(blobs.Marker, "marker");
    EXPECT_EQ(blobs.NextMarker, "next<marker>");
    EXPECT_EQ(blobs.Blobs.at(0).Name, "folder/<blob>.txt");
    EXPECT_EQ(blobs.Blobs.at(0).Snapshot, "snap&1");
    EXPECT_EQ(blobs.Blobs.at(0).Properties.ETag, "\"etag\"");
    EXPECT_EQ(blobs.Blobs.at(0).Properties.LastModified, expectedTime);
    EXPECT_EQ(blobs.Blobs.at(0).Properties.ContentLength, 123U);
    EXPECT_EQ(blobs.Blobs.at(0).Properties.ContentType, "text/plain");
    EXPECT_EQ(blobs.Blobs.at(0).Properties.ContentMd5, "abcd");
    EXPECT_EQ(blobs.Blobs.at(0).Properties.CacheControl, "max-age=60");
    EXPECT_EQ(blobs.Blobs.at(0).Properties.Type, BlobType::BlockBlob);
    EXPECT_EQ(blobs.Blobs.at(0).Properties.Metadata.at("project"), "aveva&cloud");
    EXPECT_EQ(blobs.Blobs.at(0).Properties.Metadata.at("owner"), "");
    ASSERT_EQ(blobs.BlobPrefixes.size(), 1U);
    EXPECT_EQ(blobs.BlobPrefixes.at(0), "nested/path/");

    const std::string listContainersXml = R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults xmlns:c="urn:containers">
  <Prefix>pre</Prefix>
  <Marker />
  <NextMarker>next</NextMarker>
  <Containers>
    <c:Container attr="x">
      <c:Name>demo&#38;container</c:Name>
      <Properties>
        <Etag>"container-etag"</Etag>
        <Last-Modified>Fri, 26 Jun 2015 18:59:17 GMT</Last-Modified>
      </Properties>
      <Metadata>
        <Project><![CDATA[aveva]]></Project>
        <Owner />
      </Metadata>
    </c:Container>
  </Containers>
</EnumerationResults>)";

    const auto containers = ParseListBlobContainersResultXml(listContainersXml);
    ASSERT_EQ(containers.Containers.size(), 1U);
    EXPECT_EQ(containers.Prefix, "pre");
    EXPECT_EQ(containers.Marker, "");
    EXPECT_EQ(containers.NextMarker, "next");
    EXPECT_EQ(containers.Containers.at(0).Name, "demo&container");
    EXPECT_EQ(containers.Containers.at(0).Properties.ETag, "\"container-etag\"");
    EXPECT_EQ(containers.Containers.at(0).Properties.LastModified, expectedTime);
    EXPECT_EQ(containers.Containers.at(0).Properties.Metadata.at("project"), "aveva");
    EXPECT_EQ(containers.Containers.at(0).Properties.Metadata.at("owner"), "");
}

TEST(BlobRequestHelpersTests, XmlParsers_HandleNamespacedBlockListsPageRangesAndErrors)
{
    const std::string blockListXml = R"(<?xml version="1.0" encoding="utf-8"?>
<BlockList xmlns:b="urn:block">
  <b:CommittedBlocks>
    <b:Block attr="1">
      <Name>YmxvY2sx</Name>
      <Size>16</Size>
    </b:Block>
  </b:CommittedBlocks>
  <UncommittedBlocks>
    <Block>
      <Name>YmxvY2sy</Name>
      <Size>8</Size>
    </Block>
  </UncommittedBlocks>
</BlockList>)";

    const auto blockList = ParseGetBlockListResultXml(blockListXml);
    ASSERT_EQ(blockList.CommittedBlocks.size(), 1U);
    EXPECT_EQ(blockList.CommittedBlocks.at(0).Name, "YmxvY2sx");
    EXPECT_EQ(blockList.CommittedBlocks.at(0).Size, 16U);
    ASSERT_EQ(blockList.UncommittedBlocks.size(), 1U);
    EXPECT_EQ(blockList.UncommittedBlocks.at(0).Name, "YmxvY2sy");
    EXPECT_EQ(blockList.UncommittedBlocks.at(0).Size, 8U);

    const std::string pageRangesXml = R"(<?xml version="1.0" encoding="utf-8"?>
<PageList xmlns:p="urn:page">
  <p:PageRange>
    <p:Start>0</p:Start>
    <p:End>511</p:End>
  </p:PageRange>
</PageList>)";

    const auto pageRanges = ParseGetPageRangesResultXml(pageRangesXml);
    ASSERT_EQ(pageRanges.PageRanges.size(), 1U);
    EXPECT_EQ(pageRanges.PageRanges.at(0).Start, 0U);
    EXPECT_EQ(pageRanges.PageRanges.at(0).End, 511U);

    const HttpResponse response{403,
        {{"x-ms-request-id", "request-id"}},
        R"(<Error xmlns:e="urn:error">
            <Code>AuthenticationFailed</Code>
            <e:Message><![CDATA[Auth <failed>]]></e:Message>
            <AuthenticationErrorDetail>More&#x20;detail&#38;info</AuthenticationErrorDetail>
        </Error>)"};

    const auto failure = DetermineBlobStorageFailure({}, response);
    ASSERT_TRUE(failure.Error);
    ASSERT_TRUE(failure.Details.has_value());
    EXPECT_EQ(failure.Error, make_error_code(BlobStorageErrorCode::AuthenticationFailed));
    EXPECT_EQ(ValueOrFail(failure.Details).ErrorCode, "AuthenticationFailed");
    EXPECT_EQ(ValueOrFail(failure.Details).Message, "Auth <failed> More detail&info");
    EXPECT_EQ(ValueOrFail(failure.Details).RequestId, "request-id");
}

TEST(BlobRequestHelpersTests, BoostUrlHelpers_PreserveExistingUrlShapes)
{
    EXPECT_EQ(BuildQueryString({{"comp", "list"}, {"prefix", "folder name"}}), "comp=list&prefix=folder%20name");

    BlobClientOptions blobOptions;
    blobOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    blobOptions.ContainerName = "container";
    blobOptions.BlobName = "folder name/file?.txt";
    blobOptions.Snapshot = "2024-01-02T03:04:05.0000000Z";
    blobOptions.SasToken = "sv=1&sig=a%2Bb";
    EXPECT_EQ(BuildBlobUrl(blobOptions, "comp=blocklist"),
        "https://account.blob.core.windows.net/container/folder%20name/"
        "file%3F.txt?snapshot=2024-01-02T03%3A04%3A05.0000000Z&comp=blocklist&sv=1&sig=a%2Bb");

    BlobContainerClientOptions containerOptions;
    containerOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    containerOptions.ContainerName = "container";
    containerOptions.SasToken = "sv=1&sig=a%2Bb";
    EXPECT_EQ(BuildContainerUrl(containerOptions, "comp=list&include=metadata"),
        "https://account.blob.core.windows.net/container?restype=container&comp=list&include=metadata&sv=1&sig=a%2Bb");
}

TEST(BlobRequestHelpersTests, ParseHttpDateHeader_FuzzDoesNotThrow)
{
    std::seed_seq seed{12345}; // A fixed seed keeps this test deterministic.
    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> lengthDistribution(0, 64);

    for (int iteration = 0; iteration < 256; ++iteration)
    {
        const std::string value = RandomBytes(generator, static_cast<std::size_t>(lengthDistribution(generator)));
        EXPECT_NO_THROW({
            const auto parsed = ParseHttpDateHeader(value);
            static_cast<void>(parsed);
        });
    }
}

TEST(BlobRequestHelpersTests, UrlEncode_FuzzProducesWellFormedPercentEscapes)
{
    std::seed_seq seed{54321}; // A fixed seed keeps this test deterministic.
    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> lengthDistribution(0, 64);

    RunUrlEncodeFuzzIterations(generator, lengthDistribution);
}

TEST(BlobRequestHelpersTests, FindHeaderValue_FuzzDoesNotThrowAndReturnsMatchingValue)
{
    std::seed_seq seed{9999}; // A fixed seed keeps this test deterministic.
    std::mt19937 generator(seed);
    RunFindHeaderValueFuzzIterations(generator);
}

TEST(BlobRequestHelpersTests, SharedKeyAuthorization_KnownAnswerVectorsMatchIndependentSignatures)
{
    const SharedKeyCredentialOptions sharedKey{.AccountName = "storageaccount",
        .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};

    struct KnownAnswerCase
    {
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
                    {"x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT"},
                    {"x-ms-version", "2023-11-03"},
                    {"X-Ms-Meta-Owner", "  Alice  "},
                    {"If-None-Match", "\"etag-123\""},
                    {"Range", "bytes=1-3"},
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
                    {"x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT"},
                    {"x-ms-version", "2023-11-03"},
                    {"X-Ms-Blob-Type", "BlockBlob"},
                    {"x-ms-meta-Project", " AzureClient "},
                    {"Content-MD5", "sQqNsWTgdUEFt6mb5y4/5Q=="},
                    {"Content-Type", "application/xml; charset=utf-8"},
                    {"If-Match", "\"match-me\""},
                    {"If-Unmodified-Since", "Sat, 05 Nov 1994 08:49:37 GMT"},
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
                    {"x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT"},
                    {"X-Ms-Version", "2023-11-03"},
                    {"If-Modified-Since", "Sat, 05 Nov 1994 08:49:37 GMT"},
                    {"x-ms-meta-Zebra", " z "},
                    {"x-ms-meta-alpha", " a "},
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
                    {"x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT"},
                    {"x-ms-version", "2023-11-03"},
                    {"x-ms-delete-snapshots", "include"},
                    {"If-Modified-Since", "Sat, 05 Nov 1994 08:49:37 GMT"},
                },
            .Body = "",
            .ExpectedAuthorization = "SharedKey storageaccount:/jcIn141mdTeJVNSo/mDdDy0DesOllUXRHbgKkBWAZA=",
        },
    };

    for (const auto& testCase : cases)
    {
        HttpRequest request;
        request.SetMethod(testCase.Method);
        request.SetUrl(testCase.Url);
        request.SetBody(testCase.Body);
        for (const auto& header : testCase.Headers)
        {
            request.AddHeader(header);
        }

        AuthorizeRequest(sharedKey, request);
        EXPECT_EQ(FindRequestHeaderValue(request, "Authorization"), testCase.ExpectedAuthorization) << testCase.Name;
    }
}

TEST(BlobRequestHelpersTests, XmlParsers_RejectEmptyBodiesButAcceptEmptyDocuments)
{
    // An empty 2xx body is a truncated response, not an empty listing (reported as InvalidResponse).
    EXPECT_THROW(static_cast<void>(ParseListBlobsResultXml({})), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(ParseListBlobContainersResultXml({})), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(ParseGetBlockListResultXml({})), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(ParseGetPageRangesResultXml({})), std::invalid_argument);

    EXPECT_TRUE(ParseListBlobsResultXml("<EnumerationResults/>").Blobs.empty());
    EXPECT_TRUE(ParseListBlobContainersResultXml("<EnumerationResults/>").Containers.empty());
    EXPECT_TRUE(ParseGetBlockListResultXml("<BlockList/>").CommittedBlocks.empty());
    EXPECT_TRUE(ParseGetPageRangesResultXml("<PageList/>").PageRanges.empty());
}

TEST(BlobRequestHelpersTests, XmlParsers_RejectMalformedAndTruncatedXmlClearly)
{
    EXPECT_THROW(static_cast<void>(ParseListBlobsResultXml("<EnumerationResults><Blobs>")), std::invalid_argument);
    EXPECT_THROW(
        static_cast<void>(ParseListBlobContainersResultXml("<EnumerationResults><Containers></EnumerationResults")),
        std::invalid_argument);
    EXPECT_THROW(static_cast<void>(ParseGetBlockListResultXml("<BlockList><CommittedBlocks><Block></BlockList>")),
        std::invalid_argument);
    EXPECT_THROW(static_cast<void>(ParseGetPageRangesResultXml("<PageList><PageRange><Start>0</Start>")),
        std::invalid_argument);
}

TEST(BlobRequestHelpersTests, XmlParsers_HandleNestedSameNameElementsAndNumericEntities)
{
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

TEST(BlobRequestHelpersTests, UrlBuilders_HandlePortsUnicodeLiteralPercentsAndDuplicateQueryKeys)
{
    BlobClientOptions blobOptions;
    blobOptions.ServiceEndpoint = "HTTPS://StorageAccount.blob.core.windows.net:8443";
    blobOptions.ContainerName = "container";
    blobOptions.BlobName = "emoji-\xF0\x9F\x98\x80-%2F.txt";
    blobOptions.SasToken = "sv=1&sig=a%2Bb";
    blobOptions.Snapshot = "2024-01-02T03:04:05.0000000Z";

    const std::string blobUrl = BuildBlobUrl(blobOptions, "comp=metadata");
    EXPECT_TRUE(IStartsWith(blobUrl, "https://"));
    EXPECT_NE(blobUrl.find(":8443/container/emoji-%F0%9F%98%80-%252F.txt"), std::string::npos);
    EXPECT_NE(blobUrl.find("snapshot=2024-01-02T03%3A04%3A05.0000000Z&comp=metadata&sv=1&sig=a%2Bb"),
        std::string::npos);

    EXPECT_EQ(
        BuildQueryString({{"comp", "list"}, {"include", "metadata"}, {"include", "snapshots"}, {"marker", "A+B&C=%"}}),
        "comp=list&include=metadata&include=snapshots&marker=A%2BB%26C%3D%25");

    BlobServiceClientOptions serviceOptions;
    serviceOptions.ServiceEndpoint = "https://account.blob.core.windows.net:9443";
    serviceOptions.SasToken = "sv=1&sig=s%2Bv";
    EXPECT_EQ(BuildServiceUrl(serviceOptions, BuildQueryString({{"comp", "list"}, {"marker", "na\xC3\xAFve"}})),
        "https://account.blob.core.windows.net:9443?comp=list&marker=na%C3%AFve&sv=1&sig=s%2Bv");

    BlobContainerClientOptions containerOptions;
    containerOptions.ServiceEndpoint = "https://account.blob.core.windows.net";
    containerOptions.ContainerName = "images";
    containerOptions.SasToken = "sv=1&sig=abc%2B123%26x%3D1";
    EXPECT_EQ(BuildContainerUrl(containerOptions,
                  BuildQueryString({{"comp", "list"}, {"marker", "next+page"}, {"include", "metadata"}})),
        "https://account.blob.core.windows.net/"
        "images?restype=container&comp=list&marker=next%2Bpage&include=metadata&sv=1&sig=abc%2B123%26x%3D1");
}

TEST(BlobRequestHelpersTests, DateHelpers_HandleLeapDaysPreEpochWhitespaceCaseAndBoundaries)
{
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

TEST(BlobRequestHelpersTests, DateHelpers_RejectInvalidWeekdayAndMonthNames)
{
    EXPECT_EQ(ParseHttpDateHeader("Fry, 26 Jun 2015 18:59:17 GMT"), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("Fri, 26 Jnn 2015 18:59:17 GMT"), std::nullopt);
    EXPECT_EQ(ParseHttpDateHeader("Funday, 26-Jun-15 18:59:17 GMT"), std::nullopt);
}

TEST(BlobRequestHelpersTests, BuildRangeHeaderValue_RejectsZeroLengthAndOverflow)
{
    EXPECT_THROW(static_cast<void>(AVEVA::AzureClient::Private::BuildRangeHeaderValue(
                     AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 0U})),
        std::invalid_argument);
    EXPECT_THROW(static_cast<void>(AVEVA::AzureClient::Private::BuildRangeHeaderValue(
                     AVEVA::AzureClient::Models::BlobByteRange{.Offset = std::numeric_limits<std::uint64_t>::max() - 3U,
                         .Length = 8U})),
        std::invalid_argument);
}

namespace
{
    [[nodiscard]] AVEVA::HttpRequest MakeGoldenGetRequest()
    {
        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Get);
        request.SetUrl("https://storageaccount.blob.core.windows.net/photos/"
                       "cat%20photo.jpg?zeta=last&comp=metadata&Alpha=first&timeout=30");
        for (const auto& [name, value] :
            std::vector<std::pair<std::string, std::string>>{{"x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT"},
                {"x-ms-version", "2023-11-03"},
                {"X-Ms-Meta-Owner", "  Alice  "},
                {"If-None-Match", "\"etag-123\""},
                {"Range", "bytes=1-3"}})
        {
            request.AddHeader(AVEVA::HttpHeader{name, value});
        }
        return request;
    }
} // namespace

// T12: the per-connection signer is keyed once and reused; it must give the golden signature on
// every call, including concurrent calls from several threads.
TEST(BlobRequestHelpersTests, SharedKeySigner_IsReusableAndThreadSafe)
{
    using AVEVA::AzureClient::Private::SharedKeySigner;
    constexpr std::string_view Expected = "SharedKey storageaccount:RRz8HIx1QCyRXmSRoclBHAGSviMjbDbPzYyxnnl72VA=";

    const SharedKeySigner signer{"storageaccount", "MDEyMzQ1Njc4OWFiY2RlZg=="};
    const AVEVA::HttpRequest request = MakeGoldenGetRequest();
    EXPECT_EQ(signer.AccountName(), "storageaccount");
    EXPECT_EQ(signer.Authorize(request), Expected);
    EXPECT_EQ(signer.Authorize(request), Expected);

    const std::vector<int> mismatches = CountSharedKeySignerMismatchesAcrossThreads(signer, request, Expected);
    for (std::size_t t = 0; t < mismatches.size(); ++t)
    {
        EXPECT_EQ(mismatches.at(t), 0) << "thread " << t;
    }
}

TEST(BlobRequestHelpersTests, SharedKeySigner_RejectsMalformedKey)
{
    using AVEVA::AzureClient::Private::SharedKeySigner;
    EXPECT_THROW((SharedKeySigner{"account", "not base64!"}), std::invalid_argument);
}

// T14: options are normalised once into an immutable ConnectionState shared by child clients.
TEST(BlobRequestHelpersTests, MakeConnectionState_NormalisesOptionsOnce)
{
    using AVEVA::AzureClient::BlobServiceClientOptions;
    using AVEVA::AzureClient::Private::MakeBlobTarget;
    using AVEVA::AzureClient::Private::MakeConnectionState;

    const auto sharedKey =
        MakeConnectionState(BlobServiceClientOptions{.ServiceEndpoint = "http://127.0.0.1:10000/devstoreaccount1/",
            .SharedKey = {.AccountName = "devstoreaccount1", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="}});
    ASSERT_NE(sharedKey->Signer, nullptr);
    EXPECT_EQ(sharedKey->Signer->AccountName(), "devstoreaccount1");
    EXPECT_EQ(sharedKey->BasePrefix, "http://127.0.0.1:10000");
    EXPECT_EQ(sharedKey->BasePath, "/devstoreaccount1");
    EXPECT_EQ(sharedKey->TokenCredential, nullptr);

    const auto blob = MakeBlobTarget(sharedKey, "container", "dir/blob.txt");
    EXPECT_EQ(blob.Connection, sharedKey) << "child targets share, not copy, the connection";
    EXPECT_EQ(BuildBlobUrl(blob), "http://127.0.0.1:10000/devstoreaccount1/container/dir/blob.txt");

    const auto bearer = MakeConnectionState(
        BlobServiceClientOptions{.ServiceEndpoint = "https://account.blob.core.windows.net", .BearerToken = "token"});
    EXPECT_EQ(bearer->Signer, nullptr);
    EXPECT_NE(bearer->TokenCredential, nullptr) << "BearerToken is folded into a TokenCredential";
    EXPECT_EQ(bearer->BasePath, "");

    const auto sas =
        MakeConnectionState(BlobServiceClientOptions{.ServiceEndpoint = "https://account.blob.core.windows.net",
            .SasToken = "?sv=1&sig=x"});
    EXPECT_EQ(sas->SasToken, "sv=1&sig=x");

    EXPECT_THROW(static_cast<void>(MakeConnectionState(
                     BlobServiceClientOptions{.ServiceEndpoint = "https://account.blob.core.windows.net",
                         .SharedKey = {.AccountName = "a", .AccountKey = "not base64!"}})),
        std::invalid_argument);
    EXPECT_THROW(static_cast<void>(MakeBlobTarget(sharedKey, "Bad_Name", "blob")), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(MakeBlobTarget(sharedKey, "container", "")), std::invalid_argument);
}

TEST(BlobRequestHelpersTests, ParseListBlobsResultXml_HandlesEntitiesCdataCommentsAndNamespaces)
{
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
    EXPECT_EQ(result.BlobPrefixes, (std::vector<std::string>{"dir/"}));
    EXPECT_TRUE(result.NextMarker.empty());

    EXPECT_THROW(static_cast<void>(ParseListBlobsResultXml("<EnumerationResults><Blobs></EnumerationResults>")),
        std::invalid_argument);
}

TEST(BlobRequestHelpersTests, SharedKeyStringToSign_SupportsEveryHttpMethod)
{
    using AVEVA::HttpMethod;
    for (const auto& [method, verb] : std::vector<std::pair<HttpMethod, std::string>>{{HttpMethod::Get, "GET"},
             {HttpMethod::Post, "POST"},
             {HttpMethod::Put, "PUT"},
             {HttpMethod::Patch, "PATCH"},
             {HttpMethod::Delete, "DELETE"},
             {HttpMethod::Head, "HEAD"},
             {HttpMethod::Options, "OPTIONS"},
             {HttpMethod::Trace, "TRACE"},
             {HttpMethod::Connect, "CONNECT"}})
    {
        AVEVA::HttpRequest request;
        request.SetMethod(method);
        request.SetUrl("https://account.blob.core.windows.net/container/blob");
        EXPECT_TRUE(
            AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request).starts_with(verb + "\n"))
            << verb;
    }
}

TEST(SharedKeyCanonicalizationTests, RepeatedHeadersAndQueryParametersAreGrouped)
{
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl("https://account.blob.core.windows.net/container/blob?b=2&a=z&a=y&comp=list");
    request.AddHeader(AVEVA::HttpHeader{"x-ms-meta-k", "v1"});
    request.AddHeader(AVEVA::HttpHeader{"x-ms-a", "first"});
    request.AddHeader(AVEVA::HttpHeader{"X-MS-Meta-K", " v2 "});

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_NE(toSign.find("x-ms-a:first\nx-ms-meta-k:v1,v2\n"), std::string::npos) << toSign;
    EXPECT_NE(toSign.find("/account/container/blob\na:y,z\nb:2\ncomp:list"), std::string::npos) << toSign;
}

TEST(SharedKeyCanonicalizationTests, HeadersAreOrderedWithCultureAwareRules)
{
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl("https://account.blob.core.windows.net/container/blob");
    for (const char* name : {"x-ms-meta-a1", "x-ms-meta-b-c", "x-ms-meta-a_", "x-ms-meta-bc"})
    {
        request.AddHeader(AVEVA::HttpHeader{name, "v"});
    }

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_NE(toSign.find("x-ms-meta-a_:v\nx-ms-meta-a1:v\nx-ms-meta-bc:v\nx-ms-meta-b-c:v\n"), std::string::npos)
        << toSign;
}

TEST(SharedKeyCanonicalizationTests, ResourcePathIsUsedAsSentOnTheWire)
{
    AVEVA::HttpRequest request;
    request.SetMethod(HttpMethod::Get);
    request.SetUrl("https://account.blob.core.windows.net/c/a%2Bb%20c.txt?x=1%202");

    const std::string toSign = AVEVA::AzureClient::Private::BuildSharedKeyStringToSign("account", request);
    EXPECT_NE(toSign.find("/account/c/a%2Bb%20c.txt\nx:1 2"), std::string::npos) << toSign;
}
