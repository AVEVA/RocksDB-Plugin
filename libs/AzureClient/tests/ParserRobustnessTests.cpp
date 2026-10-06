// Parser robustness (T28): every Parse* function must turn malformed service output into a
// std::exception (reported to callers as BlobStorageErrorCode::InvalidResponse), tolerate missing
// optional fields, accept namespaced XML and entities, and scale to full 5,000-item pages.
#include "BlobRequestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <cstdint>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <initializer_list>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using namespace AVEVA::AzureClient::Private;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using namespace std::chrono;

    struct XmlParser
    {
        std::string Name;
        std::function<void(std::string_view)> Parse;
    };

    [[nodiscard]] std::vector<XmlParser> XmlParsers()
    {
        return {
            {.Name = "ListBlobs",
                .Parse =
                    [](std::string_view xml)
        {
            static_cast<void>(ParseListBlobsResultXml(xml));
        }},
            {.Name = "GetPageRanges",
                .Parse =
                    [](std::string_view xml)
        {
            static_cast<void>(ParseGetPageRangesResultXml(xml));
        }},
            {.Name = "UserDelegationKey",
                .Parse =
                    [](std::string_view xml)
        {
            static_cast<void>(ParseUserDelegationKeyXml(xml));
        }},
        };
    }

    [[nodiscard]] std::string BlobXml(std::string_view properties)
    {
        return std::string{"<EnumerationResults><Blobs><Blob><Name>b</Name><Properties>"} + std::string{properties} +
               "</Properties></Blob></Blobs></EnumerationResults>";
    }

    void ExpectParserThrowsOnMalformedDocument(const XmlParser& parser, const std::string& xml)
    {
        SCOPED_TRACE(parser.Name + " <- [" + xml + "]");
        EXPECT_THROW(parser.Parse(xml), std::exception);
    }

    void ExpectMalformedDocumentsThrowForAllParsers(const std::vector<std::string>& malformed)
    {
        for (const XmlParser& parser : XmlParsers())
        {
            for (const std::string& xml : malformed)
            {
                ExpectParserThrowsOnMalformedDocument(parser, xml);
            }
        }
    }

    void ExpectListBlobsPropertiesAreRejected(std::string_view properties)
    {
        SCOPED_TRACE(properties);
        EXPECT_THROW(static_cast<void>(ParseListBlobsResultXml(BlobXml(properties))), std::exception);
    }

    [[nodiscard]] std::string MakeDeeplyNestedEnumerationResults(std::size_t depth)
    {
        std::string xml = "<EnumerationResults>";
        for (std::size_t index = 0; index < depth; ++index)
        {
            xml += "<a>";
        }
        for (std::size_t index = 0; index < depth; ++index)
        {
            xml += "</a>";
        }
        xml += "</EnumerationResults>";
        return xml;
    }

    void ExpectParserRejectsDocument(const XmlParser& parser, const std::string& xml)
    {
        SCOPED_TRACE(parser.Name);
        EXPECT_THROW(parser.Parse(xml), std::exception);
    }

    void ExpectAllParsersRejectDocument(const std::string& xml)
    {
        for (const XmlParser& parser : XmlParsers())
        {
            ExpectParserRejectsDocument(parser, xml);
        }
    }
} // namespace

TEST(ParserRobustnessTests, MalformedDocumentsThrowStdExceptions)
{
    const std::vector<std::string> malformed{
        "",
        "not xml at all",
        "<",
        "<EnumerationResults>",
        "<EnumerationResults><Blobs></EnumerationResults>",
        "<?xml version=\"1.0\"?>",
        std::string{"<A>\0</A>", 8},
        "<A attr=\"unterminated></A>",
        "\xEF\xBB\xBF<A>",
        "<!-- comment only -->",
        // Well-formed, but not the document the operation returns (e.g. a stray error body).
        "<Error><Code>InternalError</Code></Error>",
        "<Unexpected/>",
        // libxml2 enforces well-formedness: mismatched end tags and undeclared entities are rejected.
        "<EnumerationResults><Blobs></Blob></EnumerationResults>",
        "<EnumerationResults><NextMarker>&undeclared;</NextMarker></EnumerationResults>",
        // Service responses never carry a DOCTYPE; any DTD (and with it entity declarations) is refused.
        "<!DOCTYPE EnumerationResults><EnumerationResults/>",
        R"(<!DOCTYPE EnumerationResults [<!ENTITY e "x">]><EnumerationResults><NextMarker>&e;</NextMarker></EnumerationResults>)",
        "<!DOCTYPE EnumerationResults SYSTEM \"file:///etc/passwd\"><EnumerationResults/>",
    };
    ExpectMalformedDocumentsThrowForAllParsers(malformed);
}

TEST(ParserRobustnessTests, ArbitraryBytesNeverEscapeAsNonStdExceptions)
{
    // A deterministic stream of bytes biased towards XML punctuation.
    constexpr std::string_view Alphabet = "<>/=\"'&;!?[]-abcXYZ019 \n\t\x80\xFF";
    std::uint32_t state = 12345U;
    for (const XmlParser& parser : XmlParsers())
    {
        for (int sample = 0; sample < 200; ++sample)
        {
            std::string xml;
            const std::size_t length = state % 64U;
            for (std::size_t index = 0; index < length; ++index)
            {
                state = (state * 1103515245U) + 12345U;
                xml += Alphabet.at((state >> 16U) % Alphabet.size());
            }
            try
            {
                parser.Parse(xml);
            }
            catch (const std::exception& rejected)
            {
                // Throwing std::exception is the documented way for a parser to reject malformed input.
                static_cast<void>(rejected.what());
            }
            catch (...)
            {
                ADD_FAILURE() << parser.Name << " threw a non-std exception for [" << xml << "]";
            }
        }
    }
}

TEST(ParserRobustnessTests, ListBlobs_MissingOptionalFieldsUseDefaults)
{
    const auto result = ParseListBlobsResultXml(
        "<EnumerationResults><Blobs><Blob><Name>only-name</Name></Blob></Blobs></EnumerationResults>");
    ASSERT_EQ(result.Blobs.size(), 1U);
    EXPECT_EQ(result.Blobs.at(0).Name, "only-name");
    EXPECT_EQ(result.Blobs.at(0).Properties.ContentLength, 0U);
    EXPECT_TRUE(result.Blobs.at(0).Properties.ETag.empty());
    EXPECT_TRUE(result.Blobs.at(0).Properties.Metadata.empty());
    EXPECT_TRUE(result.NextMarker.empty());

    const auto empty = ParseListBlobsResultXml("<EnumerationResults/>");
    EXPECT_TRUE(empty.Blobs.empty());
    EXPECT_TRUE(empty.BlobPrefixes.empty());
}

TEST(ParserRobustnessTests, ListBlobs_MalformedNumbersAndDatesThrow)
{
    for (const std::string_view properties : {"<Content-Length>12x</Content-Length>",
             "<Content-Length>-1</Content-Length>",
             "<Content-Length>99999999999999999999999</Content-Length>",
             "<Content-Length> 1</Content-Length>",
             "<Last-Modified>yesterday</Last-Modified>",
             "<Last-Modified>Tue, 31 Feb 2024 00:00:00 GMT</Last-Modified>"})
    {
        ExpectListBlobsPropertiesAreRejected(properties);
    }
}

TEST(ParserRobustnessTests, ListBlobs_NamespacesEntitiesAndCdataAreDecoded)
{
    const auto result =
        ParseListBlobsResultXml("<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                                "<s:EnumerationResults xmlns:s=\"urn:storage\" ServiceEndpoint=\"https://a/\">"
                                "<s:Blobs><s:Blob><s:Name>a&amp;b&lt;c&gt;&quot;&apos;&#x41;&#66;</s:Name>"
                                "<s:Properties><s:Content-Length>7</s:Content-Length></s:Properties></s:Blob>"
                                "<Blob><Name><![CDATA[x<y]]></Name></Blob>"
                                "<BlobPrefix><Name>dir&amp;/</Name></BlobPrefix></s:Blobs>"
                                "<s:NextMarker>m&amp;2</s:NextMarker></s:EnumerationResults>");
    ASSERT_EQ(result.Blobs.size(), 2U);
    EXPECT_EQ(result.Blobs.at(0).Name, "a&b<c>\"'AB");
    EXPECT_EQ(result.Blobs.at(0).Properties.ContentLength, 7U);
    EXPECT_EQ(result.Blobs.at(1).Name, "x<y");
    EXPECT_EQ(result.BlobPrefixes, (std::vector<std::string>{"dir&/"}));
    EXPECT_EQ(result.NextMarker, "m&2");
}

TEST(ParserRobustnessTests, DeeplyNestedDocumentsAreRejectedWithoutExhaustingTheStack)
{
    constexpr std::size_t Depth = 100000U;
    const std::string xml = MakeDeeplyNestedEnumerationResults(Depth);
    ExpectAllParsersRejectDocument(xml);
    EXPECT_TRUE(DetermineBlobStorageFailure({}, HttpResponse{500, {}, xml}).Error);
}

TEST(ParserRobustnessTests, ListBlobs_UnboundNamespacePrefixesAreTolerated)
{
    const auto result = ParseListBlobsResultXml(
        "<s:EnumerationResults><s:Blobs><s:Blob><s:Name>n</s:Name></s:Blob></s:Blobs></s:EnumerationResults>");
    ASSERT_EQ(result.Blobs.size(), 1U);
    EXPECT_EQ(result.Blobs.at(0).Name, "n");
}

TEST(ParserRobustnessTests, ListBlobs_FullPageOf5000Items)
{
    std::string xml = "<EnumerationResults><Blobs>";
    for (int index = 0; index < 5000; ++index)
    {
        xml += "<Blob><Name>blob-" + std::to_string(index) + "</Name><Properties><Content-Length>" +
               std::to_string(index) +
               "</Content-Length><Last-Modified>Tue, 02 Jan 2024 00:00:00 GMT</Last-Modified></Properties></Blob>";
    }
    xml += "</Blobs><NextMarker>next</NextMarker></EnumerationResults>";

    const auto result = ParseListBlobsResultXml(xml);
    ASSERT_EQ(result.Blobs.size(), 5000U);
    EXPECT_EQ(result.Blobs.back().Name, "blob-4999");
    EXPECT_EQ(result.Blobs.back().Properties.ContentLength, 4999U);
    EXPECT_EQ(result.NextMarker, "next");
}

TEST(ParserRobustnessTests, UserDelegationKey_MalformedDatesThrow)
{
    EXPECT_THROW(
        static_cast<void>(ParseUserDelegationKeyXml(
            "<UserDelegationKey><SignedStart>not-a-date</SignedStart><Value>dg==</Value></UserDelegationKey>")),
        std::exception);
    EXPECT_THROW(static_cast<void>(ParseUserDelegationKeyXml("<UserDelegationKey><SignedExpiry>2024-13-01T00:00:00Z</"
                                                             "SignedExpiry><Value>dg==</Value></UserDelegationKey>")),
        std::exception);
}

TEST(ParserRobustnessTests, HttpDate_RejectsOutOfRangeAndTrailingInput)
{
    EXPECT_TRUE(ParseHttpDateHeader("Thu, 29 Feb 2024 23:59:59 GMT").has_value());
    for (const std::string_view value : {"",
             "Thu, 29 Feb 2023 00:00:00 GMT",
             "Tue, 32 Jan 2024 00:00:00 GMT",
             "Tue, 02 Jan 2024 24:00:00 GMT",
             "Tue, 02 Jan 2024 00:60:00 GMT",
             "Tue, 02 Foo 2024 00:00:00 GMT",
             "Tue, 02 Jan 2024 00:00:00 PST",
             "Tue, 02 Jan 2024 00:00:00 GMT trailing",
             "Tue, 02 Jan 2024",
             "Tue, -2 Jan 2024 00:00:00 GMT"})
    {
        SCOPED_TRACE(value);
        EXPECT_FALSE(ParseHttpDateHeader(value).has_value());
    }
}

TEST(ParserRobustnessTests, Iso8601_RejectsMalformedValues)
{
    const auto parsed = ParseIso8601Utc("2024-01-02T03:04:05.1234567Z");
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(floor<seconds>(*parsed), sys_days{2024y / January / 2} + 3h + 4min + 5s);
    for (const std::string_view value : {"",
             "2024-01-02",
             "2024-01-02T03:04:05",
             "2024-02-30T00:00:00Z",
             "2024-01-02T25:00:00Z",
             "2024-01-02T03:04:05.Z",
             "garbage"})
    {
        SCOPED_TRACE(value);
        EXPECT_FALSE(ParseIso8601Utc(value).has_value());
    }
}

TEST(ParserRobustnessTests, ContentRange_RejectsMalformedAndOverflowingValues)
{
    for (const std::string_view value : {"",
             "bytes",
             "bytes 9-0/10",
             "bytes 0-9/5",
             "bytes a-9/10",
             "bytes 0-99999999999999999999/*",
             "items 0-9/10",
             "bytes 0-9/10 extra",
             "bytes -1-9/10"})
    {
        SCOPED_TRACE(value);
        EXPECT_FALSE(ParseContentRange(value).has_value());
    }
    const auto unknownTotal = ParseContentRange("bytes 0-9/*");
    ASSERT_TRUE(unknownTotal.has_value());
    EXPECT_FALSE(ValueOrFail(unknownTotal).Total.has_value());
}

TEST(ParserRobustnessTests, ErrorResponses_MalformedBodiesStillMapToAFailure)
{
    for (const std::string_view body : {"",
             "<Error>",
             "not xml",
             "<Error><Code>ContainerNotFound</Code></Error>",
             "<e:Error xmlns:e=\"urn:x\"><e:Code>BlobNotFound</e:Code></e:Error>"})
    {
        SCOPED_TRACE(body);
        const HttpResponse response{404, {}, std::string{body}};
        const RequestFailure failure = DetermineBlobStorageFailure({}, response);
        ASSERT_TRUE(failure.Error);
        ASSERT_TRUE(failure.Details.has_value());
        EXPECT_EQ(ValueOrFail(failure.Details).StatusCode, 404U);
    }

    const RequestFailure namespaced = DetermineBlobStorageFailure({},
        HttpResponse{404, {}, "<e:Error xmlns:e=\"urn:x\"><e:Code>BlobNotFound</e:Code></e:Error>"});
    EXPECT_EQ(namespaced.Error, std::error_code{BlobStorageErrorCode::BlobNotFound});
}
