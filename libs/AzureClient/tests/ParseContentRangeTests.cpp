#include "BlobRequestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <gtest/gtest.h>

using AVEVA::AzureClient::Tests::ValueOrFail;

using AVEVA::AzureClient::Private::ParseContentRange;

TEST(ParseContentRangeTests, ParsesNormalRangeWithTotal)
{
    const auto r = ParseContentRange("bytes 0-499/1234");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(ValueOrFail(r).Start, 0U);
    EXPECT_EQ(ValueOrFail(r).End, 499U);
    ASSERT_TRUE(ValueOrFail(r).Total.has_value());
    EXPECT_EQ(*ValueOrFail(r).Total, 1234U);
}

TEST(ParseContentRangeTests, ParsesAsteriskRangeWithTotal)
{
    const auto r = ParseContentRange("bytes */1234");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(ValueOrFail(r).Start, 0U);
    EXPECT_EQ(ValueOrFail(r).End, 0U);
    ASSERT_TRUE(ValueOrFail(r).Total.has_value());
    EXPECT_EQ(*ValueOrFail(r).Total, 1234U);
}

TEST(ParseContentRangeTests, ParsesRangeWithoutTotal)
{
    const auto r = ParseContentRange("bytes 100-199/*");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(ValueOrFail(r).Start, 100U);
    EXPECT_EQ(ValueOrFail(r).End, 199U);
    EXPECT_FALSE(ValueOrFail(r).Total.has_value());
}

TEST(ParseContentRangeTests, RejectsMissingPrefix)
{
    const auto r = ParseContentRange("0-99/100");
    EXPECT_FALSE(r.has_value());
}

TEST(ParseContentRangeTests, RejectsMalformedNumbers)
{
    EXPECT_FALSE(ParseContentRange("bytes abc-def/100").has_value());
    EXPECT_FALSE(ParseContentRange("bytes 0-99/1a0").has_value());
}

TEST(ParseContentRangeTests, RejectsEndBeforeStart)
{
    EXPECT_FALSE(ParseContentRange("bytes 200-100/1000").has_value());
}

TEST(ParseContentRangeTests, AcceptsExtraWhitespace)
{
    const auto r = ParseContentRange("  bytes   5-10/  20  ");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(ValueOrFail(r).Start, 5U);
    EXPECT_EQ(ValueOrFail(r).End, 10U);
    ASSERT_TRUE(ValueOrFail(r).Total.has_value());
    EXPECT_EQ(*ValueOrFail(r).Total, 20U);
}
