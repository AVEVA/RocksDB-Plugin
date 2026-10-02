#include "AzureIntegrationTestHelpers.hpp"

#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <ios>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace
{
    using AVEVA::AzureClient::IntegrationTests::AzureTestConfig;
    namespace Detail = AVEVA::AzureClient::IntegrationTests::Detail;
} // namespace

TEST(AzureIntegrationTestHelpersTests, EncodeBlockIdProducesExpectedBase64)
{
    EXPECT_EQ(AVEVA::AzureClient::IntegrationTests::EncodeBlockId("block-0001"), "YmxvY2stMDAwMQ==");
    EXPECT_EQ(Detail::Base64Encode(std::vector<unsigned char>{'A', 'B', 'C'}), "QUJD");
}

TEST(AzureIntegrationTestHelpersTests, UrlEncodeAndTokenBodyEscapeReservedCharacters)
{
    AzureTestConfig config{
        .AccountName = "account",
        .ServiceEndpoint = "https://account.blob.core.windows.net/",
        .TenantId = "tenant",
        .ClientId = "client id",
        .ClientSecret = "a+b&c=d",
    };

    EXPECT_EQ(Detail::UrlEncodeQueryValue("a b+/="), "a%20b%2B%2F%3D");
    EXPECT_EQ(Detail::BuildAadTokenRequestBody(config),
        "grant_type=client_credentials&client_id=client%20id&client_secret=a%2Bb%26c%3Dd&scope=https%3A%2F%2Fstorage."
        "azure.com%2F.default");
}

TEST(AzureIntegrationTestHelpersTests, JsonExtractionAndAccessTokenParsingHandleEscapesAndFailures)
{
    EXPECT_EQ(Detail::ExtractJsonStringField(R"({"access_token":"token\"value","other":"x"})", "access_token"),
        std::optional<std::string>{"token\"value"});
    EXPECT_EQ(Detail::ExtractJsonStringField(R"({"other":"x"})", "access_token"), std::nullopt);

    const AVEVA::HttpResponse success{200, {}, R"({"access_token":"abc123"})"};
    const AVEVA::HttpResponse failure{500, {}, R"({"access_token":"abc123"})"};
    EXPECT_EQ(Detail::ExtractAadAccessToken(success), std::optional<std::string>{"abc123"});
    EXPECT_EQ(Detail::ExtractAadAccessToken(failure), std::nullopt);
    EXPECT_EQ(Detail::ExtractAadAccessToken(success, std::make_error_code(std::errc::timed_out)), std::nullopt);
}

TEST(AzureIntegrationTestHelpersTests, GenerateUniqueContainerNameStaysLowercaseAndPrefixed)
{
    const std::string first = AVEVA::AzureClient::IntegrationTests::GenerateUniqueContainerName("aveva-it-");
    const std::string second = AVEVA::AzureClient::IntegrationTests::GenerateUniqueContainerName("aveva-it-");

    EXPECT_TRUE(first.starts_with("aveva-it-"));
    EXPECT_TRUE(second.starts_with("aveva-it-"));
    EXPECT_NE(first, second);
    EXPECT_EQ(first.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-"), std::string::npos);
}

TEST(AzureIntegrationTestHelpersTests, TemporaryCaBundleDestructorDeletesTheFile)
{
    const std::filesystem::path path = std::filesystem::temp_directory_path() / "azure-client-temp-ca.pem";
    {
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        file << "temporary";
    }
    ASSERT_TRUE(std::filesystem::exists(path));

    {
        AVEVA::AzureClient::IntegrationTests::TemporaryCaBundle const bundle{path.string()};
        EXPECT_EQ(bundle.Path, path.string());
    }

    EXPECT_FALSE(std::filesystem::exists(path));
}
