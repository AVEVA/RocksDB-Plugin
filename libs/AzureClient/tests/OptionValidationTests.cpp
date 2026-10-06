#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/ITokenCredential.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace
{
    using AVEVA::AzureClient::BlobClient;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;

    struct ValidationCase
    {
        std::string Name;
        // Starts the operation under test, reporting the completion's error (empty on success).
        std::function<void(FakeHttpClient&, std::function<void(std::error_code)>)> Start;
        std::errc Expected;
    };

    std::vector<ValidationCase> MakeCases()
    {
        std::vector<ValidationCase> cases;

        cases.push_back({"PageUploadUnalignedOffset",
            [](FakeHttpClient& http, auto report)
        {
            PageBlobClient client{http, MakeBlobClientOptions()};
            client.UploadPagesAsync(1,
                std::string(512, 'x'),
                [report](auto result)
            {
                report(result.has_value() ? std::error_code{} : result.error().Code);
            });
        },
            std::errc::invalid_argument});
        cases.push_back({"PageUploadUnalignedLength",
            [](FakeHttpClient& http, auto report)
        {
            PageBlobClient client{http, MakeBlobClientOptions()};
            client.UploadPagesAsync(0,
                std::string(100, 'x'),
                [report](auto result)
            {
                report(result.has_value() ? std::error_code{} : result.error().Code);
            });
        },
            std::errc::invalid_argument});
        cases.push_back({"PageUploadEmptyContent",
            [](FakeHttpClient& http, auto report)
        {
            PageBlobClient client{http, MakeBlobClientOptions()};
            client.UploadPagesAsync(0,
                std::string{},
                [report](auto result)
            {
                report(result.has_value() ? std::error_code{} : result.error().Code);
            });
        },
            std::errc::invalid_argument});
        cases.push_back({"PageClearUnalignedOffset",
            [](FakeHttpClient& http, auto report)
        {
            PageBlobClient client{http, MakeBlobClientOptions()};
            client.ClearPagesAsync(3,
                512,
                [report](auto result)
            {
                report(result.has_value() ? std::error_code{} : result.error().Code);
            });
        },
            std::errc::invalid_argument});
        cases.push_back({"PageClearZeroLength",
            [](FakeHttpClient& http, auto report)
        {
            PageBlobClient client{http, MakeBlobClientOptions()};
            client.ClearPagesAsync(0,
                0,
                [report](auto result)
            {
                report(result.has_value() ? std::error_code{} : result.error().Code);
            });
        },
            std::errc::invalid_argument});
        cases.push_back({"StageBlockInvalidBase64Id",
            [](FakeHttpClient& http, auto report)
        {
            BlockBlobClient client{http, MakeBlobClientOptions()};
            client.StageBlockAsync("not-base64!",
                "data",
                [report](auto result)
            {
                report(result.has_value() ? std::error_code{} : result.error().Code);
            });
        },
            std::errc::invalid_argument});
        cases.push_back({"CommitBlockListInconsistentIdLengths",
            [](FakeHttpClient& http, auto report)
        {
            BlockBlobClient client{http, MakeBlobClientOptions()};
            const std::vector<std::string> ids{"YmxvY2swMQ==", "YmI="};
            client.CommitBlockListAsync(ids,
                [report](auto result)
            {
                report(result.has_value() ? std::error_code{} : result.error().Code);
            });
        },
            std::errc::invalid_argument});
        cases.push_back({"BlobClientSnapshotAndVersionIdTogether",
            [](FakeHttpClient& http, auto report)
        {
            // Rejected at construction; the throw is translated into an error code for the table.
            auto options = MakeBlobClientOptions();
            options.Snapshot = "2020-01-01T00:00:00.0000000Z";
            options.VersionId = "2020-01-02T00:00:00.0000000Z";
            try
            {
                BlobClient client{http, options};
                report({});
            }
            catch (const std::invalid_argument&)
            {
                report(std::make_error_code(std::errc::invalid_argument));
            }
        },
            std::errc::invalid_argument});

        return cases;
    }
} // namespace

class OptionValidationTests : public ::testing::TestWithParam<ValidationCase>
{
};

TEST_P(OptionValidationTests, RejectsInvalidOptionsBeforeSendingAnyRequest)
{
    FakeHttpClient httpClient;
    std::optional<std::error_code> error;
    GetParam().Start(httpClient,
        [&](std::error_code code)
    {
        error = code;
    });
    httpClient.Poll();

    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(*error, GetParam().Expected);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

INSTANTIATE_TEST_SUITE_P(Matrix,
    OptionValidationTests,
    ::testing::ValuesIn(MakeCases()),
    [](const ::testing::TestParamInfo<ValidationCase>& info)
{
    return info.param.Name;
});

namespace
{
    struct TokenTransportCase
    {
        std::string Name;
        std::string Endpoint;
        bool UseTokenCredential;
        bool UseBearerToken;
        bool UseSharedKey;
        bool UseSas;
        bool ExpectThrow;
    };

    std::vector<TokenTransportCase> MakeTokenTransportCases()
    {
        return {
            {"HttpTokenCredentialThrows", "http://account.blob.core.windows.net", true, false, false, false, true},
            {"HttpBearerTokenThrows", "http://account.blob.core.windows.net", false, true, false, false, true},
            {"UpperCaseHttpBearerThrows", "HTTP://account.blob.core.windows.net", false, true, false, false, true},
            {"HttpLookalikeHostThrows", "http://localhost.evil.example/path", false, true, false, false, true},
            {"HttpUserinfoHostThrows", "http://127.0.0.1@evil.example/", false, true, false, false, true},
            {"LoopbackIpv4TokenOk", "http://127.0.0.1:10000/devstoreaccount1", true, false, false, false, false},
            {"LocalhostBearerOk", "http://localhost:10000/devstoreaccount1", false, true, false, false, false},
            {"LoopbackIpv6BearerOk", "http://[::1]:10000/devstoreaccount1", false, true, false, false, false},
            {"HttpsTokenCredentialOk", "https://account.blob.core.windows.net", true, false, false, false, false},
            {"HttpsBearerOk", "https://account.blob.core.windows.net", false, true, false, false, false},
            {"HttpSharedKeyThrows", "http://account.blob.core.windows.net", false, false, true, false, true},
            {"HttpSasThrows", "http://account.blob.core.windows.net", false, false, false, true, true},
        };
    }

    template <class TOptions> void ApplyCase(TOptions& options, const TokenTransportCase& testCase)
    {
        options.ServiceEndpoint = testCase.Endpoint;
        options.SasToken.clear();
        if (testCase.UseTokenCredential)
        {
            options.TokenCredential = std::make_shared<AVEVA::AzureClient::StaticTokenCredential>("token");
        }
        if (testCase.UseBearerToken)
        {
            options.BearerToken = "token";
        }
        if (testCase.UseSharedKey)
        {
            options.SharedKey.AccountName = "account";
            options.SharedKey.AccountKey = "a2V5";
        }
        if (testCase.UseSas)
        {
            options.SasToken = "sv=2023-11-03&sig=x";
        }
    }
} // namespace

class TokenTransportTests : public ::testing::TestWithParam<TokenTransportCase>
{
};

TEST_P(TokenTransportTests, TokenCredentialsRequireHttpsUnlessLoopback)
{
    FakeHttpClient httpClient;
    const TokenTransportCase& testCase = GetParam();

    auto blobOptions = MakeBlobClientOptions();
    ApplyCase(blobOptions, testCase);
    auto containerOptions = AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions();
    ApplyCase(containerOptions, testCase);
    auto serviceOptions = AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions();
    ApplyCase(serviceOptions, testCase);

    if (testCase.ExpectThrow)
    {
        EXPECT_THROW((BlobClient{httpClient, blobOptions}), std::invalid_argument);
        EXPECT_THROW((AVEVA::AzureClient::BlobContainerClient{httpClient, containerOptions}), std::invalid_argument);
        EXPECT_THROW((AVEVA::AzureClient::BlobServiceClient{httpClient, serviceOptions}), std::invalid_argument);
    }
    else
    {
        EXPECT_NO_THROW((BlobClient{httpClient, blobOptions}));
        EXPECT_NO_THROW((AVEVA::AzureClient::BlobContainerClient{httpClient, containerOptions}));
        EXPECT_NO_THROW((AVEVA::AzureClient::BlobServiceClient{httpClient, serviceOptions}));
    }
}

INSTANTIATE_TEST_SUITE_P(Matrix,
    TokenTransportTests,
    ::testing::ValuesIn(MakeTokenTransportCases()),
    [](const ::testing::TestParamInfo<TokenTransportCase>& info)
{
    return info.param.Name;
});
