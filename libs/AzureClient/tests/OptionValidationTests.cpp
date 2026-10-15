// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>

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

namespace {
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::PageBlobClient;

struct ValidationCase {
    std::string Name;
    // Starts the operation under test, reporting the completion's error (empty on success).
    std::function<void(FakeHttpClient&, std::function<void(std::error_code)>)> Start;
    std::errc Expected;
};

std::vector<ValidationCase> MakeCases() {
    std::vector<ValidationCase> cases;

    cases.push_back({"PageUploadUnalignedOffset",
                     [](FakeHttpClient& http, auto report) {
                         PageBlobClient client{http, MakeBlobClientOptions()};
                         client.UploadPagesAsync(1, std::string(512, 'x'), [report](auto result) {
                             report(result.has_value() ? std::error_code{} : result.error().Code);
                         });
                     },
                     std::errc::invalid_argument});
    cases.push_back({"PageUploadUnalignedLength",
                     [](FakeHttpClient& http, auto report) {
                         PageBlobClient client{http, MakeBlobClientOptions()};
                         client.UploadPagesAsync(0, std::string(100, 'x'), [report](auto result) {
                             report(result.has_value() ? std::error_code{} : result.error().Code);
                         });
                     },
                     std::errc::invalid_argument});
    cases.push_back({"PageUploadEmptyContent",
                     [](FakeHttpClient& http, auto report) {
                         PageBlobClient client{http, MakeBlobClientOptions()};
                         client.UploadPagesAsync(0, std::string{}, [report](auto result) {
                             report(result.has_value() ? std::error_code{} : result.error().Code);
                         });
                     },
                     std::errc::invalid_argument});

    return cases;
}
} // namespace

class OptionValidationTests : public ::testing::TestWithParam<ValidationCase> {};

TEST_P(OptionValidationTests, RejectsInvalidOptionsBeforeSendingAnyRequest) {
    FakeHttpClient httpClient;
    std::optional<std::error_code> error;
    GetParam().Start(httpClient, [&](std::error_code code) { error = code; });
    httpClient.Poll();

    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(*error, GetParam().Expected);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

INSTANTIATE_TEST_SUITE_P(Matrix, OptionValidationTests, ::testing::ValuesIn(MakeCases()),
                         [](const ::testing::TestParamInfo<ValidationCase>& paramInfo) {
                             return paramInfo.param.Name;
                         });

namespace {
struct TokenTransportCase {
    std::string Name;
    std::string Endpoint;
    bool UseTokenCredential;
    bool UseSharedKey;
    bool UseSas;
    bool ExpectThrow;
};

std::vector<TokenTransportCase> MakeTokenTransportCases() {
    return {
        {"HttpTokenCredentialThrows", "http://account.blob.core.windows.net", true, false, false, true},
        {"HttpTokenCredentialEndpointThrows", "http://account.blob.core.windows.net", true, false, false, true},
        {"UpperCaseHttpTokenThrows", "HTTP://account.blob.core.windows.net", true, false, false, true},
        {"HttpLookalikeHostThrows", "http://localhost.evil.example/path", true, false, false, true},
        {"HttpUserinfoHostThrows", "http://127.0.0.1@evil.example/", true, false, false, true},
        {"LoopbackIpv4TokenOk", "http://127.0.0.1:10000/devstoreaccount1", true, false, false, false},
        {"LocalhostTokenOk", "http://localhost:10000/devstoreaccount1", true, false, false, false},
        {"LoopbackIpv6TokenOk", "http://[::1]:10000/devstoreaccount1", true, false, false, false},
        {"HttpsTokenCredentialOk", "https://account.blob.core.windows.net", true, false, false, false},
        {"HttpsTokenOk", "https://account.blob.core.windows.net", true, false, false, false},
        {"HttpSharedKeyThrows", "http://account.blob.core.windows.net", false, true, false, true},
        {"HttpSasThrows", "http://account.blob.core.windows.net", false, false, true, true},
    };
}

using StaticBlobClient =
    AVEVA::AzureClient::BasicBlobClient<FakeHttpClient, AVEVA::AzureClient::Tests::StaticTokenCredential>;
using StaticContainerClient =
    AVEVA::AzureClient::BasicBlobContainerClient<FakeHttpClient, AVEVA::AzureClient::Tests::StaticTokenCredential>;
using StaticServiceClient =
    AVEVA::AzureClient::BasicBlobServiceClient<FakeHttpClient, AVEVA::AzureClient::Tests::StaticTokenCredential>;

template <class TOptions> void ApplyCase(TOptions& options, const TokenTransportCase& testCase) {
    options.ServiceEndpoint = testCase.Endpoint;
    options.AllowInsecureHttp = true;
    options.SasToken.clear();
    if (testCase.UseSharedKey) {
        options.SharedKey.AccountName = "account";
        options.SharedKey.AccountKey = "a2V5";
    }
    if (testCase.UseSas) {
        options.SasToken = "sv=2023-11-03&sig=x";
    }
}
} // namespace

class TokenTransportTests : public ::testing::TestWithParam<TokenTransportCase> {};

TEST_P(TokenTransportTests, TokenCredentialsRequireHttpsUnlessLoopback) {
    FakeHttpClient httpClient;
    const TokenTransportCase& testCase = GetParam();
    const auto credential = testCase.UseTokenCredential
                                ? std::make_shared<AVEVA::AzureClient::Tests::StaticTokenCredential>("token")
                                : nullptr;

    auto blobOptions = MakeBlobClientOptions();
    ApplyCase(blobOptions, testCase);
    auto containerOptions = AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions();
    ApplyCase(containerOptions, testCase);
    auto serviceOptions = AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions();
    ApplyCase(serviceOptions, testCase);

    if (testCase.ExpectThrow) {
        EXPECT_THROW((StaticBlobClient{httpClient, blobOptions, credential}), std::invalid_argument);
        EXPECT_THROW((StaticContainerClient{httpClient, containerOptions, credential}), std::invalid_argument);
        EXPECT_THROW((StaticServiceClient{httpClient, serviceOptions, credential}), std::invalid_argument);
    } else {
        EXPECT_NO_THROW((StaticBlobClient{httpClient, blobOptions, credential}));
        EXPECT_NO_THROW((StaticContainerClient{httpClient, containerOptions, credential}));
        EXPECT_NO_THROW((StaticServiceClient{httpClient, serviceOptions, credential}));
    }
}

INSTANTIATE_TEST_SUITE_P(Matrix, TokenTransportTests, ::testing::ValuesIn(MakeTokenTransportCases()),
                         [](const ::testing::TestParamInfo<TokenTransportCase>& paramInfo) {
                             return paramInfo.param.Name;
                         });

TEST(InsecureHttpTests, HttpEndpointIsRejectedUnlessExplicitlyAllowed) {
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.SasToken.clear();
    for (const char* endpoint : {"http://127.0.0.1:10000/devstoreaccount1", "HTTP://account.blob.core.windows.net"}) {
        options.ServiceEndpoint = endpoint;
        options.AllowInsecureHttp = false;
        EXPECT_THROW((BlobClient{httpClient, options}), std::invalid_argument) << endpoint;
        options.AllowInsecureHttp = true;
        EXPECT_NO_THROW((BlobClient{httpClient, options})) << endpoint;
    }
}

TEST(InsecureHttpTests, HttpsEndpointNeedsNoOptIn) {
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.ServiceEndpoint = "https://account.blob.core.windows.net";
    ASSERT_FALSE(options.AllowInsecureHttp);
    EXPECT_NO_THROW((BlobClient{httpClient, options}));
}

TEST(SasTokenValidationTests, ControlCharactersFragmentsAndEmbeddedQuestionMarksAreRejected) {
    FakeHttpClient httpClient;
    for (const std::string token : {std::string{"sv=1&sig=a\r\nx-ms-evil: 1"}, std::string{"sv=1#frag"},
                                    std::string{"sv=1?sig=x"}, std::string{"sv=1 &sig=x"}, std::string{"sv=1\0x", 6}}) {
        auto options = MakeBlobClientOptions();
        options.SasToken = token;
        EXPECT_THROW((BlobClient{httpClient, options}), std::invalid_argument) << token;
    }
}

TEST(SasTokenValidationTests, MalformedPairsAreRejected) {
    FakeHttpClient httpClient;
    for (const std::string token : {std::string{"sv"}, std::string{"sv=1&&sig=x"}, std::string{"=1&sig=x"},
                                    std::string{"sv=1&sig"}, std::string{"sv=1&"}}) {
        auto options = MakeBlobClientOptions();
        options.SasToken = token;
        EXPECT_THROW((BlobClient{httpClient, options}), std::invalid_argument) << token;
    }
}

TEST(SasTokenValidationTests, OrdinaryTokenWithOneLeadingQuestionMarkIsAccepted) {
    FakeHttpClient httpClient;
    auto options = MakeBlobClientOptions();
    options.SasToken = "?sv=2023-11-03&sig=abc%2Bdef%3D";
    EXPECT_NO_THROW((BlobClient{httpClient, options}));
}
