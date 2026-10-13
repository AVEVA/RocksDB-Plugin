// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/TokenCredential.hpp"
#include "FakeHttpClient.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <ios>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {
using AVEVA::HttpMethod;
using AVEVA::HttpRequest;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::AccessToken;
using AVEVA::AzureClient::AssertionCompletion;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::ClientAssertionCredentialOptions;
using AVEVA::AzureClient::ClientSecretCredentialOptions;
using AVEVA::AzureClient::ManagedIdentityCredentialOptions;
using AVEVA::AzureClient::WorkloadIdentityCredentialOptions;
using AVEVA::AzureClient::Tests::AzurePipelinesCredential;
using AVEVA::AzureClient::Tests::ClientAssertionCredential;
using AVEVA::AzureClient::Tests::ClientSecretCredential;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::ManagedIdentityCredential;
using AVEVA::AzureClient::Tests::WorkloadIdentityCredential;
using Clock = std::chrono::system_clock;

[[nodiscard]] std::vector<std::string> StorageScope() { return {"https://storage.azure.com/.default"}; }

struct TokenResult {
    std::error_code Error;
    AccessToken Token;
};

template <class TCred>
[[nodiscard]] TokenResult GetToken(FakeHttpClient& httpClient, TCred& credential,
                                   std::vector<std::string> scopes = StorageScope()) {
    std::optional<TokenResult> result;
    credential.GetTokenAsync(std::move(scopes), [&](std::error_code error, AccessToken token) {
        result = TokenResult{.Error = error, .Token = std::move(token)};
    });
    EXPECT_FALSE(result.has_value()) << "completion must not run inside GetTokenAsync";
    httpClient.Poll();
    EXPECT_TRUE(result.has_value());
    return result.value_or(TokenResult{});
}

// A token body whose "expires_on" (Unix seconds, as a string) is `lifetimeSeconds` from now.
[[nodiscard]] std::string ExpiresOnBody(std::string_view token, long long lifetimeSeconds) {
    const long long now = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
    return R"({"access_token":")" + std::string{token} + R"(","expires_on":")" + std::to_string(now + lifetimeSeconds) +
           R"("})";
}

[[nodiscard]] HttpResponse Json(unsigned int status, std::string body) {
    return HttpResponse{status, {}, std::move(body)};
}

[[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name) {
    return FakeHttpClient::FindHeaderValue(request, name);
}

[[nodiscard]] ClientSecretCredentialOptions SecretOptions() {
    ClientSecretCredentialOptions options;
    options.TenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
    options.ClientId = "client id";
    options.ClientSecret = "s3cr&t=";
    options.Retry.MaxRetries = 0;
    return options;
}

[[nodiscard]] WorkloadIdentityCredentialOptions WorkloadOptions(std::string tenantId, std::string clientId,
                                                                std::string tokenFile) {
    WorkloadIdentityCredentialOptions options;
    options.TenantId = std::move(tenantId);
    options.ClientId = std::move(clientId);
    options.TokenFilePath = std::move(tokenFile);
    options.Retry.MaxRetries = 0;
    return options;
}

[[nodiscard]] ManagedIdentityCredentialOptions ManagedOptions(std::string clientId, std::string resourceId,
                                                              std::string identityEndpoint = {},
                                                              std::string identityHeader = {}) {
    ManagedIdentityCredentialOptions options;
    options.ClientId = std::move(clientId);
    options.ResourceId = std::move(resourceId);
    options.IdentityEndpoint = std::move(identityEndpoint);
    options.IdentityHeader = std::move(identityHeader);
    options.Retry.MaxRetries = 0;
    return options;
}
} // namespace

TEST(CredentialsTests, ClientSecretCredentialPostsClientCredentialsGrant) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(200, R"({"token_type":"Bearer","expires_in":3599,"access_token":"tok-1"})"));
    ClientSecretCredential credential{httpClient, SecretOptions()};

    const auto before = Clock::now();
    const TokenResult result =
        GetToken(httpClient, credential, {"https://storage.azure.com/.default", "offline_access"});
    ASSERT_FALSE(result.Error) << result.Error.message();
    EXPECT_EQ(result.Token.Token, "tok-1");
    EXPECT_GE(result.Token.ExpiresOn, before + std::chrono::seconds{3599});
    EXPECT_LE(result.Token.ExpiresOn, Clock::now() + std::chrono::seconds{3599});

    const HttpRequest& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Post);
    EXPECT_EQ(request.GetUrl(),
              "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/v2.0/token");
    EXPECT_EQ(Header(request, "Content-Type"), "application/x-www-form-urlencoded");
    EXPECT_EQ(FakeHttpClient::BodyAsString(request),
              "grant_type=client_credentials&client_id=client%20id&client_secret=s3cr%26t%3D"
              "&scope=https%3A%2F%2Fstorage.azure.com%2F.default%20offline_access");
}

TEST(CredentialsTests, ClientSecretCredentialHonoursAuthorityHost) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(200, R"({"expires_in":"60","access_token":"tok"})"));
    ClientSecretCredentialOptions options = SecretOptions();
    options.AuthorityHost = "https://login.microsoftonline.us";
    ClientSecretCredential credential{httpClient, options};

    EXPECT_FALSE(GetToken(httpClient, credential).Error);
    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
              "https://login.microsoftonline.us/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/v2.0/token");
}

TEST(CredentialsTests, InvalidConfigurationFailsWithoutSendingARequest) {
    FakeHttpClient httpClient;
    const auto expectInvalid = [&](auto& credential, std::vector<std::string> scopes = StorageScope()) {
        EXPECT_EQ(GetToken(httpClient, credential, std::move(scopes)).Error,
                  std::make_error_code(std::errc::invalid_argument));
    };

    for (const std::string tenant : {"", "contoso.com/../evil", "tenant?x=1"}) {
        ClientSecretCredentialOptions options = SecretOptions();
        options.TenantId = tenant;
        ClientSecretCredential credential{httpClient, options};
        expectInvalid(credential);
    }
    ClientSecretCredentialOptions noSecret = SecretOptions();
    noSecret.ClientSecret.clear();
    ClientSecretCredential noSecretCredential{httpClient, noSecret};
    expectInvalid(noSecretCredential);
    ClientSecretCredential valid{httpClient, SecretOptions()};
    expectInvalid(valid, {});

    WorkloadIdentityCredential noFile{httpClient, WorkloadOptions("t", "c", {})};
    expectInvalid(noFile);

    ManagedIdentityCredential managed{httpClient, ManagedIdentityCredentialOptions{}};
    expectInvalid(managed, {"a", "b"});
    ManagedIdentityCredential both{httpClient, ManagedOptions("c", "/subscriptions/x")};
    expectInvalid(both);
    ManagedIdentityCredential missingHeader{httpClient, ManagedOptions({}, {}, "http://localhost:8081/msi/token")};
    expectInvalid(missingHeader);
    ManagedIdentityCredential remotePlainHttp{httpClient,
                                              ManagedOptions({}, {}, "http://example.com/msi/token", "secret")};
    expectInvalid(remotePlainHttp);
    for (const std::string endpoint :
         {"http://127.evil.com/msi/token", "http://169.254.attacker.nip.io/msi/token",
          "http://127.0.0.1@evil.example/msi/token", "http://localhost@evil.example/msi/token",
          "http://localhost.evil.com/msi/token", "http://127.0.0.1.evil.com/msi/token", "http://[::2]/msi/token",
          "http://169.255.0.1/msi/token", "https://user@login.example/msi/token", "ftp://localhost/msi/token"}) {
        ManagedIdentityCredential bypass{httpClient, ManagedOptions({}, {}, endpoint, "secret")};
        expectInvalid(bypass);
    }

    EXPECT_TRUE(httpClient.Requests().empty());
}

TEST(CredentialsTests, TokenEndpointFailuresMapToErrorCodes) {
    FakeHttpClient httpClient;
    ClientSecretCredential credential{httpClient, SecretOptions()};

    httpClient.EnqueueResponse(Json(401, R"({"error":"invalid_client"})"));
    EXPECT_EQ(GetToken(httpClient, credential).Error, std::error_code{BlobStorageErrorCode::AuthenticationFailed});

    for (const std::string body : {"not json", R"({"expires_in":3600})", R"({"access_token":"t"})",
                                   R"({"access_token":"t","expires_in":"soon"})"}) {
        httpClient.EnqueueResponse(Json(200, body));
        const TokenResult result = GetToken(httpClient, credential);
        EXPECT_EQ(result.Error, std::error_code{BlobStorageErrorCode::InvalidResponse}) << body;
        EXPECT_TRUE(result.Token.Token.empty());
    }

    const auto transport = std::make_error_code(std::errc::connection_refused);
    httpClient.EnqueueResponse(HttpResponse{}, transport);
    EXPECT_EQ(GetToken(httpClient, credential).Error, transport);
}

TEST(CredentialsTests, WorkloadIdentityCredentialSendsFederatedTokenAsClientAssertion) {
    const std::filesystem::path tokenFile = std::filesystem::temp_directory_path() / "azc-federated-token.txt";
    {
        std::ofstream file{tokenFile, std::ios::binary | std::ios::trunc};
        file << "eyJhbGciOi.payload.sig\n";
    }

    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(200, R"({"expires_in":3600,"access_token":"wi-token"})"));
    WorkloadIdentityCredential credential{httpClient,
                                          WorkloadOptions("contoso.onmicrosoft.com", "app", tokenFile.string())};

    const TokenResult result = GetToken(httpClient, credential);
    ASSERT_FALSE(result.Error) << result.Error.message();
    EXPECT_EQ(result.Token.Token, "wi-token");
    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
              "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token");
    EXPECT_EQ(FakeHttpClient::BodyAsString(httpClient.LastRequest()),
              "grant_type=client_credentials&client_id=app"
              "&client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"
              "&client_assertion=eyJhbGciOi.payload.sig&scope=https%3A%2F%2Fstorage.azure.com%2F.default");

    std::filesystem::remove(tokenFile);
    EXPECT_EQ(GetToken(httpClient, credential).Error, std::make_error_code(std::errc::no_such_file_or_directory));
    EXPECT_EQ(httpClient.Requests().size(), 1U);
}

[[nodiscard]] ClientAssertionCredentialOptions AssertionOptions(std::function<void(AssertionCompletion)> provider) {
    ClientAssertionCredentialOptions options;
    options.TenantId = "contoso.onmicrosoft.com";
    options.ClientId = "app";
    options.AssertionProvider = std::move(provider);
    options.Retry.MaxRetries = 0;
    return options;
}

TEST(CredentialsTests, ClientAssertionCredentialSendsTheProvidedAssertion) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(200, R"({"expires_in":3600,"access_token":"ca-token"})"));
    int calls = 0;
    ClientAssertionCredential credential{httpClient, AssertionOptions([&calls](AssertionCompletion done) {
                                             ++calls;
                                             done(std::error_code{}, "a.b+c");
                                         })};

    const TokenResult result = GetToken(httpClient, credential);
    ASSERT_FALSE(result.Error) << result.Error.message();
    EXPECT_EQ(result.Token.Token, "ca-token");
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
              "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token");
    EXPECT_EQ(FakeHttpClient::BodyAsString(httpClient.LastRequest()),
              "grant_type=client_credentials&client_id=app"
              "&client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"
              "&client_assertion=a.b%2Bc&scope=https%3A%2F%2Fstorage.azure.com%2F.default");
}

TEST(CredentialsTests, ClientAssertionCredentialRequestsAFreshAssertionEachTime) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = Json(200, R"({"expires_in":3600,"access_token":"t"})");
    int calls = 0;
    ClientAssertionCredential credential{httpClient, AssertionOptions([&calls](AssertionCompletion done) {
                                             done(std::error_code{}, "assertion" + std::to_string(++calls));
                                         })};

    ASSERT_FALSE(GetToken(httpClient, credential).Error);
    EXPECT_NE(FakeHttpClient::BodyAsString(httpClient.LastRequest()).find("client_assertion=assertion1&"),
              std::string::npos);
    ASSERT_FALSE(GetToken(httpClient, credential).Error);
    EXPECT_NE(FakeHttpClient::BodyAsString(httpClient.LastRequest()).find("client_assertion=assertion2&"),
              std::string::npos);
}

TEST(CredentialsTests, ClientAssertionProviderErrorIsPassedThroughWithoutARequest) {
    FakeHttpClient httpClient;
    const auto providerError = make_error_code(BlobStorageErrorCode::AuthenticationFailed);
    ClientAssertionCredential credential{
        httpClient, AssertionOptions([providerError](AssertionCompletion done) { done(providerError, {}); })};

    EXPECT_EQ(GetToken(httpClient, credential).Error, providerError);
    EXPECT_EQ(httpClient.Requests().size(), 0U);
}

TEST(CredentialsTests, ClientAssertionInvalidConfigurationFailsWithoutARequest) {
    FakeHttpClient httpClient;
    const auto provider = [](AssertionCompletion done) { done(std::error_code{}, "assertion"); };
    const auto invalid = std::make_error_code(std::errc::invalid_argument);

    ClientAssertionCredential noProvider{httpClient, AssertionOptions({})};
    EXPECT_EQ(GetToken(httpClient, noProvider).Error, invalid);

    auto badTenant = AssertionOptions(provider);
    badTenant.TenantId = "../evil";
    ClientAssertionCredential badTenantCredential{httpClient, badTenant};
    EXPECT_EQ(GetToken(httpClient, badTenantCredential).Error, invalid);

    ClientAssertionCredential emptyAssertion{
        httpClient, AssertionOptions([](AssertionCompletion done) { done(std::error_code{}, {}); })};
    EXPECT_EQ(GetToken(httpClient, emptyAssertion).Error, invalid);

    ClientAssertionCredential noScopes{httpClient, AssertionOptions(provider)};
    EXPECT_EQ(GetToken(httpClient, noScopes, {}).Error, invalid);
    EXPECT_EQ(httpClient.Requests().size(), 0U);

    auto badAuthority = AssertionOptions(provider);
    badAuthority.AuthorityHost = "http://login.example/";
    EXPECT_THROW((ClientAssertionCredential{httpClient, badAuthority}), std::invalid_argument);
}
TEST(CredentialsTests, ManagedIdentityCredentialUsesImds) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(
        200,
        R"({"access_token":"mi","expires_in":"86399","expires_on":"1700000000","resource":"https://storage.azure.com"})"));
    httpClient.EnqueueResponse(Json(200, ExpiresOnBody("mi2", 7200)));
    ManagedIdentityCredential system{httpClient, ManagedIdentityCredentialOptions{}};

    const TokenResult result = GetToken(httpClient, system);
    ASSERT_FALSE(result.Error) << result.Error.message();
    EXPECT_EQ(result.Token.Token, "mi");
    EXPECT_GT(result.Token.ExpiresOn, Clock::now() + std::chrono::hours{23});
    const HttpRequest& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Get);
    EXPECT_EQ(request.GetUrl(), "http://169.254.169.254/metadata/identity/oauth2/"
                                "token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com");
    EXPECT_EQ(Header(request, "Metadata"), "true");

    ManagedIdentityCredential user{httpClient, ManagedOptions({}, "/subscriptions/s/rg/id")};
    const TokenResult userResult = GetToken(httpClient, user, {"https://storage.azure.com"});
    ASSERT_FALSE(userResult.Error);
    EXPECT_GT(userResult.Token.ExpiresOn, Clock::now() + std::chrono::minutes{119});
    EXPECT_LE(userResult.Token.ExpiresOn, Clock::now() + std::chrono::minutes{121});
    EXPECT_EQ(httpClient.LastRequest().GetUrl(), "http://169.254.169.254/metadata/identity/oauth2/"
                                                 "token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com"
                                                 "&msi_res_id=%2Fsubscriptions%2Fs%2Frg%2Fid");
}

TEST(CredentialsTests, ManagedIdentityCredentialUsesAppServiceEndpointWhenConfigured) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(200, ExpiresOnBody("as", 3600)));
    ManagedIdentityCredential credential{
        httpClient, ManagedOptions("uami", {}, "http://localhost:8081/msi/token", "secret-header")};

    ASSERT_FALSE(GetToken(httpClient, credential).Error);
    const HttpRequest& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetUrl(), "http://localhost:8081/msi/"
                                "token?api-version=2019-08-01&resource=https%3A%2F%2Fstorage.azure.com&client_id=uami");
    EXPECT_EQ(Header(request, "X-IDENTITY-HEADER"), "secret-header");
    EXPECT_EQ(Header(request, "Metadata"), "");
}

TEST(CredentialsTests, ExpiresInOutOfRangeIsRejectedAndNormalAccepted) {
    FakeHttpClient httpClient;
    ClientSecretCredential credential{httpClient, SecretOptions()};
    for (const std::string value : {"0", "-1"}) {
        httpClient.EnqueueResponse(Json(200, R"({"access_token":"t","expires_in":)" + value + "}"));
        const TokenResult result = GetToken(httpClient, credential);
        EXPECT_EQ(result.Error, std::error_code{BlobStorageErrorCode::InvalidResponse}) << value;
    }
    httpClient.EnqueueResponse(Json(200, R"({"access_token":"t","expires_in":3600})"));
    EXPECT_FALSE(GetToken(httpClient, credential).Error);
}

TEST(CredentialsTests, ExpiresInBeyondMaximumIsClampedNotRejected) {
    FakeHttpClient httpClient;
    ClientSecretCredential credential{httpClient, SecretOptions()};
    for (const std::string value : {"86401", "9223372036854775807"}) {
        httpClient.EnqueueResponse(Json(200, R"({"access_token":"t","expires_in":)" + value + "}"));
        const TokenResult result = GetToken(httpClient, credential);
        EXPECT_FALSE(result.Error) << value;
        EXPECT_LE(result.Token.ExpiresOn, Clock::now() + std::chrono::hours{24});
    }
}

TEST(CredentialsTests, ExpiresOnOutOfRangeIsRejectedAndNormalAccepted) {
    FakeHttpClient httpClient;
    ManagedIdentityCredential credential{httpClient, ManagedIdentityCredentialOptions{}};
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
    for (const std::string& value : std::vector<std::string>{"0", std::to_string(now - 10)}) {
        httpClient.EnqueueResponse(Json(200, R"({"access_token":"t","expires_on":")" + value + R"("})"));
        const TokenResult result = GetToken(httpClient, credential);
        EXPECT_EQ(result.Error, std::error_code{BlobStorageErrorCode::InvalidResponse}) << value;
    }
    httpClient.EnqueueResponse(Json(200, ExpiresOnBody("t", 3600)));
    const TokenResult valid = GetToken(httpClient, credential);
    EXPECT_FALSE(valid.Error);
    EXPECT_GT(valid.Token.ExpiresOn, Clock::now() + std::chrono::minutes{59});
}

TEST(CredentialsTests, ExpiresOnBeyondMaximumIsClampedNotRejected) {
    FakeHttpClient httpClient;
    ManagedIdentityCredential credential{httpClient, ManagedIdentityCredentialOptions{}};
    httpClient.EnqueueResponse(Json(200, ExpiresOnBody("t", 24 * 60 * 60 + 5)));
    const TokenResult result = GetToken(httpClient, credential);
    EXPECT_FALSE(result.Error);
    EXPECT_LE(result.Token.ExpiresOn, Clock::now() + std::chrono::hours{24});
}

TEST(CredentialsTests, TransientTokenEndpointFailuresAreRetried) {
    FakeHttpClient httpClient;
    ClientSecretCredentialOptions options = SecretOptions();
    options.Retry = {
        .MaxRetries = 2, .InitialDelay = std::chrono::milliseconds{1}, .MaxDelay = std::chrono::milliseconds{2}};
    ClientSecretCredential credential{httpClient, options};

    httpClient.EnqueueResponse(Json(503, "busy"));
    httpClient.EnqueueResponse(Json(200, R"({"access_token":"ok","expires_in":3600})"));
    std::optional<TokenResult> result;
    credential.GetTokenAsync(StorageScope(), [&](std::error_code error, AccessToken token) {
        result = TokenResult{.Error = error, .Token = std::move(token)};
    });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));
    EXPECT_FALSE(result->Error);
    EXPECT_EQ(result->Token.Token, "ok");
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}

TEST(CredentialsTests, BadRequestTokenFailureIsNotRetried) {
    FakeHttpClient httpClient;
    ClientSecretCredentialOptions options = SecretOptions();
    options.Retry = {
        .MaxRetries = 3, .InitialDelay = std::chrono::milliseconds{1}, .MaxDelay = std::chrono::milliseconds{2}};
    ClientSecretCredential credential{httpClient, options};

    httpClient.EnqueueResponse(Json(400, R"({"error":"invalid_request"})"));
    EXPECT_EQ(GetToken(httpClient, credential).Error, std::error_code{BlobStorageErrorCode::AuthenticationFailed});
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(CredentialsTests, ManagedIdentityRetriesNotFoundAndGone) {
    FakeHttpClient httpClient;
    ManagedIdentityCredentialOptions options = ManagedOptions({}, {});
    options.Retry = {
        .MaxRetries = 3, .InitialDelay = std::chrono::milliseconds{1}, .MaxDelay = std::chrono::milliseconds{2}};
    ManagedIdentityCredential credential{httpClient, options};

    httpClient.EnqueueResponse(Json(404, "nf"));
    httpClient.EnqueueResponse(Json(410, "gone"));
    httpClient.EnqueueResponse(Json(200, ExpiresOnBody("mi", 3600)));
    std::optional<TokenResult> result;
    credential.GetTokenAsync(StorageScope(), [&](std::error_code error, AccessToken token) {
        result = TokenResult{.Error = error, .Token = std::move(token)};
    });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));
    EXPECT_FALSE(result->Error);
    EXPECT_EQ(httpClient.RequestCount(), 3U);
}

TEST(CredentialsTests, InvalidAuthorityHostThrowsFromConstructor) {
    FakeHttpClient httpClient;
    for (const std::string host : {"http://login.microsoftonline.com", "https://login.microsoftonline.com?x=1",
                                   "https://login.microsoftonline.com#f", "https://login.microsoftonline.com/path",
                                   "https://login.microsoftonline.com@evil.example", "https://a:b:c",
                                   "https://host:99999", "https://[::1", "https://user:pw@login.microsoftonline.com",
                                   "https://login.microsoftonline.com\\@evil.example", "https://", ""}) {
        ClientSecretCredentialOptions options = SecretOptions();
        options.AuthorityHost = host;
        EXPECT_THROW((ClientSecretCredential{httpClient, options}), std::invalid_argument) << host;
        WorkloadIdentityCredentialOptions workload = WorkloadOptions("t", "c", "f");
        workload.AuthorityHost = host;
        EXPECT_THROW((WorkloadIdentityCredential{httpClient, workload}), std::invalid_argument) << host;
    }
    EXPECT_NO_THROW((ClientSecretCredential{httpClient, SecretOptions()}));
    for (const std::string host : {"https://login.microsoftonline.com:8443", "HTTPS://LOGIN.MicrosoftOnline.com"}) {
        ClientSecretCredentialOptions options = SecretOptions();
        options.AuthorityHost = host;
        EXPECT_NO_THROW((ClientSecretCredential{httpClient, options})) << host;
    }
}

TEST(CredentialsTests, FederatedTokenFileWhitespaceHandling) {
    const std::filesystem::path tokenFile = std::filesystem::temp_directory_path() / "azc-federated-ws.txt";
    const auto run = [&](std::string_view content) {
        {
            std::ofstream file{tokenFile, std::ios::binary | std::ios::trunc};
            file << content;
        }
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(Json(200, R"({"expires_in":3600,"access_token":"wi"})"));
        WorkloadIdentityCredential credential{httpClient, WorkloadOptions("t", "app", tokenFile.string())};
        const std::error_code error = GetToken(httpClient, credential).Error;
        std::string body;
        if (!httpClient.Requests().empty()) {
            body = FakeHttpClient::BodyAsString(httpClient.LastRequest());
        }
        return std::pair{error, body};
    };

    EXPECT_FALSE(run("abc\t\r\n").first);
    EXPECT_NE(run("abc\t\r\n").second.find("client_assertion=abc&"), std::string::npos);
    EXPECT_FALSE(run(" \t abc").first);
    EXPECT_TRUE(run("").first);
    EXPECT_TRUE(run("a bc").first);
    std::filesystem::remove(tokenFile);
}

TEST(CredentialsTests, AllCredentialsRejectErrorStatusesAndLeaveTokenEmpty) {
    const std::filesystem::path tokenFile = std::filesystem::temp_directory_path() / "azc-federated-status.txt";
    {
        std::ofstream file{tokenFile, std::ios::binary | std::ios::trunc};
        file << "federated";
    }

    for (const unsigned int status : {400U, 401U, 500U}) {
        FakeHttpClient httpClient;
        ClientSecretCredential secret{httpClient, SecretOptions()};
        WorkloadIdentityCredential workload{httpClient, WorkloadOptions("t", "app", tokenFile.string())};
        ManagedIdentityCredential managed{httpClient, ManagedOptions({}, {})};
        const auto check = [&](auto& credential) {
            httpClient.EnqueueResponse(Json(status, R"({"error":"x","access_token":"must-not-leak"})"));
            const TokenResult result = GetToken(httpClient, credential);
            EXPECT_TRUE(result.Error) << status;
            EXPECT_TRUE(result.Token.Token.empty()) << status;
        };
        check(secret);
        check(workload);
        check(managed);
    }
    std::filesystem::remove(tokenFile);
}

TEST(CredentialsTests, AllCredentialsAreCancelledWhileTheRequestIsInFlight) {
    const std::filesystem::path tokenFile = std::filesystem::temp_directory_path() / "azc-federated-cancel.txt";
    {
        std::ofstream file{tokenFile, std::ios::binary | std::ios::trunc};
        file << "federated";
    }

    const auto expectCancelled = [](FakeHttpClient& httpClient, boost::asio::cancellation_signal& signal,
                                    auto& credential) {
        httpClient.EnqueueDeferredResponse(Json(200, R"({"access_token":"t","expires_in":3600})"));
        std::optional<std::error_code> error;
        credential.GetTokenAsync(StorageScope(), [&](std::error_code code, AccessToken /*token*/) { error = code; });
        EXPECT_EQ(httpClient.PendingCount(), 1U);
        signal.emit(boost::asio::cancellation_type::terminal);
        httpClient.Poll();
        ASSERT_TRUE(error.has_value());
        EXPECT_EQ(*error, std::make_error_code(std::errc::operation_canceled));
    };

    {
        FakeHttpClient httpClient;
        boost::asio::cancellation_signal signal;
        ClientSecretCredentialOptions options = SecretOptions();
        options.RequestOptions.SetCancellationSlot(signal.slot());
        ClientSecretCredential credential{httpClient, options};
        expectCancelled(httpClient, signal, credential);
    }
    {
        FakeHttpClient httpClient;
        boost::asio::cancellation_signal signal;
        WorkloadIdentityCredentialOptions options = WorkloadOptions("t", "app", tokenFile.string());
        options.RequestOptions.SetCancellationSlot(signal.slot());
        WorkloadIdentityCredential credential{httpClient, options};
        expectCancelled(httpClient, signal, credential);
    }
    {
        FakeHttpClient httpClient;
        boost::asio::cancellation_signal signal;
        ManagedIdentityCredentialOptions options;
        options.RequestOptions.SetCancellationSlot(signal.slot());
        ManagedIdentityCredential credential{httpClient, options};
        expectCancelled(httpClient, signal, credential);
    }
    std::filesystem::remove(tokenFile);
}
namespace {
void SetEnvironmentValue(const char* name, const char* value) {
#ifdef _WIN32
    _putenv_s(name, value);
#else
    if (*value == '\0') {
        unsetenv(name);
    } else {
        setenv(name, value, 1);
    }
#endif
}
} // namespace

TEST(CredentialsTests, ClientSecretOptionsFromEnvironmentReadsTheStandardVariables) {
    SetEnvironmentValue("AZURE_TENANT_ID", "tenant-env");
    SetEnvironmentValue("AZURE_CLIENT_ID", "client-env");
    SetEnvironmentValue("AZURE_CLIENT_SECRET", "secret-env");
    SetEnvironmentValue("AZURE_AUTHORITY_HOST", "https://authority.example/");

    const auto options = ClientSecretCredentialOptions::FromEnvironment();
    EXPECT_EQ(options.TenantId, "tenant-env");
    EXPECT_EQ(options.ClientId, "client-env");
    EXPECT_EQ(options.ClientSecret, "secret-env");
    EXPECT_EQ(options.AuthorityHost, "https://authority.example/");

    SetEnvironmentValue("AZURE_AUTHORITY_HOST", "");
    EXPECT_EQ(ClientSecretCredentialOptions::FromEnvironment().AuthorityHost,
              std::string{AVEVA::AzureClient::DefaultAuthorityHost});

    SetEnvironmentValue("AZURE_TENANT_ID", "");
    SetEnvironmentValue("AZURE_CLIENT_ID", "");
    SetEnvironmentValue("AZURE_CLIENT_SECRET", "");
    EXPECT_TRUE(ClientSecretCredentialOptions::FromEnvironment().TenantId.empty());
}

namespace {
[[nodiscard]] AVEVA::AzureClient::AzurePipelinesCredentialOptions PipelinesOptions() {
    AVEVA::AzureClient::AzurePipelinesCredentialOptions options;
    options.TenantId = "tenant";
    options.ClientId = "client";
    options.ServiceConnectionId = "conn&id";
    options.SystemAccessToken = "system";
    options.OidcRequestUri = "https://pipelines.example/oidc";
    options.Retry.MaxRetries = 0;
    return options;
}
} // namespace

TEST(CredentialsTests, AzurePipelinesCredentialExchangesTheOidcTokenAsClientAssertion) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(200, R"({"oidcToken":"oidc-jwt"})"));
    httpClient.EnqueueResponse(Json(200, R"({"access_token":"tok","expires_in":3600})"));
    AzurePipelinesCredential credential{httpClient, PipelinesOptions()};

    const TokenResult result = GetToken(httpClient, credential);

    EXPECT_FALSE(result.Error);
    EXPECT_EQ(result.Token.Token, "tok");
    ASSERT_EQ(httpClient.Requests().size(), 2U);
    const HttpRequest& oidc = httpClient.Requests().front().Request;
    EXPECT_EQ(oidc.GetMethod(), HttpMethod::Post);
    EXPECT_EQ(oidc.GetUrl(), "https://pipelines.example/oidc?api-version=7.1&serviceConnectionId=conn%26id");
    EXPECT_EQ(Header(oidc, "Authorization"), "Bearer system");
    const std::string exchangeBody = FakeHttpClient::BodyAsString(httpClient.LastRequest());
    EXPECT_NE(exchangeBody.find("client_assertion=oidc-jwt"), std::string::npos);
}

TEST(CredentialsTests, AzurePipelinesCredentialAppendsToAnExistingQueryString) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(401, ""));
    auto options = PipelinesOptions();
    options.OidcRequestUri = "https://pipelines.example/oidc?x=1";
    AzurePipelinesCredential credential{httpClient, options};

    static_cast<void>(GetToken(httpClient, credential));

    ASSERT_EQ(httpClient.Requests().size(), 1U);
    EXPECT_EQ(httpClient.Requests().front().Request.GetUrl(),
              "https://pipelines.example/oidc?x=1&api-version=7.1&serviceConnectionId=conn%26id");
}

TEST(CredentialsTests, AzurePipelinesCredentialRejectsMissingConfigurationWithoutSendingRequests) {
    FakeHttpClient httpClient;
    for (const auto clear :
         {+[](AVEVA::AzureClient::AzurePipelinesCredentialOptions& o) { o.OidcRequestUri.clear(); },
          +[](AVEVA::AzureClient::AzurePipelinesCredentialOptions& o) { o.ServiceConnectionId.clear(); },
          +[](AVEVA::AzureClient::AzurePipelinesCredentialOptions& o) { o.SystemAccessToken.clear(); }}) {
        auto options = PipelinesOptions();
        clear(options);
        SetEnvironmentValue("SYSTEM_OIDCREQUESTURI", "");
        AzurePipelinesCredential credential{httpClient, options};
        EXPECT_EQ(GetToken(httpClient, credential).Error, std::make_error_code(std::errc::invalid_argument));
    }
    EXPECT_TRUE(httpClient.Requests().empty());
}

TEST(CredentialsTests, AzurePipelinesCredentialDefaultsTheOidcUriFromTheEnvironment) {
    SetEnvironmentValue("SYSTEM_OIDCREQUESTURI", "https://env.example/oidc");
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(401, ""));
    auto options = PipelinesOptions();
    options.OidcRequestUri.clear();
    AzurePipelinesCredential credential{httpClient, options};

    static_cast<void>(GetToken(httpClient, credential));
    SetEnvironmentValue("SYSTEM_OIDCREQUESTURI", "");

    ASSERT_EQ(httpClient.Requests().size(), 1U);
    EXPECT_TRUE(httpClient.Requests().front().Request.GetUrl().starts_with("https://env.example/oidc?api-version=7.1"));
}

TEST(CredentialsTests, AzurePipelinesCredentialMapsOidcFailuresToErrorCodes) {
    FakeHttpClient httpClient;
    AzurePipelinesCredential credential{httpClient, PipelinesOptions()};

    httpClient.EnqueueResponse(Json(401, ""));
    EXPECT_EQ(GetToken(httpClient, credential).Error, std::error_code{BlobStorageErrorCode::AuthenticationFailed});

    for (const std::string body : {"not json", "[]", R"({"other":"x"})", R"({"oidcToken":""})"}) {
        httpClient.EnqueueResponse(Json(200, body));
        EXPECT_EQ(GetToken(httpClient, credential).Error, std::error_code{BlobStorageErrorCode::InvalidResponse})
            << body;
    }

    const auto transport = std::make_error_code(std::errc::connection_refused);
    httpClient.EnqueueResponse(HttpResponse{}, transport);
    EXPECT_EQ(GetToken(httpClient, credential).Error, transport);
    EXPECT_EQ(httpClient.Requests().size(), 6U);
}

TEST(CredentialsTests, AzurePipelinesCredentialHonorsRetryAfterOnTheOidcRequest) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{429, {AVEVA::HttpHeader{"Retry-After", "1"}}, ""});
    httpClient.EnqueueResponse(Json(200, R"({"oidcToken":"oidc"})"));
    httpClient.EnqueueResponse(Json(200, R"({"access_token":"tok","expires_in":"3600"})"));
    auto options = PipelinesOptions();
    options.Retry.MaxRetries = 2;
    AzurePipelinesCredential credential{httpClient, options};

    std::optional<TokenResult> result;
    const auto start = std::chrono::steady_clock::now();
    credential.GetTokenAsync(StorageScope(), [&](std::error_code error, AccessToken token) {
        result = TokenResult{.Error = error, .Token = std::move(token)};
    });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));

    EXPECT_FALSE(result->Error);
    EXPECT_EQ(result->Token.Token, "tok");
    EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(900));
}

TEST(CredentialsTests, AzurePipelinesCredentialDoesNotRetryNonTransientOidcFailures) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(Json(401, ""));
    auto options = PipelinesOptions();
    options.Retry.MaxRetries = 2;
    AzurePipelinesCredential credential{httpClient, options};

    EXPECT_TRUE(GetToken(httpClient, credential).Error);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}
