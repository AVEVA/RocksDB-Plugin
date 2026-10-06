#include "Redaction.hpp"

#include <gtest/gtest.h>

namespace
{
    using namespace AVEVA::AzureClient::Private;
}

TEST(RedactionTests, AuthorizationLikeHeadersAreRedactedCaseInsensitively)
{
    EXPECT_EQ(RedactHeaderForDiagnostics("Authorization", "SharedKey a:b"), "[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("AUTHORIZATION", "Bearer t"), "[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("x-ms-copy-source-authorization", "Bearer t"), "[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("X-IDENTITY-HEADER", "secret"), "[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("x-ms-encryption-key", "secret"), "[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("x-ms-version", "2023-11-03"), "2023-11-03");
}

TEST(RedactionTests, UrlValuedHeadersHaveTheirSasSignatureRedacted)
{
    EXPECT_EQ(RedactHeaderForDiagnostics("x-ms-copy-source", "https://a/c/b?sv=2023&sig=abc&se=2030"),
        "https://a/c/b?sv=2023&sig=[REDACTED]&se=2030");
    EXPECT_EQ(RedactHeaderForDiagnostics("X-MS-COPY-SOURCE", "https://a/c/b?SIG=abc"), "https://a/c/b?SIG=[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("Location", "HTTPS://a/c/b?sig=abc"), "HTTPS://a/c/b?sig=[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("Referer", "http://a/c/b?sig=abc"), "http://a/c/b?sig=[REDACTED]");
    EXPECT_EQ(RedactHeaderForDiagnostics("x-ms-copy-source", "https://a/c/b"), "https://a/c/b");
    EXPECT_EQ(RedactHeaderForDiagnostics("Content-Type", "text/plain?sig=keep"), "text/plain?sig=keep");
}

TEST(RedactionTests, UrlSasSignatureIsRedactedAndOtherParametersAreKept)
{
    EXPECT_EQ(RedactUrlForDiagnostics("https://a.blob.core.windows.net/c/b?sv=2023&sig=abc%2Bdef&se=2030"),
        "https://a.blob.core.windows.net/c/b?sv=2023&sig=[REDACTED]&se=2030");
    EXPECT_EQ(RedactUrlForDiagnostics("https://a/c/b?SIG=x#frag"), "https://a/c/b?SIG=[REDACTED]#frag");
    EXPECT_EQ(RedactUrlForDiagnostics("https://a/c/b"), "https://a/c/b");
    EXPECT_EQ(RedactUrlForDiagnostics("https://a/c/b?signature=keep"), "https://a/c/b?signature=keep");
    EXPECT_EQ(RedactUrlForDiagnostics("https://a/c/b?%73ig=abc&se=1"), "https://a/c/b?%73ig=[REDACTED]&se=1");
}

TEST(RedactionTests, FormBodySecretsAreRedacted)
{
    EXPECT_EQ(RedactFormBodyForDiagnostics("grant_type=client_credentials&client_id=id&client_secret=s3cret&scope=x"),
        "grant_type=client_credentials&client_id=id&client_secret=[REDACTED]&scope=x");
    EXPECT_EQ(RedactFormBodyForDiagnostics("client_assertion=jwt&refresh_token=r"),
        "client_assertion=[REDACTED]&refresh_token=[REDACTED]");
    EXPECT_EQ(RedactFormBodyForDiagnostics(""), "");
}
