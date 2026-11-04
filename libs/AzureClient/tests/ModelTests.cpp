// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <cstdint>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <initializer_list>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {
using AVEVA::HttpHeader;
using AVEVA::HttpMethod;
using AVEVA::HttpRequest;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::AcquireLeaseOptions;
using AVEVA::AzureClient::CreatePageBlobOptions;
using AVEVA::AzureClient::DeleteBlobOptions;
using AVEVA::AzureClient::ResizePageBlobOptions;
using AVEVA::AzureClient::ServerEncryption;
using AVEVA::AzureClient::VersionCurrency;
using AVEVA::AzureClient::Models::LeaseDurationType;
using AVEVA::AzureClient::Models::LeaseState;
using AVEVA::AzureClient::Models::LeaseStatus;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RunAwaitable;
namespace Private = AVEVA::AzureClient::Private;
using namespace std::chrono_literals;

[[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name) {
    return FakeHttpClient::FindHeaderValue(request, name);
}

void ExpectMalformedExtendedHeaderIsRejected(std::string_view name, std::string_view value) {
    EXPECT_THROW(static_cast<void>(Private::ParseBlobProperties(HttpResponse{
                     AVEVA::HttpStatus::Ok,
                     {AVEVA::HttpHeader(AVEVA::HeaderName{std::string{name}}, AVEVA::HeaderValue{std::string{value}})},
                     ""})),
                 std::invalid_argument)
        << name;
}

void ExpectMalformedExtendedHeadersAreRejected() {
    for (const auto& [name, value] :
         std::vector<std::pair<std::string, std::string>>{{"x-ms-creation-time", "yesterday"},
                                                          {"x-ms-server-encrypted", "maybe"},
                                                          {"x-ms-blob-sequence-number", "-1"},
                                                          {"x-ms-is-current-version", "yes"}}) {
        ExpectMalformedExtendedHeaderIsRejected(name, value);
    }
}

void ExpectLeaseDurationRejects(std::initializer_list<std::chrono::seconds> durations) {
    for (const auto duration : durations) {
        EXPECT_THROW((void)AVEVA::AzureClient::LeaseDuration{duration}, std::invalid_argument) << duration.count();
    }
}
} // namespace

TEST(ModelTests, GetPropertiesParsesExtendedHeaders) {
    const HttpResponse response{
        AVEVA::HttpStatus::Ok,
        {AVEVA::HttpHeader(AVEVA::HeaderName{"ETag"}, AVEVA::HeaderValue{"\"0x1\""}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Length"}, AVEVA::HeaderValue{"10"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Encoding"}, AVEVA::HeaderValue{"gzip"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Language"}, AVEVA::HeaderValue{"en-GB"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Disposition"}, AVEVA::HeaderValue{"attachment; filename=a.txt"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-blob-type"}, AVEVA::HeaderValue{"AppendBlob"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-creation-time"},
                           AVEVA::HeaderValue{"Wed, 01 Oct 2025 10:00:00 GMT"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-lease-status"}, AVEVA::HeaderValue{"locked"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-lease-state"}, AVEVA::HeaderValue{"leased"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-lease-duration"}, AVEVA::HeaderValue{"infinite"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-server-encrypted"}, AVEVA::HeaderValue{"true"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-version-id"}, AVEVA::HeaderValue{"2025-10-01T10:00:00.0000000Z"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-is-current-version"}, AVEVA::HeaderValue{"false"}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-blob-sequence-number"}, AVEVA::HeaderValue{"42"})},
        ""};

    const auto properties = Private::ParseBlobProperties(response);

    EXPECT_EQ(properties.ContentEncoding, "gzip");
    EXPECT_EQ(properties.ContentLanguage, "en-GB");
    EXPECT_EQ(properties.ContentDisposition, "attachment; filename=a.txt");
    ASSERT_TRUE(properties.CreatedOn.has_value());
    EXPECT_EQ(*properties.CreatedOn, *Private::ParseHttpDateHeader("Wed, 01 Oct 2025 10:00:00 GMT"));
    EXPECT_EQ(properties.LeaseStatus, LeaseStatus::Locked);
    EXPECT_EQ(properties.LeaseState, LeaseState::Leased);
    EXPECT_EQ(properties.LeaseDuration, LeaseDurationType::Infinite);
    EXPECT_EQ(properties.ServerEncrypted, std::optional<ServerEncryption>{ServerEncryption::Encrypted});
    EXPECT_EQ(properties.VersionId, "2025-10-01T10:00:00.0000000Z");
    EXPECT_EQ(properties.IsCurrentVersion, std::optional<VersionCurrency>{VersionCurrency::Superseded});
    EXPECT_EQ(properties.SequenceNumber, std::optional<std::uint64_t>{42});
}

TEST(ModelTests, GetPropertiesLeavesAbsentExtendedFieldsUnset) {
    const auto properties = Private::ParseBlobProperties(HttpResponse{
        AVEVA::HttpStatus::Ok, {AVEVA::HttpHeader(AVEVA::HeaderName{"ETag"}, AVEVA::HeaderValue{"\"0x1\""})}, ""});

    EXPECT_FALSE(properties.CreatedOn.has_value());
    EXPECT_EQ(properties.LeaseStatus, LeaseStatus::Unknown);
    EXPECT_FALSE(properties.ServerEncrypted.has_value());
    EXPECT_FALSE(properties.IsCurrentVersion.has_value());
    EXPECT_FALSE(properties.SequenceNumber.has_value());
}

TEST(ModelTests, GetPropertiesRejectsMalformedExtendedHeaders) { ExpectMalformedExtendedHeadersAreRejected(); }

TEST(ModelTests, ListBlobsParsesExtendedPropertiesAndVersionSiblings) {
    const auto result = Private::ParseListBlobsResultXml(R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults><Blobs><Blob>
  <Name>a.txt</Name>
  <VersionId>v1</VersionId>
  <IsCurrentVersion>true</IsCurrentVersion>
  <Properties>
    <Creation-Time>Wed, 01 Oct 2025 10:00:00 GMT</Creation-Time>
    <Content-Encoding>gzip</Content-Encoding>
    <Content-Language>de</Content-Language>
    <Content-Disposition>inline</Content-Disposition>
    <x-ms-blob-sequence-number>3</x-ms-blob-sequence-number>
    <BlobType>PageBlob</BlobType>
    <LeaseStatus>unlocked</LeaseStatus>
    <LeaseState>available</LeaseState>
    <ServerEncrypted>true</ServerEncrypted>
  </Properties>
</Blob></Blobs></EnumerationResults>)");

    ASSERT_EQ(result.Blobs.size(), 1U);
    const auto& properties = result.Blobs.at(0).Properties;
    EXPECT_EQ(properties.VersionId, "v1");
    EXPECT_EQ(properties.IsCurrentVersion, std::optional<VersionCurrency>{VersionCurrency::Current});
    EXPECT_TRUE(properties.CreatedOn.has_value());
    EXPECT_EQ(properties.ContentEncoding, "gzip");
    EXPECT_EQ(properties.ContentLanguage, "de");
    EXPECT_EQ(properties.ContentDisposition, "inline");
    EXPECT_EQ(properties.SequenceNumber, std::optional<std::uint64_t>{3});
    EXPECT_EQ(properties.LeaseStatus, LeaseStatus::Unlocked);
    EXPECT_EQ(properties.LeaseState, LeaseState::Available);
    EXPECT_EQ(properties.ServerEncrypted, std::optional<ServerEncryption>{ServerEncryption::Encrypted});
}

TEST(ModelTests, SharedKeyStringToSignIncludesContentEncodingAndLanguage) {
    HttpRequest request;
    request.SetMethod(HttpMethod::Put);
    request.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/c/b"});
    request.AddHeader(HttpHeader(AVEVA::HeaderName{"Content-Encoding"}, AVEVA::HeaderValue{"gzip"}));
    request.AddHeader(HttpHeader(AVEVA::HeaderName{"Content-Language"}, AVEVA::HeaderValue{"en"}));

    EXPECT_TRUE(Private::BuildSharedKeyStringToSign("account", request).starts_with("PUT\ngzip\nen\n\n"));

    HttpRequest plain;
    plain.SetMethod(HttpMethod::Get);
    plain.SetUrl(AVEVA::HttpUrl{"https://account.blob.core.windows.net/c/b"});
    EXPECT_TRUE(Private::BuildSharedKeyStringToSign("account", plain).starts_with("GET\n\n\n\n\n\n\n"));
}

TEST(ModelTests, AcquireLeaseDurationIsInfiniteByDefaultAndValidated) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    static_cast<void>(RunAwaitable(httpClient, client.AcquireLeaseAsync()));
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-lease-duration"), "-1");

    static_cast<void>(RunAwaitable(
        httpClient, client.AcquireLeaseAsync(AcquireLeaseOptions{
                        .Conditions = {},
                        .ProposedLeaseId = {},
                        .Duration = AVEVA::AzureClient::LeaseDuration{AVEVA::AzureClient::LeaseDuration::Min}})));
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-lease-duration"), "15");
    const std::size_t sent = httpClient.RequestCount();

    ExpectLeaseDurationRejects(
        {AVEVA::AzureClient::LeaseDuration::Min - 1s, AVEVA::AzureClient::LeaseDuration::Max + 1s, 0s});
    EXPECT_EQ(httpClient.RequestCount(), sent);
}

TEST(ModelTests, ResizeUsesResizePageBlobOptionsConditions) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, MakeBlobClientOptions()};

    ResizePageBlobOptions options;
    options.Conditions.IfMatch = "\"etag\"";
    static_cast<void>(RunAwaitable(
        httpClient, client.ResizeAsync(AVEVA::AzureClient::PageBlobSize{2U * AVEVA::AzureClient::PageBlobPageSize},
                                       std::move(options))));

    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-blob-content-length"), "1024");
    EXPECT_EQ(Header(httpClient.LastRequest(), "If-Match"), "\"etag\"");
}
