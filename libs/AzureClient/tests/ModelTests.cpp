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

[[nodiscard]] bool HasHeader(const HttpRequest& request, std::string_view name) {
    return std::ranges::any_of(request.GetHeaders(),
                               [name](const auto& header) { return Private::IEquals(header.GetName(), name); });
}

[[nodiscard]] std::string Header(const HttpRequest& request, std::string_view name) {
    return FakeHttpClient::FindHeaderValue(request, name);
}

void ExpectMalformedExtendedHeaderIsRejected(std::string_view name, std::string_view value) {
    EXPECT_THROW(static_cast<void>(
                     Private::ParseBlobProperties(HttpResponse{200, {{std::string{name}, std::string{value}}}, ""})),
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

void ExpectAcquireLeaseRejectsInvalidDurations(BlobClient& client, FakeHttpClient& httpClient,
                                               std::initializer_list<std::chrono::seconds> durations) {
    for (const auto duration : durations) {
        const auto result =
            RunAwaitable(httpClient, client.AcquireLeaseAsync(AcquireLeaseOptions{
                                         .Conditions = {}, .ProposedLeaseId = {}, .Duration = duration}));
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument)) << duration.count();
    }
}
} // namespace

TEST(ModelTests, GetPropertiesParsesExtendedHeaders) {
    const HttpResponse response{200,
                                {{"ETag", "\"0x1\""},
                                 {"Content-Length", "10"},
                                 {"Content-Encoding", "gzip"},
                                 {"Content-Language", "en-GB"},
                                 {"Content-Disposition", "attachment; filename=a.txt"},
                                 {"x-ms-blob-type", "AppendBlob"},
                                 {"x-ms-creation-time", "Wed, 01 Oct 2025 10:00:00 GMT"},
                                 {"x-ms-lease-status", "locked"},
                                 {"x-ms-lease-state", "leased"},
                                 {"x-ms-lease-duration", "infinite"},
                                 {"x-ms-server-encrypted", "true"},
                                 {"x-ms-version-id", "2025-10-01T10:00:00.0000000Z"},
                                 {"x-ms-is-current-version", "false"},
                                 {"x-ms-blob-sequence-number", "42"}},
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
    EXPECT_EQ(properties.ServerEncrypted, std::optional<bool>{true});
    EXPECT_EQ(properties.VersionId, "2025-10-01T10:00:00.0000000Z");
    EXPECT_EQ(properties.IsCurrentVersion, std::optional<bool>{false});
    EXPECT_EQ(properties.SequenceNumber, std::optional<std::uint64_t>{42});
}

TEST(ModelTests, GetPropertiesLeavesAbsentExtendedFieldsUnset) {
    const auto properties = Private::ParseBlobProperties(HttpResponse{200, {{"ETag", "\"0x1\""}}, ""});

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
    EXPECT_EQ(properties.IsCurrentVersion, std::optional<bool>{true});
    EXPECT_TRUE(properties.CreatedOn.has_value());
    EXPECT_EQ(properties.ContentEncoding, "gzip");
    EXPECT_EQ(properties.ContentLanguage, "de");
    EXPECT_EQ(properties.ContentDisposition, "inline");
    EXPECT_EQ(properties.SequenceNumber, std::optional<std::uint64_t>{3});
    EXPECT_EQ(properties.LeaseStatus, LeaseStatus::Unlocked);
    EXPECT_EQ(properties.LeaseState, LeaseState::Available);
    EXPECT_EQ(properties.ServerEncrypted, std::optional<bool>{true});
}

TEST(ModelTests, SharedKeyStringToSignIncludesContentEncodingAndLanguage) {
    HttpRequest request;
    request.SetMethod(HttpMethod::Put);
    request.SetUrl("https://account.blob.core.windows.net/c/b");
    request.AddHeader(HttpHeader{"Content-Encoding", "gzip"});
    request.AddHeader(HttpHeader{"Content-Language", "en"});

    EXPECT_TRUE(Private::BuildSharedKeyStringToSign("account", request).starts_with("PUT\ngzip\nen\n\n"));

    HttpRequest plain;
    plain.SetMethod(HttpMethod::Get);
    plain.SetUrl("https://account.blob.core.windows.net/c/b");
    EXPECT_TRUE(Private::BuildSharedKeyStringToSign("account", plain).starts_with("GET\n\n\n\n\n\n\n"));
}

TEST(ModelTests, AcquireLeaseDurationIsInfiniteByDefaultAndValidated) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    static_cast<void>(RunAwaitable(httpClient, client.AcquireLeaseAsync()));
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-lease-duration"), "-1");

    static_cast<void>(RunAwaitable(httpClient, client.AcquireLeaseAsync(AcquireLeaseOptions{
                                                   .Conditions = {}, .ProposedLeaseId = {}, .Duration = 15s})));
    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-lease-duration"), "15");
    const std::size_t sent = httpClient.RequestCount();

    ExpectAcquireLeaseRejectsInvalidDurations(client, httpClient, {14s, 61s, 0s});
    EXPECT_EQ(httpClient.RequestCount(), sent);
}

TEST(ModelTests, ResizeUsesResizePageBlobOptionsConditions) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, MakeBlobClientOptions()};

    ResizePageBlobOptions options;
    options.Conditions.IfMatch = "\"etag\"";
    static_cast<void>(RunAwaitable(httpClient, client.ResizeAsync(1024U, std::move(options))));

    EXPECT_EQ(Header(httpClient.LastRequest(), "x-ms-blob-content-length"), "1024");
    EXPECT_EQ(Header(httpClient.LastRequest(), "If-Match"), "\"etag\"");
}
