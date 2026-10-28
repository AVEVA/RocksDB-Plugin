// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Detail/DownloadSinks.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <filesystem>
#include <limits>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace {
using AVEVA::HttpMethod;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobClientOptions;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::CreatePageBlobOptions;
using AVEVA::AzureClient::PageBlobPageSize;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::Models::AcquireBlobLeaseResult;
using AVEVA::AzureClient::Models::BlobProperties;
using AVEVA::AzureClient::Models::BlobRequestConditions;
using AVEVA::AzureClient::Models::BlobType;
using AVEVA::AzureClient::Models::CreatePageBlobResult;
using AVEVA::AzureClient::Models::DeleteBlobResult;
using AVEVA::AzureClient::Models::DownloadBlobResult;
using AVEVA::AzureClient::Models::DownloadBlobToResult;
using AVEVA::AzureClient::Models::ReleaseBlobLeaseResult;
using AVEVA::AzureClient::Models::ResizePageBlobResult;
using AVEVA::AzureClient::Models::SetBlobMetadataResult;
using AVEVA::AzureClient::Models::UploadPagesResult;
using AVEVA::AzureClient::Tests::DefaultETag;
using AVEVA::AzureClient::Tests::ExpectRequestContract;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RunAwaitable;

[[nodiscard]] BlobClientOptions BuildOptions() { return MakeBlobClientOptions("disks", "disk.vhd"); }

void VerifyMalformedDeleteServiceErrorResult(
    const std::expected<Response<DeleteBlobResult>, BlobStorageError>& result) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::InternalError);
    EXPECT_EQ(result.error().StatusCode, 500U);
    EXPECT_TRUE(result.error().ErrorCode.empty());
    EXPECT_TRUE(result.error().Message.empty());
    EXPECT_TRUE(result.error().RequestId.empty());
}

void VerifyUnrecognizedDeleteServiceErrorResult(
    const std::expected<Response<DeleteBlobResult>, BlobStorageError>& result) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
    EXPECT_EQ(result.error().ErrorCode, "OddProblem");
    EXPECT_EQ(result.error().Message, "Unexpected detail");
    EXPECT_EQ(result.error().RequestId, "page-409");
}

void StartCreateAsyncExpectingUnalignedLength(FakeHttpClient& httpClient, PageBlobClient& client) {
    const auto result = RunAwaitable(httpClient, client.CreateAsync(100));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(result.error().Message, "contentLength must be a multiple of 512 bytes.");
}

void StartUploadPagesAsyncExpectingUnalignedOffset(FakeHttpClient& httpClient, PageBlobClient& client,
                                                   const std::string& content) {
    const auto result = RunAwaitable(httpClient, client.UploadPagesAsync(100, content));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(result.error().Message, "offset must be a multiple of 512 bytes.");
}

void StartUploadPagesAsyncExpectingZeroLengthContent(FakeHttpClient& httpClient, PageBlobClient& client) {
    const auto result = RunAwaitable(httpClient, client.UploadPagesAsync(0, std::string{}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(result.error().Message, "length must be greater than zero.");
}

void StartDeleteAsyncExpectingTransportError(FakeHttpClient& httpClient, PageBlobClient& client) {
    const auto result = RunAwaitable(httpClient, client.DeleteAsync());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_TRUE(result.error().RequestId.empty());
}

void StartDeleteAsyncExpectingRedirectServiceError(FakeHttpClient& httpClient, PageBlobClient& client) {
    const auto result = RunAwaitable(httpClient, client.DeleteAsync());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
    EXPECT_EQ(result.error().StatusCode, 307U);
    EXPECT_EQ(result.error().RequestId, "page-redirect");
}

void StartDeleteAsyncExpectingMalformedXmlServiceError(FakeHttpClient& httpClient, PageBlobClient& client) {
    VerifyMalformedDeleteServiceErrorResult(RunAwaitable(httpClient, client.DeleteAsync()));
}

void StartDeleteAsyncExpectingUnrecognizedServiceError(FakeHttpClient& httpClient, PageBlobClient& client) {
    VerifyUnrecognizedDeleteServiceErrorResult(RunAwaitable(httpClient, client.DeleteAsync()));
}

void StartDownloadAsyncAndVerify(FakeHttpClient& httpClient, PageBlobClient& client) {
    const auto result = RunAwaitable(httpClient, client.DownloadAsync());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().Properties.Type, BlobType::PageBlob);
    EXPECT_EQ(result->Value().Properties.ContentLength, 3U);
    EXPECT_EQ(result->Value().Content, "abc");
}

void StartDownloadToSpanAsyncAndVerify(FakeHttpClient& httpClient, PageBlobClient& client,
                                       std::span<char> destination) {
    const auto result =
        RunAwaitable(httpClient, client.DownloadToAsync(destination, AVEVA::AzureClient::DownloadBlobOptions{}));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().BytesWritten, 4U);
}

void StartDownloadToPathAsyncAndVerify(FakeHttpClient& httpClient, PageBlobClient& client,
                                       const std::filesystem::path& path,
                                       const AVEVA::AzureClient::DownloadBlobOptions& options) {
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(path, options));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().BytesWritten, 5U);
}

} // namespace

TEST(StringSinkTests, SingleChunkCoveringExpectedSizeIsMovedNotCopied) {
    AVEVA::AzureClient::Private::StringSink sink;
    std::string body(1024, 'x');
    const char* original = body.data();
    sink.Reserve(body.size());
    ASSERT_TRUE(sink.Write(std::move(body)));
    auto result = sink.Take();
    EXPECT_EQ(result.data(), original);
    EXPECT_EQ(result.size(), 1024U);
}

TEST(StringSinkTests, MultipleChunksAreAppended) {
    AVEVA::AzureClient::Private::StringSink sink;
    sink.Reserve(6);
    ASSERT_TRUE(sink.Write("abc"));
    ASSERT_TRUE(sink.Write("def"));
    EXPECT_EQ(sink.Take(), "abcdef");
}

TEST(PageBlobClientTests, ResourceUrl_HasNoQueryStringWhileUrlCarriesTheSas) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    EXPECT_EQ(client.ResourceUrl(), "https://storageaccount.blob.core.windows.net/disks/disk.vhd");
    EXPECT_EQ(client.ResourceUrl().find("sig="), std::string::npos);
    EXPECT_NE(client.Url().find("sig=fakesig"), std::string::npos);
}

TEST(PageBlobClientTests, CreateAsync_BuildsPutRequestWithPageBlobType) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.CreateAsync(2U * PageBlobPageSize));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().ETag, DefaultETag);

    const auto& request = httpClient.LastRequest();
    ExpectRequestContract(request, HttpMethod::Put,
                          "https://storageaccount.blob.core.windows.net/disks/disk.vhd?sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-type"), "PageBlob");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-content-length"), "1024");
}

TEST(PageBlobClientTests, CreateAsync_ReportsUnalignedLengthViaCompletion) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    StartCreateAsyncExpectingUnalignedLength(httpClient, client);

    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_SendsIfNoneMatchStar) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobPageSize));
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->Error().has_value());
    EXPECT_EQ(result->Value().ETag, DefaultETag);

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Put);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "If-None-Match"), "*");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-type"), "PageBlob");
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_SuppressesAlreadyExists) {
    for (const auto& [status, code] : {std::pair<unsigned, const char*>{409, "BlobAlreadyExists"},
                                       std::pair<unsigned, const char*>{412, "ConditionNotMet"}}) {
        FakeHttpClient httpClient;
        httpClient.DefaultResponse() = HttpResponse{status, {{"x-ms-error-code", code}}, ""};
        PageBlobClient client{httpClient, BuildOptions()};

        const auto result = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobPageSize));
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result->Error().has_value());
        EXPECT_EQ(result->Error()->StatusCode, status);
    }
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_OtherErrorsAndValidationStillFail) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{404, {{"x-ms-error-code", "ContainerNotFound"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto failedOnService = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobPageSize));
    ASSERT_FALSE(failedOnService.has_value());
    EXPECT_EQ(failedOnService.error().Code, BlobStorageErrorCode::ContainerNotFound);

    const auto failedValidation = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(100));
    ASSERT_FALSE(failedValidation.has_value());
    EXPECT_EQ(failedValidation.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_RejectsConditionsThatCouldOverwriteOrMisclassify) {
    using Mutator = void (*)(BlobRequestConditions&);
    const Mutator mutators[] = {
        [](BlobRequestConditions& c) { c.IfNoneMatch = "\"other\""; },
        [](BlobRequestConditions& c) { c.IfMatch = "\"abc\""; },
        [](BlobRequestConditions& c) { c.IfMatch = "*"; },
        [](BlobRequestConditions& c) { c.IfModifiedSince = std::chrono::system_clock::now(); },
        [](BlobRequestConditions& c) { c.IfUnmodifiedSince = std::chrono::system_clock::now(); },
    };
    for (const auto mutate : mutators) {
        FakeHttpClient httpClient;
        PageBlobClient client{httpClient, BuildOptions()};
        CreatePageBlobOptions options;
        mutate(options.Conditions);

        const auto result =
            RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobPageSize, std::move(options)));
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
        EXPECT_EQ(httpClient.RequestCount(), 0U);
    }
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_AcceptsExplicitWildcardAndLease) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};
    CreatePageBlobOptions options;
    options.Conditions.IfNoneMatch = "*";
    options.Conditions.LeaseId = "lease-1";

    const auto result = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobPageSize, std::move(options)));
    EXPECT_TRUE(result.has_value() && !result->Error().has_value());
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "If-None-Match"), "*");
}

TEST(PageBlobClientTests, UploadPagesAsync_SetsRangeAndPageWriteHeaders) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize, 'x');
    (void)RunAwaitable(httpClient, client.UploadPagesAsync(PageBlobPageSize, content));

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetUrl(),
              "https://storageaccount.blob.core.windows.net/disks/disk.vhd?comp=page&sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-page-write"), "update");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-range"), "bytes=512-1023");
    EXPECT_EQ(request.GetBody(), content);
}

TEST(PageBlobClientTests, UploadPagesAsync_SpanOverloadUsesSingleRequestPath) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content(PageBlobPageSize, std::byte{'x'});
    (void)RunAwaitable(httpClient, client.UploadPagesAsync(0, std::span<const std::byte>{content}));

    EXPECT_EQ(httpClient.LastRequest().GetBody().size(), PageBlobPageSize);
}

TEST(PageBlobClientTests, UploadPagesAsync_SharedBufferOverloadSendsViewWithoutCopying) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    auto content = std::make_shared<const std::vector<char>>(PageBlobPageSize, 'x');
    const auto* originalData = content->data();
    (void)RunAwaitable(httpClient, client.UploadPagesAsync(PageBlobPageSize, content));

    const auto& request = httpClient.LastRequest();
    EXPECT_TRUE(request.HasBodyView());
    EXPECT_EQ(static_cast<const void*>(request.GetBodyView().data()), static_cast<const void*>(originalData));
    EXPECT_EQ(request.GetBodySize(), PageBlobPageSize);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-range"), "bytes=512-1023");

    // The request keeps the buffer alive after the caller drops its reference.
    content.reset();
    EXPECT_EQ(request.GetBodyView().size(), PageBlobPageSize);
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsUnalignedOffsetViaCompletion) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize, 'x');
    StartUploadPagesAsyncExpectingUnalignedOffset(httpClient, client, content);

    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsZeroLengthContentViaCompletion) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    StartUploadPagesAsyncExpectingZeroLengthContent(httpClient, client);

    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, UploadPagesFromUrlAsync_SendsCopySourceHeadersAndNoBody) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    (void)RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(
                                       1024, "https://account.blob.core.windows.net/files/src.sst", 512, 2048,
                                       AVEVA::AzureClient::UploadPagesFromUrlOptions{.SourceIfMatch = "\"etag\""}));

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Put);
    EXPECT_EQ(request.GetUrl(),
              "https://storageaccount.blob.core.windows.net/disks/disk.vhd?comp=page&sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-page-write"), "update");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-copy-source"),
              "https://account.blob.core.windows.net/files/src.sst");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-source-range"), "bytes=512-2559");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-source-if-match"), "\"etag\"");
    EXPECT_TRUE(request.GetBody().empty());
}

TEST(PageBlobClientTests, UploadPagesFromUrlAsync_ClientCredentialIsForwardedAsBearerToken) {
    FakeHttpClient httpClient;
    auto options = BuildOptions();
    options.SasToken.clear();
    auto credential = std::make_shared<AVEVA::AzureClient::Tests::StaticTokenCredential>("client-token");
    AVEVA::AzureClient::BasicPageBlobClient<FakeHttpClient, AVEVA::AzureClient::Tests::StaticTokenCredential> client{
        httpClient, options, credential};

    (void)RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(0, "https://a/b/c", 0, 512,
                                                                  AVEVA::AzureClient::UploadPagesFromUrlOptions{
                                                                      .AuthorizeSourceWithClientCredential = true}));

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-copy-source-authorization"), "Bearer client-token");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "Authorization"), "Bearer client-token");
}

TEST(PageBlobClientTests, UploadPagesFromUrlAsync_ClientCredentialMarkerIsDroppedForSasClients) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    (void)RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(0, "https://a/b/c", 0, 512,
                                                                  AVEVA::AzureClient::UploadPagesFromUrlOptions{
                                                                      .AuthorizeSourceWithClientCredential = true}));

    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-copy-source-authorization"), "");
}

TEST(PageBlobClientTests, UploadPagesFromUrlAsync_RejectsBadRangesBeforeSending) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};
    int failures = 0;
    const auto expectInvalid = [&](std::expected<Response<UploadPagesResult>, BlobStorageError> result) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
        ++failures;
    };

    expectInvalid(RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(1, "https://a/b/c", 0, 512)));
    expectInvalid(RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(0, "https://a/b/c", 100, 512)));
    expectInvalid(RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(0, "https://a/b/c", 0, 0)));
    expectInvalid(
        RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(0, "https://a/b/c", 0, 4U * 1024U * 1024U + 512U)));
    expectInvalid(RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(0, "", 0, 512)));

    EXPECT_EQ(failures, 5);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, GetPageRangesAsync_ParsesRangesAndHeaders) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{
        200,
        {{"ETag", "\"0x1\""}, {"x-ms-blob-content-length", "8192"}},
        R"(<?xml version="1.0" encoding="utf-8"?><PageList><PageRange><Start>0</Start><End>511</End></PageRange>)"
        R"(<ClearRange><Start>512</Start><End>1023</End></ClearRange>)"
        R"(<PageRange><Start>4096</Start><End>5119</End></PageRange><NextMarker>m2</NextMarker></PageList>)"};
    PageBlobClient client{httpClient, BuildOptions()};

    AVEVA::AzureClient::GetPageRangesOptions options;
    options.Conditions.IfMatch = "\"0x1\"";
    const auto result = RunAwaitable(httpClient, client.GetPageRangesAsync(options));
    ASSERT_TRUE(result.has_value());
    const auto& value = result->Value();
    ASSERT_EQ(value.Ranges.size(), 2U);
    EXPECT_EQ(value.Ranges[0].Offset, 0U);
    EXPECT_EQ(value.Ranges[0].Length, 512U);
    EXPECT_EQ(value.Ranges[1].Offset, 4096U);
    EXPECT_EQ(value.Ranges[1].Length, 1024U);
    EXPECT_EQ(value.ETag, "\"0x1\"");
    EXPECT_EQ(value.BlobContentLength, 8192U);
    EXPECT_EQ(value.NextMarker, "m2");

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Get);
    EXPECT_NE(request.GetUrl().find("comp=pagelist"), std::string::npos);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "If-Match"), "\"0x1\"");
    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(request, "x-ms-range").empty());
}

TEST(PageBlobClientTests, GetPageRangesAsync_SendsMarker) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {}, "<PageList />"};
    PageBlobClient client{httpClient, BuildOptions()};

    AVEVA::AzureClient::GetPageRangesOptions options;
    options.Marker = "abc";
    const auto result = RunAwaitable(httpClient, client.GetPageRangesAsync(options));
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->Value().Ranges.empty());

    const auto& request = httpClient.LastRequest();
    EXPECT_NE(request.GetUrl().find("marker=abc"), std::string::npos);
}

TEST(PageBlobClientTests, GetPageRangesAsync_MalformedRangeIsInvalidResponse) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{200, {}, "<PageList><PageRange><Start>512</Start><End>0</End></PageRange></PageList>"};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.GetPageRangesAsync());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::InvalidResponse);
}

TEST(PageBlobClientTests, GetPageRangesAsync_RangeLengthOverflowIsInvalidResponse) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{
        200, {}, "<PageList><PageRange><Start>0</Start><End>18446744073709551615</End></PageRange></PageList>"};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.GetPageRangesAsync());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::InvalidResponse);
}

TEST(PageBlobClientTests, ResizeAsync_SetsContentLengthHeader) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    (void)RunAwaitable(httpClient, client.ResizeAsync(2048));

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetUrl(),
              "https://storageaccount.blob.core.windows.net/disks/disk.vhd?comp=properties&sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-content-length"), "2048");
}

TEST(PageBlobClientTests, DeleteAsync_UsesDeleteMethod) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    (void)RunAwaitable(httpClient, client.DeleteAsync());

    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Delete);
}

TEST(PageBlobClientTests, GetPropertiesAsync_ParsesBlobType) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {{"x-ms-blob-type", "PageBlob"}, {"Content-Length", "2048"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.GetPropertiesAsync());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().Type, BlobType::PageBlob);
    EXPECT_EQ(result->Value().ContentLength, 2048U);

    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Head);
}

TEST(PageBlobClientTests, CreateAsync_MapsServiceErrorCode) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.CreateAsync(PageBlobPageSize));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobNotFound);
}

TEST(PageBlobClientTests, Constructor_ThrowsForMissingBlobName) {
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.BlobName.clear();

    EXPECT_THROW((PageBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(PageBlobClientTests, Constructor_ThrowsForInvalidSharedKeyBase64) {
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "bad=="};

    EXPECT_THROW((PageBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(PageBlobClientTests, CreateAsync_UsesSharedKeyAuthorizationWhenConfigured) {
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    PageBlobClient client{httpClient, options};

    (void)RunAwaitable(httpClient, client.CreateAsync(PageBlobPageSize));

    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization")
                    .starts_with("SharedKey storageaccount:"));
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsOverflowingRangeViaCompletion) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize * 2U, 'x');
    const auto result = RunAwaitable(
        httpClient,
        client.UploadPagesAsync(std::numeric_limits<std::uint64_t>::max() - (PageBlobPageSize - 1U), content));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, DeleteAsync_PropagatesTransportErrorWithoutInspectingHttpStatus) {
    FakeHttpClient httpClient;
    httpClient.DefaultError() = std::make_error_code(std::errc::timed_out);
    httpClient.DefaultResponse() = HttpResponse{409, {{"x-ms-error-code", "BlobNotFound"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingTransportError(httpClient, client);
}

TEST(PageBlobClientTests, DeleteAsync_TreatsRedirectAsServiceError) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{307, {{"x-ms-request-id", "page-redirect"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingRedirectServiceError(httpClient, client);
}

TEST(PageBlobClientTests, DeleteAsync_FallsBackToServiceErrorForMalformedXmlAndMissingRequestId) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{500, {}, "<Error><Message>broken"};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingMalformedXmlServiceError(httpClient, client);
}

TEST(PageBlobClientTests, DeleteAsync_UnrecognizedStorageCodeFallsBackToServiceErrorButPreservesDetails) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{409,
                                                {{"x-ms-error-code", "OddProblem"}, {"x-ms-request-id", "page-409"}},
                                                "<Error><Message>Unexpected detail</Message></Error>"};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingUnrecognizedServiceError(httpClient, client);
}

TEST(PageBlobClientTests, DownloadAsyncAndDownloadToAsyncParseContentAndProperties) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{
        200,
        MakeCanonicalSuccessHeaders(
            {{"Content-Length", "3"}, {"Content-Type", "application/octet-stream"}, {"x-ms-blob-type", "PageBlob"}}),
        "abc"});
    httpClient.EnqueueResponse(HttpResponse{
        200, MakeCanonicalSuccessHeaders({{"Content-Length", "4"}, {"x-ms-blob-type", "PageBlob"}}), "data"});
    httpClient.EnqueueResponse(HttpResponse{
        200, MakeCanonicalSuccessHeaders({{"Content-Length", "5"}, {"x-ms-blob-type", "PageBlob"}}), "bytes"});
    PageBlobClient client{httpClient, BuildOptions()};

    StartDownloadAsyncAndVerify(httpClient, client);

    std::string destination(4, '\0');
    StartDownloadToSpanAsyncAndVerify(httpClient, client, destination);
    EXPECT_EQ(destination, "data");

    const std::filesystem::path fsPath = std::filesystem::current_path() / "azure-client-page-download.bin";
    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 5U};
    StartDownloadToPathAsyncAndVerify(httpClient, client, fsPath, options);
    EXPECT_EQ(std::filesystem::file_size(fsPath), 5U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "Range"), "bytes=0-4");

    std::error_code ignored;
    std::filesystem::remove(fsPath, ignored);
}

TEST(PageBlobClientTests, DownloadAsyncReturnsContent) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{
        200, MakeCanonicalSuccessHeaders({{"Content-Length", "5"}, {"x-ms-blob-type", "PageBlob"}}), "hello"};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.DownloadAsync());
    ASSERT_TRUE(result.has_value());
    const auto& content = result->Value().Content;
    EXPECT_EQ(std::string(content.data(), content.size()), "hello");
}

TEST(PageBlobClientTests, UploadPagesAsyncReportsServiceFailure) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "BlobAlreadyExists", "conflict", "upload-pages-request"));
    PageBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content(PageBlobPageSize, std::byte{'x'});
    const auto result = RunAwaitable(httpClient, client.UploadPagesAsync(0, std::span<const std::byte>{content}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobAlreadyExists);
    EXPECT_EQ(result.error().RequestId, "upload-pages-request");
}

// Compile-only proof that the "options" and "no options" overloads are unambiguous. Never run; only needs to compile.
namespace {
[[maybe_unused]] void PageBlobClientNoTokenOverloadsCompile(PageBlobClient& client) {
    using AVEVA::AzureClient::AcquireLeaseOptions;
    using AVEVA::AzureClient::CreatePageBlobOptions;
    using AVEVA::AzureClient::DeleteBlobOptions;
    using AVEVA::AzureClient::DownloadBlobOptions;
    using AVEVA::AzureClient::GetBlobPropertiesOptions;
    using AVEVA::AzureClient::UploadPagesOptions;

    const std::vector<std::byte> bytes;
    const std::span<const std::byte> span{bytes};
    std::string const str;
    std::string spanStorage(4, '\0');
    const std::span<char> destination{spanStorage};
    std::filesystem::path const path;

    static_cast<void>(client.CreateAsync(PageBlobPageSize));
    static_cast<void>(client.CreateAsync(PageBlobPageSize, CreatePageBlobOptions{}));

    static_cast<void>(client.UploadPagesAsync(0, str));
    static_cast<void>(client.UploadPagesAsync(0, str, UploadPagesOptions{}));
    static_cast<void>(client.UploadPagesAsync(0, span));
    static_cast<void>(client.UploadPagesAsync(0, span, UploadPagesOptions{}));

    static_cast<void>(client.ResizeAsync(PageBlobPageSize));
    static_cast<void>(client.ResizeAsync(PageBlobPageSize, AVEVA::AzureClient::ResizePageBlobOptions{}));

    static_cast<void>(client.DownloadAsync());
    static_cast<void>(client.DownloadAsync(DownloadBlobOptions{}));

    static_cast<void>(client.DownloadToAsync(destination, DownloadBlobOptions{}));
    static_cast<void>(client.DownloadToAsync(path));
    static_cast<void>(client.DownloadToAsync(path, DownloadBlobOptions{}));

    static_cast<void>(client.DeleteAsync());
    static_cast<void>(client.DeleteAsync(DeleteBlobOptions{}));

    static_cast<void>(client.GetPropertiesAsync());
    static_cast<void>(client.GetPropertiesAsync(GetBlobPropertiesOptions{}));

    static_cast<void>(client.AcquireLeaseAsync());
    static_cast<void>(client.AcquireLeaseAsync(AcquireLeaseOptions{}));

    static_cast<void>(client.ExistsAsync());
    static_cast<void>(client.DeleteIfExistsAsync());
    static_cast<void>(client.DeleteIfExistsAsync(AVEVA::AzureClient::DeleteBlobOptions{}));
}
} // namespace
