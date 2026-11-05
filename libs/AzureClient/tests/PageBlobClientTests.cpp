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
#include <AVEVA/AzureClient/Types.hpp>

#include <AVEVA/HttpClient/HttpBody.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <cstddef>
#include <cstdint>
#include <gsl/pointers>
#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace {
// Page-aligned byte values whose wire forms the tests spell out in the expected headers.
constexpr std::uint64_t OnePage = AVEVA::AzureClient::PageBlobPageSize;
constexpr std::uint64_t TwoPages = 2U * OnePage;
constexpr std::uint64_t FourPages = 4U * OnePage;
// A body larger than one chunk, so the sink sees a single whole write.
constexpr std::size_t SinkBodyBytes = 1024;
// HTTP has no named constant for 307 Temporary Redirect.
constexpr AVEVA::HttpStatusCode TemporaryRedirect{307U};
constexpr std::uint64_t ShortRangeBytes = 5U;
using AVEVA::HttpBody;
using AVEVA::HttpMethod;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobClientOptions;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::CreatePageBlobOptions;
using AVEVA::AzureClient::PageBlobPageSize;
using AVEVA::AzureClient::PageBlobSize;
using AVEVA::AzureClient::PageLength;
using AVEVA::AzureClient::PageOffset;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::SecretUrl;
using AVEVA::AzureClient::SourcePageOffset;
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
    EXPECT_EQ(result.error().StatusCode, AVEVA::HttpStatus::InternalServerError);
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

void StartUploadPagesAsyncExpectingZeroLengthContent(FakeHttpClient& httpClient, PageBlobClient& client) {
    const auto result = RunAwaitable(httpClient, client.UploadPagesAsync(PageOffset{0U}, HttpBody{std::string{}}));
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
    EXPECT_EQ(result.error().StatusCode, TemporaryRedirect);
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
                                       const AVEVA::AzureClient::DownloadToOptions& options) {
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(path, options));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().BytesWritten, 5U);
}

} // namespace

TEST(StringSinkTests, SingleChunkCoveringExpectedSizeIsMovedNotCopied) {
    AVEVA::AzureClient::Private::StringSink sink;
    std::string body(SinkBodyBytes, 'x');
    const char* original = body.data();
    sink.Reserve(body.size());
    ASSERT_EQ(sink.Write(std::move(body)), AVEVA::AzureClient::WriteOutcome::Written);
    auto result = sink.Take();
    EXPECT_EQ(result.data(), original);
    EXPECT_EQ(result.size(), SinkBodyBytes);
}

TEST(StringSinkTests, MultipleChunksAreAppended) {
    AVEVA::AzureClient::Private::StringSink sink;
    constexpr std::size_t twoWrites = 6;
    sink.Reserve(twoWrites);
    ASSERT_EQ(sink.Write("abc"), AVEVA::AzureClient::WriteOutcome::Written);
    ASSERT_EQ(sink.Write("def"), AVEVA::AzureClient::WriteOutcome::Written);
    EXPECT_EQ(sink.Take(), "abcdef");
}

TEST(PageBlobClientTests, ResourceUrl_HasNoQueryStringWhileUrlCarriesTheSas) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    EXPECT_EQ(client.ResourceUrl(), "https://storageaccount.blob.core.windows.net/disks/disk.vhd");
    EXPECT_EQ(client.ResourceUrl().find("sig="), std::string::npos);
    EXPECT_NE(client.Url().Value().find("sig=fakesig"), std::string::npos);
}

TEST(PageBlobClientTests, CreateAsync_BuildsPutRequestWithPageBlobType) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{::AVEVA::HttpStatus::Created, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.CreateAsync(PageBlobSize{2U * PageBlobPageSize}));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().ETag, DefaultETag);

    const auto& request = httpClient.LastRequest();
    ExpectRequestContract(request, HttpMethod::Put,
                          "https://storageaccount.blob.core.windows.net/disks/disk.vhd?sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-type"), "PageBlob");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-content-length"), "1024");
}

TEST(PageBlobClientTests, PageAlignedValues_RejectUnalignedBytesAtConstruction) {
    constexpr std::uint64_t unaligned = PageBlobPageSize + 1U;
    EXPECT_THROW((void)PageBlobSize{unaligned}, std::invalid_argument);
    EXPECT_THROW((void)PageOffset{unaligned}, std::invalid_argument);
    EXPECT_THROW((void)SourcePageOffset{unaligned}, std::invalid_argument);
    EXPECT_THROW((void)PageLength{unaligned}, std::invalid_argument);
    EXPECT_EQ(PageBlobSize{PageBlobPageSize}.Value(), PageBlobPageSize);
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_SendsIfNoneMatchStar) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{::AVEVA::HttpStatus::Created, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobSize{PageBlobPageSize}));
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->Error().has_value());
    EXPECT_EQ(result->Value().ETag, DefaultETag);

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Put);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "If-None-Match"), "*");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-type"), "PageBlob");
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_SuppressesAlreadyExists) {
    for (const auto& [status, code] :
         {std::pair<AVEVA::HttpStatusCode, const char*>{::AVEVA::HttpStatus::Conflict, "BlobAlreadyExists"},
          std::pair<AVEVA::HttpStatusCode, const char*>{::AVEVA::HttpStatus::PreconditionFailed, "ConditionNotMet"}}) {
        FakeHttpClient httpClient;
        httpClient.DefaultResponse() = HttpResponse{
            status, {AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-error-code"}, AVEVA::HeaderValue{code})}, ""};
        PageBlobClient client{httpClient, BuildOptions()};

        const auto result = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobSize{PageBlobPageSize}));
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result->Error().has_value());
        EXPECT_EQ(result->Error()->StatusCode, status);
    }
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_OtherErrorsAndValidationStillFail) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{AVEVA::HttpStatus::NotFound,
                     {AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-error-code"}, AVEVA::HeaderValue{"ContainerNotFound"})},
                     ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto failedOnService =
        RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobSize{PageBlobPageSize}));
    ASSERT_FALSE(failedOnService.has_value());
    EXPECT_EQ(failedOnService.error().Code, BlobStorageErrorCode::ContainerNotFound);

    CreatePageBlobOptions invalid;
    invalid.Conditions.IfMatch = "\"abc\"";
    const auto failedValidation =
        RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobSize{PageBlobPageSize}, std::move(invalid)));
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
            RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobSize{PageBlobPageSize}, std::move(options)));
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
        EXPECT_EQ(httpClient.RequestCount(), 0U);
    }
}

TEST(PageBlobClientTests, CreateIfNotExistsAsync_AcceptsExplicitWildcardAndLease) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{::AVEVA::HttpStatus::Created, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};
    CreatePageBlobOptions options;
    options.Conditions.IfNoneMatch = "*";
    options.Conditions.LeaseId = "lease-1";

    const auto result =
        RunAwaitable(httpClient, client.CreateIfNotExistsAsync(PageBlobSize{PageBlobPageSize}, std::move(options)));
    EXPECT_TRUE(result.has_value() && !result->Error().has_value());
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "If-None-Match"), "*");
}

TEST(PageBlobClientTests, UploadPagesAsync_SetsRangeAndPageWriteHeaders) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize, 'x');
    (void)RunAwaitable(httpClient, client.UploadPagesAsync(PageOffset{PageBlobPageSize}, HttpBody{content}));

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetUrl(),
              "https://storageaccount.blob.core.windows.net/disks/disk.vhd?comp=page&sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-page-write"), "update");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-range"), "bytes=512-1023");
    EXPECT_EQ(FakeHttpClient::BodyAsString(request), content);
}

TEST(PageBlobClientTests, UploadPagesAsync_SharedBufferBodySendsViewWithoutCopying) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    auto content = std::make_shared<const std::vector<char>>(PageBlobPageSize, 'x');
    const auto* originalData = content->data();
    const auto bytes = std::as_bytes(std::span{*content});
    (void)RunAwaitable(httpClient,
                       client.UploadPagesAsync(PageOffset{PageBlobPageSize},
                                               HttpBody{bytes, gsl::not_null<std::shared_ptr<const void>>{content}}));

    const auto record = httpClient.Requests().back();
    EXPECT_EQ(record.SentBodyAddress, static_cast<const void*>(originalData));
    EXPECT_EQ(record.Body.size(), PageBlobPageSize);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(record.Request, "x-ms-range"), "bytes=512-1023");
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsUnalignedContentViaCompletion) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(
        httpClient, client.UploadPagesAsync(PageOffset{0U}, HttpBody{std::string(PageBlobPageSize + 1U, 'x')}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
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

    (void)RunAwaitable(httpClient,
                       client.UploadPagesFromUrlAsync(
                           PageOffset{TwoPages}, SecretUrl{"https://account.blob.core.windows.net/files/src.sst"},
                           SourcePageOffset{OnePage}, PageLength{FourPages},
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
    EXPECT_EQ(request.GetBody().Size().Value(), 0U);
}

TEST(PageBlobClientTests, UploadPagesFromUrlAsync_ClientCredentialIsForwardedAsBearerToken) {
    FakeHttpClient httpClient;
    auto options = BuildOptions();
    options.SasToken.clear();
    auto credential = std::make_shared<AVEVA::AzureClient::Tests::StaticTokenCredential>("client-token");
    AVEVA::AzureClient::BasicPageBlobClient<FakeHttpClient, AVEVA::AzureClient::Tests::StaticTokenCredential> client{
        httpClient, options, credential};

    (void)RunAwaitable(httpClient,
                       client.UploadPagesFromUrlAsync(
                           PageOffset{0U}, SecretUrl{"https://a/b/c"}, SourcePageOffset{0U}, PageLength{OnePage},
                           AVEVA::AzureClient::UploadPagesFromUrlOptions{
                               .SourceAuth = AVEVA::AzureClient::SourceAuthorization::ClientCredential}));

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-copy-source-authorization"), "Bearer client-token");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "Authorization"), "Bearer client-token");
}

TEST(PageBlobClientTests, UploadPagesFromUrlAsync_ClientCredentialMarkerIsDroppedForSasClients) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    (void)RunAwaitable(httpClient,
                       client.UploadPagesFromUrlAsync(
                           PageOffset{0U}, SecretUrl{"https://a/b/c"}, SourcePageOffset{0U}, PageLength{OnePage},
                           AVEVA::AzureClient::UploadPagesFromUrlOptions{
                               .SourceAuth = AVEVA::AzureClient::SourceAuthorization::ClientCredential}));

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

    constexpr std::uint64_t tooLong = AVEVA::AzureClient::MaxPagesFromUrlBytes + PageBlobPageSize;
    const auto upload = [&](std::string url, std::uint64_t length) {
        return RunAwaitable(httpClient, client.UploadPagesFromUrlAsync(PageOffset{0U}, SecretUrl{std::move(url)},
                                                                       SourcePageOffset{0U}, PageLength{length}));
    };
    expectInvalid(upload("https://a/b/c", 0U));
    expectInvalid(upload("https://a/b/c", tooLong));
    expectInvalid(upload("", PageBlobPageSize));

    EXPECT_EQ(failures, 3);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, GetPageRangesAsync_ParsesRangesAndHeaders) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{
        AVEVA::HttpStatus::Ok,
        {AVEVA::HttpHeader(AVEVA::HeaderName{"ETag"}, AVEVA::HeaderValue{"\"0x1\""}),
         AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-blob-content-length"}, AVEVA::HeaderValue{"8192"})},
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
    httpClient.DefaultResponse() = HttpResponse{AVEVA::HttpStatus::Ok, {}, "<PageList />"};
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
    httpClient.DefaultResponse() = HttpResponse{
        AVEVA::HttpStatus::Ok, {}, "<PageList><PageRange><Start>512</Start><End>0</End></PageRange></PageList>"};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.GetPageRangesAsync());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::InvalidResponse);
}

TEST(PageBlobClientTests, GetPageRangesAsync_RangeLengthOverflowIsInvalidResponse) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{AVEVA::HttpStatus::Ok,
                     {},
                     "<PageList><PageRange><Start>0</Start><End>18446744073709551615</End></PageRange></PageList>"};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.GetPageRangesAsync());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::InvalidResponse);
}

TEST(PageBlobClientTests, ResizeAsync_SetsContentLengthHeader) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    (void)RunAwaitable(httpClient, client.ResizeAsync(PageBlobSize{FourPages}));

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
    httpClient.DefaultResponse() =
        HttpResponse{AVEVA::HttpStatus::Ok,
                     {AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-blob-type"}, AVEVA::HeaderValue{"PageBlob"}),
                      AVEVA::HttpHeader(AVEVA::HeaderName{"Content-Length"}, AVEVA::HeaderValue{"2048"})},
                     ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.GetPropertiesAsync());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().Type, BlobType::PageBlob);
    EXPECT_EQ(result->Value().ContentLength, 2048U);

    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Head);
}

TEST(PageBlobClientTests, CreateAsync_MapsServiceErrorCode) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{AVEVA::HttpStatus::NotFound,
                     {AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-error-code"}, AVEVA::HeaderValue{"BlobNotFound"})},
                     ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.CreateAsync(PageBlobSize{PageBlobPageSize}));
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

    (void)RunAwaitable(httpClient, client.CreateAsync(PageBlobSize{PageBlobPageSize}));

    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization")
                    .starts_with("SharedKey storageaccount:"));
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsOverflowingRangeViaCompletion) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize * 2U, 'x');
    const auto result = RunAwaitable(
        httpClient,
        client.UploadPagesAsync(PageOffset{std::numeric_limits<std::uint64_t>::max() - (PageBlobPageSize - 1U)},
                                HttpBody{content}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, DeleteAsync_PropagatesTransportErrorWithoutInspectingHttpStatus) {
    FakeHttpClient httpClient;
    httpClient.DefaultError() = std::make_error_code(std::errc::timed_out);
    httpClient.DefaultResponse() =
        HttpResponse{::AVEVA::HttpStatus::Conflict,
                     {AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-error-code"}, AVEVA::HeaderValue{"BlobNotFound"})},
                     ""};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingTransportError(httpClient, client);
}

TEST(PageBlobClientTests, DeleteAsync_TreatsRedirectAsServiceError) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{TemporaryRedirect,
                     {AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-request-id"}, AVEVA::HeaderValue{"page-redirect"})},
                     ""};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingRedirectServiceError(httpClient, client);
}

TEST(PageBlobClientTests, DeleteAsync_FallsBackToServiceErrorForMalformedXmlAndMissingRequestId) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{AVEVA::HttpStatus::InternalServerError, {}, "<Error><Message>broken"};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingMalformedXmlServiceError(httpClient, client);
}

TEST(PageBlobClientTests, DeleteAsync_UnrecognizedStorageCodeFallsBackToServiceErrorButPreservesDetails) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{::AVEVA::HttpStatus::Conflict,
                     {AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-error-code"}, AVEVA::HeaderValue{"OddProblem"}),
                      AVEVA::HttpHeader(AVEVA::HeaderName{"x-ms-request-id"}, AVEVA::HeaderValue{"page-409"})},
                     "<Error><Message>Unexpected detail</Message></Error>"};
    PageBlobClient client{httpClient, BuildOptions()};

    StartDeleteAsyncExpectingUnrecognizedServiceError(httpClient, client);
}

TEST(PageBlobClientTests, DownloadAsyncAndDownloadToAsyncParseContentAndProperties) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{
        AVEVA::HttpStatus::Ok,
        MakeCanonicalSuccessHeaders(
            {{"Content-Length", "3"}, {"Content-Type", "application/octet-stream"}, {"x-ms-blob-type", "PageBlob"}}),
        "abc"});
    httpClient.EnqueueResponse(
        HttpResponse{AVEVA::HttpStatus::Ok,
                     MakeCanonicalSuccessHeaders({{"Content-Length", "4"}, {"x-ms-blob-type", "PageBlob"}}), "data"});
    httpClient.EnqueueResponse(
        HttpResponse{AVEVA::HttpStatus::Ok,
                     MakeCanonicalSuccessHeaders({{"Content-Length", "5"}, {"x-ms-blob-type", "PageBlob"}}), "bytes"});
    PageBlobClient client{httpClient, BuildOptions()};

    StartDownloadAsyncAndVerify(httpClient, client);

    std::string destination(4, '\0');
    StartDownloadToSpanAsyncAndVerify(httpClient, client, destination);
    EXPECT_EQ(destination, "data");

    const std::filesystem::path fsPath = std::filesystem::current_path() / "azure-client-page-download.bin";
    AVEVA::AzureClient::DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = ShortRangeBytes};
    StartDownloadToPathAsyncAndVerify(httpClient, client, fsPath, options);
    EXPECT_EQ(std::filesystem::file_size(fsPath), 5U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "Range"), "bytes=0-4");

    std::error_code ignored;
    std::filesystem::remove(fsPath, ignored);
}

TEST(PageBlobClientTests, DownloadAsyncReturnsContent) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{AVEVA::HttpStatus::Ok,
                     MakeCanonicalSuccessHeaders({{"Content-Length", "5"}, {"x-ms-blob-type", "PageBlob"}}), "hello"};
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result = RunAwaitable(httpClient, client.DownloadAsync());
    ASSERT_TRUE(result.has_value());
    const auto& content = result->Value().Content;
    EXPECT_EQ(std::string(content.data(), content.size()), "hello");
}

TEST(PageBlobClientTests, UploadPagesAsyncReportsServiceFailure) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(
        MakeAzureErrorResponse(::AVEVA::HttpStatus::Conflict, "BlobAlreadyExists", "conflict", "upload-pages-request"));
    PageBlobClient client{httpClient, BuildOptions()};

    const auto result =
        RunAwaitable(httpClient, client.UploadPagesAsync(PageOffset{0U}, HttpBody{std::string(PageBlobPageSize, 'x')}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobAlreadyExists);
    EXPECT_EQ(result.error().RequestId, "upload-pages-request");
}
