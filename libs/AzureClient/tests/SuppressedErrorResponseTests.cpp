// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>

namespace {
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::BlobContainerClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RunAwaitable;

struct Observed {
    bool Succeeded = false;
    unsigned int RawStatus = 0;
    std::string RawRequestId;
    std::string ErrorRequestId;
};

// Runs a suppressing operation against `status`/`code` and records what the caller sees.
template <class TStart>
[[nodiscard]] Observed RunSuppressed(FakeHttpClient& httpClient, unsigned int status, std::string_view code,
                                     TStart start) {
    httpClient.EnqueueResponse(MakeAzureErrorResponse(status, code, "suppressed", "req-1"));
    Observed observed;
    {
        auto result = RunAwaitable(httpClient, start());
        Observed value;
        value.Succeeded = result.has_value();
        if (result.has_value()) {
            value.RawStatus = result->RawResponse().GetStatus();
            for (const auto& header : result->RawResponse().GetHeaders()) {
                if (header.GetName() == "x-ms-request-id") {
                    value.RawRequestId = header.GetValue();
                }
            }
            value.ErrorRequestId = result->Error().has_value() ? result->Error()->RequestId : std::string{};
        }
        observed = value;
    }
    return observed;
}

} // namespace

TEST(SuppressedErrorResponseTests, OtherFailuresAreStillReported) {
    FakeHttpClient httpClient;
    PageBlobClient blockBlob{httpClient, MakeBlobClientOptions()};
    BlobContainerClient container{httpClient, MakeBlobContainerClientOptions()};

    EXPECT_FALSE(RunSuppressed(httpClient, 403, "AuthorizationFailure", [&] {
                     return blockBlob.DeleteIfExistsAsync();
                 }).Succeeded);
    EXPECT_FALSE(RunSuppressed(httpClient, 409, "ContainerBeingDeleted", [&] {
                     return container.CreateIfNotExistsAsync();
                 }).Succeeded);
}
