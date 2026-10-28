// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Helpers shared by AsyncCancellationTests.cpp and AsyncLifetimeTests.cpp. Operations are coroutines, so these tests
// start them with StartAwaitable (TestSupport.hpp) and cancel through the returned Operation.

#pragma once

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/cancellation_signal.hpp>

#include <gtest/gtest.h>

#include <expected>
#include <memory>
#include <optional>
#include <system_error>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace AsyncBehaviorSupport {
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobClientOptions;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::Models::BlobProperties;
using AVEVA::AzureClient::Models::DeleteBlobResult;
using AVEVA::AzureClient::Models::UploadPagesResult;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::BlobContainerClient;
using AVEVA::AzureClient::Tests::BlobServiceClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::MakeToken;
using AVEVA::AzureClient::Tests::Operation;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RunAwaitable;
using AVEVA::AzureClient::Tests::ScriptedTokenCredential;
using AVEVA::AzureClient::Tests::StartAwaitable;
using ScriptedBlobClient =
    AVEVA::AzureClient::BasicBlobClient<AVEVA::AzureClient::Tests::FakeHttpClient, ScriptedTokenCredential>;
using DeleteResult = std::expected<Response<DeleteBlobResult>, BlobStorageError>;
using GetBlobPropertiesResult = std::expected<Response<BlobProperties>, BlobStorageError>;

[[nodiscard]] inline BlobClientOptions MakePageBlobOptions() { return MakeBlobClientOptions("images", "page.bin"); }

[[nodiscard]] inline bool IsCanceled(const BlobStorageError& error) {
    return error.Code == std::make_error_code(std::errc::operation_canceled);
}
} // namespace AsyncBehaviorSupport
