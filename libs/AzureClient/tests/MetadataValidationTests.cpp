// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <system_error>
#include <utility>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::AzureClient::BlobClient;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::CreateBlobContainerOptions;
    using AVEVA::AzureClient::SetBlobMetadataOptions;
    using AVEVA::AzureClient::UploadBlockBlobOptions;
    using AVEVA::AzureClient::Models::MetadataMap;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;

    // Calls SetMetadataAsync with a single `name` entry; returns the error, or nullopt on success.
    [[nodiscard]] std::optional<BlobStorageError> SetMetadataWithName(FakeHttpClient& httpClient,
        const std::string& name)
    {
        BlobClient client{httpClient, MakeBlobClientOptions()};
        SetBlobMetadataOptions options;
        options.Metadata.emplace(name, "value");
        std::optional<BlobStorageError> error;
        bool completed = false;
        client.SetMetadataAsync(std::move(options),
            [&](auto result)
        {
            completed = true;
            if (!result.has_value())
            {
                error = result.error();
            }
        });
        httpClient.Poll();
        EXPECT_TRUE(completed);
        return error;
    }
} // namespace

TEST(MetadataValidationTests, InvalidNamesAreRejectedBeforeSending)
{
    for (const std::string name : {"", "1leading", "has-dash", "has space", "dot.ted", "caf\xC3\xA9"})
    {
        SCOPED_TRACE(name);
        FakeHttpClient httpClient;
        const std::optional<BlobStorageError> error = SetMetadataWithName(httpClient, name);
        ASSERT_TRUE(error.has_value());
        EXPECT_EQ(ValueOrFail(error).Code, std::error_code{BlobStorageErrorCode::InvalidMetadata});
        EXPECT_EQ(ValueOrFail(error).Code, std::errc::invalid_argument);
        EXPECT_TRUE(httpClient.Requests().empty());
    }
}

TEST(MetadataValidationTests, IdentifierNamesAreSent)
{
    for (const std::string name : {"a", "_", "_private", "Name_42", "ALLCAPS"})
    {
        SCOPED_TRACE(name);
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200, {}, {}});
        EXPECT_FALSE(SetMetadataWithName(httpClient, name).has_value());
        ASSERT_EQ(httpClient.Requests().size(), 1U);
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-meta-" + name), "value");
    }
}

TEST(MetadataValidationTests, CaseInsensitiveDuplicateKeysCollapseToOneEntry)
{
    MetadataMap metadata;
    metadata.emplace("Owner", "first");
    const bool inserted = metadata.emplace("OWNER", "second").second;

    EXPECT_FALSE(inserted);
    ASSERT_EQ(metadata.size(), 1U);
    EXPECT_EQ(metadata.begin()->second, "first");
}

TEST(MetadataValidationTests, ValidationAppliesToUploadAndContainerCreate)
{
    FakeHttpClient httpClient;
    BlockBlobClient blockBlob{httpClient, MakeBlobClientOptions()};
    BlobContainerClient container{httpClient, MakeBlobContainerClientOptions()};

    UploadBlockBlobOptions uploadOptions;
    uploadOptions.Metadata.emplace("bad-name", "v");
    std::optional<std::error_code> uploadError;
    blockBlob.UploadAsync(std::string{"content"},
        std::move(uploadOptions),
        [&](auto result)
    {
        uploadError = result.has_value() ? std::error_code{} : result.error().Code;
    });

    CreateBlobContainerOptions createOptions;
    createOptions.Metadata.emplace("9lives", "v");
    std::optional<std::error_code> createError;
    container.CreateAsync(std::move(createOptions),
        [&](auto result)
    {
        createError = result.has_value() ? std::error_code{} : result.error().Code;
    });

    httpClient.Poll();
    EXPECT_EQ(uploadError, std::error_code{BlobStorageErrorCode::InvalidMetadata});
    EXPECT_EQ(createError, std::error_code{BlobStorageErrorCode::InvalidMetadata});
    EXPECT_TRUE(httpClient.Requests().empty());
}
