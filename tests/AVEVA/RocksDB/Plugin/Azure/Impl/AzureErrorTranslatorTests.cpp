// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <azure/core/exception.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::AzureErrorTranslator;
using AVEVA::RocksDB::Plugin::Azure::ReadableFile;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using ::Azure::Core::Http::HttpStatusCode;
using ::testing::_;
using ::testing::Return;
using ::testing::Throw;

TEST(AzureErrorTranslatorTests, NotFound_ReturnsPathNotFound) {
    const auto status = AzureErrorTranslator::IOStatusFromError("missing.log", HttpStatusCode::NotFound);

    // RocksDB's secondary only tolerates a WAL purged by the primary when IsPathNotFound() is true
    EXPECT_TRUE(status.IsPathNotFound());
    EXPECT_TRUE(status.IsNotFound());
    EXPECT_FALSE(status.ok());
}

TEST(AzureErrorTranslatorTests, OtherStatusCodes_AreNotPathNotFound) {
    EXPECT_FALSE(AzureErrorTranslator::IOStatusFromError("x", HttpStatusCode::BadRequest).IsNotFound());
    EXPECT_FALSE(AzureErrorTranslator::IOStatusFromError("x", HttpStatusCode::PreconditionFailed).IsNotFound());
    EXPECT_FALSE(AzureErrorTranslator::IOStatusFromError("x", HttpStatusCode::InternalServerError).IsNotFound());
    EXPECT_TRUE(AzureErrorTranslator::IOStatusFromError("x", HttpStatusCode::ServiceUnavailable).IsBusy());
}

TEST(AzureErrorTranslatorTests, ReadableFile_BlobDeletedDuringRead_ReturnsPathNotFound) {
    // Arrange
    auto blobClient = std::make_shared<BlobClientMock>();
    ON_CALL(*blobClient, GetSize()).WillByDefault(Return(Configuration::PageBlob::PageSize));
    ON_CALL(*blobClient, GetEtag()).WillByDefault(Return(::Azure::ETag{"etag"}));

    ::Azure::Core::RequestFailedException notFound{"The specified blob does not exist."};
    notFound.StatusCode = HttpStatusCode::NotFound;
    EXPECT_CALL(*blobClient, Download(_, _, _, _)).WillOnce(Throw(notFound));

    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    ReadableFile file{ReadableFileImpl{"000123.log", blobClient, nullptr, logger}};
    std::vector<char> buffer(64);
    rocksdb::Slice result;

    // Act
    const auto sequential = file.Read(buffer.size(), rocksdb::IOOptions{}, &result, buffer.data(), nullptr);

    // Assert
    EXPECT_TRUE(sequential.IsPathNotFound());
}
