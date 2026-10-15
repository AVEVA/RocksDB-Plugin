// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Deliberately includes only the public headers, and nothing else, before checking what they dragged in.
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Directory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/LockFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/SequentialFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"

#include <gtest/gtest.h>

#include <type_traits>

#ifdef BOOST_ASIO_VERSION
#error "The public headers must not include Boost.Asio."
#endif
#ifdef AVEVA_AZURE_CLIENT_INCLUDED
#error "The public headers must not include the Azure client."
#endif

TEST(PublicHeadersTests, ReadableFile_IsANonTemplateRandomAccessFile) {
    static_assert(std::is_base_of_v<rocksdb::FSRandomAccessFile, AVEVA::RocksDB::Plugin::Azure::ReadableFile>);
    SUCCEED();
}
