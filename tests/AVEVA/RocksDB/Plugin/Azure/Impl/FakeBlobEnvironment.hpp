// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "FakePageBlobClient.hpp"

#include <boost/asio/io_context.hpp>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
using FakeReadableFileImpl = BasicReadableFileImpl<FakePageBlobClient>;
using FakeWriteableFileImpl = BasicWriteableFileImpl<FakePageBlobClient>;
using FakeReadWriteFileImpl = BasicReadWriteFileImpl<FakePageBlobClient>;
using FakeSequentialFileImpl = BasicSequentialFileImpl<FakePageBlobClient>;
using FakeReadableFile = BasicReadableFile<FakeReadableFileImpl>;

// Everything a file object needs to run against an in-memory blob. The runtime is declared before the blob so teardown
// order matches production (the blob is released before the runtime that backs it).
struct FakeBlobEnvironment {
    boost::asio::io_context Context;
    std::shared_ptr<ClientRuntime> Runtime = std::make_shared<ClientRuntime>(Context);
    std::shared_ptr<FakePageBlobClient> Blob = std::make_shared<FakePageBlobClient>(Runtime->HttpClient());

    // Makes the blob hold `size` bytes of `fill`, with that size also recorded in its metadata.
    void Fill(const int64_t size, const char fill = 'x') {
        Blob->Data.assign(static_cast<size_t>(size), fill);
        Blob->Size = size;
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
