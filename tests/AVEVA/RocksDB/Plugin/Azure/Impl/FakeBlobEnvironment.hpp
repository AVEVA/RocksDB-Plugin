// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "FakeBlobServer.hpp"
#include "FakeHttpClient.hpp"

#include <boost/asio/io_context.hpp>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
using FakeReadableFileImpl = BasicReadableFileImpl<FakeBlobServer::BlobClientType>;
using FakeWriteableFileImpl = BasicWriteableFileImpl<FakeBlobServer::BlobClientType>;
using FakeReadWriteFileImpl = BasicReadWriteFileImpl<FakeBlobServer::BlobClientType>;
using FakeSequentialFileImpl = BasicSequentialFileImpl<FakeBlobServer::BlobClientType>;
using FakeReadableFile = BasicReadableFile<FakeReadableFileImpl>;

// Everything a file object needs to run against an in-memory blob: the library's real page blob client talking to a
// FakeBlobServer through the fake HTTP transport. `Blob` is the server (the blob's state and the requests it saw);
// `Client` is what the file classes are constructed with. Completions run inline on the calling thread, and a
// background pump runs the ones the client posts itself (validation failures, download sink writes).
//
// Members are declared so that teardown runs in the production order: the client is released before the transport,
// which is released before the runtime.
struct FakeBlobEnvironment {
    boost::asio::io_context Context;
    // The runtime owns the fake transport, as it owns the real HTTP client in production.
    std::shared_ptr<BasicClientRuntime<FakeHttpClient>> Runtime =
        std::make_shared<BasicClientRuntime<FakeHttpClient>>(std::make_unique<FakeHttpClient>());
    FakeHttpClient& Http = Runtime->HttpClient();
    std::shared_ptr<FakeBlobServer> Blob = std::make_shared<FakeBlobServer>(Http);
    std::shared_ptr<FakeBlobServer::BlobClientType> Client = Blob->Client;

    FakeBlobEnvironment() {
        Http.CompleteInline() = true;
        Http.StartBackgroundPump();
    }

    // Makes the blob hold `size` bytes of `fill`, with that size also recorded in its metadata.
    void Fill(const int64_t size, const char fill = 'x') {
        Blob->Data.assign(static_cast<size_t>(size), fill);
        Blob->Size = size;
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
