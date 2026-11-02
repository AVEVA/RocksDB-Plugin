// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "BackgroundPump.hpp"
#include "BasicReadableFile.hpp"
#include "FakeBlobServer.hpp"
#include "FakeHttpClient.hpp"

#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <thread>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
using FakeReadableFileImpl = BasicReadableFileImpl<FakeBlobServer::BlobClientType>;
using FakeWriteableFileImpl = BasicWriteableFileImpl<FakeBlobServer::BlobClientType>;
using FakeReadWriteFileImpl = BasicReadWriteFileImpl<FakeBlobServer::BlobClientType>;
using FakeSequentialFileImpl = BasicSequentialFileImpl<FakeBlobServer::BlobClientType>;
using FakeReadableFile = BasicReadableFile<FakeReadableFileImpl>;

// A static-dispatch seam for MultiRead's planning phase: every completed-prefetch probe takes `ProbeDelay`.
struct SlowProbeReadableFileImpl : FakeReadableFileImpl {
    using FakeReadableFileImpl::FakeReadableFileImpl;
    std::chrono::milliseconds ProbeDelay{0};

    [[nodiscard]] std::optional<size_t> TryReadFromCompletedPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                                     char* buffer,
                                                                     uint64_t* const deferredRetire = nullptr) const {
        const auto result =
            FakeReadableFileImpl::TryReadFromCompletedPrefetch(offset, bytesToRead, buffer, deferredRetire);
        std::this_thread::sleep_for(ProbeDelay);
        return result;
    }
};
using SlowProbeReadableFile = BasicReadableFile<SlowProbeReadableFileImpl>;

// Everything a file object needs to run against an in-memory blob: the library's real page blob client talking to a
// FakeBlobServer through the fake HTTP transport. `Blob` is the server (the blob's state and the requests it saw);
// `Client` is what the file classes are constructed with. A background pump drives the transport's io_context, so
// every completion runs on that thread while the test thread blocks in the file classes.
//
// Members are declared so that teardown runs in the production order: the client is released before the transport,
// which is released before the runtime.
struct FakeBlobEnvironment {
    // Declared first so it outlives the runtime whose clock reads it.
    std::atomic<int64_t> m_metadataClockNanos{1'000'000'000'000};
    boost::asio::io_context Context;
    // The runtime owns the fake transport, as it owns the real HTTP client in production.
    std::shared_ptr<BasicClientRuntime<FakeHttpClient>> Runtime =
        std::make_shared<BasicClientRuntime<FakeHttpClient>>(std::make_unique<FakeHttpClient>());
    FakeHttpClient& Http = Runtime->HttpClient();
    // Stopped after the client and server are released and before the runtime, which owns the transport it drives.
    BackgroundPump Pump{Http};
    std::shared_ptr<FakeBlobServer> Blob;
    std::shared_ptr<FakeBlobServer::BlobClientType> Client;

    explicit FakeBlobEnvironment(const std::string& container = "files", const std::string& blob = "test.sst")
        : Blob(std::make_shared<FakeBlobServer>(Http, container, blob)), Client(Blob->Client) {
        Runtime->SetMetadataClockForTesting(&m_metadataClockNanos);
    }

    // The metadata and size caches read a manual clock, so their TTL expires only when a test says so.
    void AdvanceMetadataClock(const std::chrono::steady_clock::duration by) {
        m_metadataClockNanos += std::chrono::duration_cast<std::chrono::nanoseconds>(by).count();
    }

    // Makes the blob hold `size` bytes of `fill`, with that size also recorded in its metadata.
    void Fill(const int64_t size, const char fill = 'x') {
        Blob->WithPrimaryBlob([&](auto& b) { b.Data.assign(static_cast<size_t>(size), fill); });
        Blob->SetSize(size);
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
