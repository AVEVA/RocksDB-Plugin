// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include "FakeHttpClient.hpp"
#include "FakePageBlobClient.hpp"

#include <boost/asio/io_context.hpp>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// Everything a file object needs to run against an in-memory blob. The runtime is declared after the HTTP client and
// before the blob so teardown order matches production (the blob is released before the runtime that backs it).
struct FakeBlobEnvironment {
    boost::asio::io_context Context;
    AzureClient::Tests::FakeHttpClient Http;
    std::shared_ptr<ClientRuntime> Runtime = std::make_shared<ClientRuntime>(Context);
    std::shared_ptr<FakePageBlobClient> Blob = std::make_shared<FakePageBlobClient>(Http);

    // Makes the blob hold `size` bytes of `fill`, with that size also recorded in its metadata.
    void Fill(const int64_t size, const char fill = 'x') {
        Blob->Data.assign(static_cast<size_t>(size), fill);
        Blob->Size = size;
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
