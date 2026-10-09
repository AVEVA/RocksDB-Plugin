// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// Mocks the type-erased operation entry points that the public ...Async templates dispatch to, so every completion
// token (use_future, callbacks, ...) keeps working against the mock. Operations that are not mocked here still run
// the real implementation against the HTTP client the mock was constructed with.
class PageBlobClientMock : public AzureClient::PageBlobClient {
  public:
    using AzureClient::PageBlobClient::PageBlobClient;

    MOCK_METHOD(void, GetPropertiesAsyncImpl,
                (const AzureClient::GetBlobPropertiesOptions& options, GetPropertiesCompletionHandler completion,
                 AVEVA::HttpRequestOptions requestOptions),
                (override));
    MOCK_METHOD(void, ResizeAsyncImpl,
                (std::uint64_t newSize, const AzureClient::ResizePageBlobOptions& options,
                 ResizeCompletionHandler completion, AVEVA::HttpRequestOptions requestOptions),
                (override));
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
