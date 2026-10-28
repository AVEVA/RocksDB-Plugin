// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <boost/asio/io_context.hpp>

#include <memory>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// How a transport is created on the host's io_context. Any transport constructible from an io_context works as is;
/// the production one is specialised (in ClientRuntime.cpp) to configure connection pooling and trust roots.
/// </summary>
template <AzureClient::HttpClientLike THttp> struct HttpClientFactory {
    static std::shared_ptr<THttp> Create(boost::asio::io_context& context) { return std::make_shared<THttp>(context); }
};
template <> struct HttpClientFactory<::AVEVA::HttpClient> {
    static std::shared_ptr<::AVEVA::HttpClient> Create(boost::asio::io_context& context);
};

/// <summary>
/// Holds the HTTP client that every Azure Blob Storage client of a filesystem uses, bound to an io_context
/// owned and run by the host application. The runtime never runs, stops or destroys that io_context: the host
/// must keep it alive and running on at least one thread for as long as the runtime exists.
/// The HTTP client is shared with the Azure clients and credentials, which keep it alive for as long as they exist.
/// Operations are run as coroutines on the transport's executor and waited for on the caller's thread, which
/// must never be one of the threads running the io_context.
/// </summary>
template <AzureClient::HttpClientLike THttp> class BasicClientRuntime : public ClientRuntimeBase {
    std::shared_ptr<THttp> m_httpClient;

  public:
    /// <summary>    Shares an already constructed transport (the way tests inject a fake).</summary>
    explicit BasicClientRuntime(std::shared_ptr<THttp> httpClient) : m_httpClient(std::move(httpClient)) {}
    /// <summary>Creates the transport on the host's io_context through HttpClientFactory.</summary>
    explicit BasicClientRuntime(boost::asio::io_context& context)
        : m_httpClient(HttpClientFactory<THttp>::Create(context)) {}

    [[nodiscard]] THttp& HttpClient() const noexcept { return *m_httpClient; }
    /// <summary>The owning handle to pass to Azure clients and credentials.</summary>
    [[nodiscard]] const std::shared_ptr<THttp>& SharedHttpClient() const noexcept { return m_httpClient; }
};

/// <summary>The production runtime, over the real HTTP transport.</summary>
using ClientRuntime = BasicClientRuntime<::AVEVA::HttpClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
