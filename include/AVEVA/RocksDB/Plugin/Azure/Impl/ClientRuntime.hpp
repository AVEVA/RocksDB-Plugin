// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <atomic>
#include <cstdint>
#include <expected>
#include <memory>
#include <utility>

#ifdef _WIN32
// The target is built with _WIN32_WINNT=0x0A00 (exported publicly) and Boost.Asio's ABI depends on it.
#if defined(_WIN32_WINNT) && _WIN32_WINNT < 0x0A00
#error "aveva-rocksdb-plugin-azure-impl requires _WIN32_WINNT >= 0x0A00 (Windows 10)."
#endif
// Boost.Asio pulls in <windows.h>, whose macros clash with RocksDB method names (mirrors rocksdb/env.h).
#undef DeleteFile
#undef GetCurrentTime
#undef GetFreeSpace
#undef LoadLibrary
#endif
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// What every file, lock and blob operation needs from the runtime, independent of the HTTP transport: shared
/// ownership that keeps the HTTP client alive for as long as an Azure client that borrows it exists, and the write
/// fence. Kept non-template so those classes need not know which transport they run on.
/// </summary>
class ClientRuntimeBase {
    std::atomic<bool> m_writesFenced{false};

  public:
    ClientRuntimeBase() = default;
    virtual ~ClientRuntimeBase() = default;
    ClientRuntimeBase(const ClientRuntimeBase&) = delete;
    ClientRuntimeBase& operator=(const ClientRuntimeBase&) = delete;
    ClientRuntimeBase(ClientRuntimeBase&&) = delete;
    ClientRuntimeBase& operator=(ClientRuntimeBase&&) = delete;

    /// <summary>
    /// Makes every later mutation through this runtime's blobs fail. Called once the filesystem can no longer
    /// prove it still holds its locks, so files opened earlier stop writing too. Irreversible.
    /// </summary>
    void FenceWrites() noexcept;
    [[nodiscard]] bool WritesFenced() const noexcept;
    /// <summary>Throws std::runtime_error when writes are fenced.</summary>
    void ThrowIfWritesFenced() const;
};

/// <summary>
/// Holds the HTTP client that every Azure Blob Storage client of a filesystem uses, bound to an io_context
/// owned and run by the host application. The runtime never runs, stops or destroys that io_context: the host
/// must keep it alive and running on at least one thread for as long as the runtime exists.
/// The AVEVA Azure clients only hold a reference to the HTTP client, so every object that owns such a
/// client also shares ownership of the runtime to keep it alive for as long as the client exists.
/// Operations are started with boost::asio::use_future and waited for on the caller's thread, which
/// must never be one of the threads running the io_context.
/// </summary>
template <AzureClient::HttpClientLike THttp> class BasicClientRuntime : public ClientRuntimeBase {
    std::unique_ptr<THttp> m_httpClient;

  public:
    /// <summary>Takes ownership of an already constructed transport (the way tests inject a fake).</summary>
    explicit BasicClientRuntime(std::unique_ptr<THttp> httpClient) : m_httpClient(std::move(httpClient)) {}
    /// <summary>
    /// Creates the production transport on the host's io_context. Defined (in ClientRuntime.cpp) only for
    /// ::AVEVA::HttpClient.
    /// </summary>
    explicit BasicClientRuntime(boost::asio::io_context& context);

    [[nodiscard]] THttp& HttpClient() const noexcept { return *m_httpClient; }
};

template <> BasicClientRuntime<::AVEVA::HttpClient>::BasicClientRuntime(boost::asio::io_context& context);

/// <summary>The production runtime, over the real HTTP transport.</summary>
using ClientRuntime = BasicClientRuntime<::AVEVA::HttpClient>;

[[noreturn]] void ThrowRequestFailed(const AzureClient::BlobStorageError& error);

/// <summary>
/// Request options for a request transferring `bytes` of payload: raises the response body limit so the
/// payload fits, and extends the timeout so that large transfers on slow links are not cut short.
/// </summary>
[[nodiscard]] HttpRequestOptions RequestOptionsForTransfer(const HttpRequestOptions& defaults, uint64_t bytes);

/// Request options for moving `bytes` through `client`: the client's defaults with the timeout and response limit
/// scaled to the transfer size.
template <typename Client> [[nodiscard]] HttpRequestOptions TransferOptions(const Client& client, uint64_t bytes) {
    return RequestOptionsForTransfer(client.GetDefaultRequestOptions(), bytes);
}

/// <summary>
/// Returns the full response of a completed operation, or throws RequestFailedException on failure.
/// Use this for operations whose Response::Error() carries information (e.g. DeleteIfExists).
/// </summary>
template <class T>
AzureClient::Response<T> UnwrapResponse(std::expected<AzureClient::Response<T>, AzureClient::BlobStorageError> result) {
    if (!result.has_value()) {
        ThrowRequestFailed(result.error());
    }

    return std::move(*result);
}

/// <summary>
/// Returns the value of a completed operation, or throws RequestFailedException on failure.
/// </summary>
template <class T> T Unwrap(std::expected<AzureClient::Response<T>, AzureClient::BlobStorageError> result) {
    return std::move(UnwrapResponse(std::move(result))).Value();
}

/// <summary>
/// Starts an operation, waits for it on the calling thread and returns its value, throwing RequestFailedException on
/// failure. The thread is checked before the request starts. `start` receives the completion token, e.g.
/// `Call(c, [&](auto token) { return c.GetPropertiesAsync(token); })`.
/// </summary>
template <class Client, class Start> auto Call(Client& client, Start&& start) {
    return Unwrap(BlockOn(client, std::forward<Start>(start)));
}

/// <summary>As Call, but returns the full response (for operations whose Response::Error() carries
/// information).</summary>
template <class Client, class Start> auto CallResponse(Client& client, Start&& start) {
    return UnwrapResponse(BlockOn(client, std::forward<Start>(start)));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
