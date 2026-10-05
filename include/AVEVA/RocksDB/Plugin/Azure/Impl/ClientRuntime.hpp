// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

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
/// Holds the HTTP client that every Azure Blob Storage client of a filesystem uses, bound to an io_context
/// owned and run by the host application. The runtime never runs, stops or destroys that io_context: the host
/// must keep it alive and running on at least one thread for as long as the runtime exists.
/// The AVEVA Azure clients only hold a reference to the HTTP client, so every object that owns such a
/// client also shares ownership of the runtime to keep it alive for as long as the client exists.
/// Operations are started with boost::asio::use_future and waited for on the caller's thread, which
/// must never be one of the threads running the io_context.
/// </summary>
class ClientRuntime : public std::enable_shared_from_this<ClientRuntime> {
    std::unique_ptr<::AVEVA::IHttpClient> m_httpClient;

  public:
    explicit ClientRuntime(boost::asio::io_context& context);
    ~ClientRuntime();
    ClientRuntime(const ClientRuntime&) = delete;
    ClientRuntime& operator=(const ClientRuntime&) = delete;
    ClientRuntime(ClientRuntime&&) = delete;
    ClientRuntime& operator=(ClientRuntime&&) = delete;

    [[nodiscard]] ::AVEVA::IHttpClient& HttpClient() const noexcept;
};

[[noreturn]] void ThrowRequestFailed(const AzureClient::BlobStorageError& error);

/// <summary>
/// Request options for a request transferring `bytes` of payload: raises the response body limit so the
/// payload fits, and extends the timeout so that large transfers on slow links are not cut short.
/// </summary>
[[nodiscard]] HttpRequestOptions RequestOptionsForTransfer(const HttpRequestOptions& defaults, uint64_t bytes);

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
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
