// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlockOn.hpp>
#include <AVEVA/AzureClient/Response.hpp>

#include <cstdint>
#include <expected>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>Builds the exception ThrowRequestFailed would throw, for error paths that hand it on as an
/// exception_ptr without throwing.</summary>
[[nodiscard]] RequestFailedException MakeRequestFailed(const AzureClient::BlobStorageError& error);

[[noreturn]] void ThrowRequestFailed(const AzureClient::BlobStorageError& error);

/// <summary>
/// Request options for a request transferring `bytes` of payload: raises the response body limit so the
/// payload fits, and extends the timeout so that large transfers on slow links are not cut short.
/// </summary>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions
RequestOptionsForTransfer(const AVEVA::AzureClient::AzureRequestOptions& defaults, uint64_t bytes);

/// Request options for moving `bytes` through `client`: the client's defaults with the timeout and response limit
/// scaled to the transfer size.
template <typename Client>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions TransferOptions(const Client& client, uint64_t bytes) {
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
/// failure. The library's block_on token checks the thread before the request starts. `start` receives the completion
/// token, e.g.
/// `Call([&](auto token) { return c.GetPropertiesAsync(token); })`.
/// </summary>
template <class Start> auto Call(Start&& start) { return Unwrap(std::forward<Start>(start)(AzureClient::block_on)); }

/// <summary>As Call, but returns the full response (for operations whose Response::Error() carries
/// information).</summary>
template <class Start> auto CallResponse(Start&& start) {
    return UnwrapResponse(std::forward<Start>(start)(AzureClient::block_on));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
