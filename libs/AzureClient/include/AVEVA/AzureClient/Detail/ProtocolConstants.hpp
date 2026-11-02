// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace AVEVA::AzureClient::Private {
/// <summary>Client-side retry policy (not an Azure service limit).</summary>
struct RetryPolicyConstants {
    /// <summary>
    /// Lower bound of the multiplicative jitter range applied to each exponential backoff delay, to de-synchronize
    /// retries.
    /// </summary>
    static constexpr double MinBackoffJitter = 0.8;
    /// <summary>
    /// Upper bound of the multiplicative jitter range applied to each exponential backoff delay, to de-synchronize
    /// retries.
    /// </summary>
    static constexpr double MaxBackoffJitter = 1.3;
    /// <summary>Keeps <c>InitialDelay * 2^n</c> representable even for absurd retry counts.</summary>
    static constexpr int MaxBackoffExponent = 62;
};

/// <summary>Azure Blob Storage size limits and client transfer defaults.</summary>
struct BlobTransferLimits {
    /// <summary>
    /// Client default: parallel download chunk size when <c>DownloadToOptions::ChunkSize</c> is 0.
    /// </summary>
    static constexpr std::size_t DefaultDownloadChunkSize = std::size_t{4} * 1024U * 1024U;
    /// <summary>Client policy: headroom on top of the requested range length for each response body limit.</summary>
    static constexpr std::uint64_t ResponseBodySlack = std::uint64_t{64} * 1024U;
    /// <summary>
    /// Client policy: chunks in flight, buffered out of order or being written never exceed this many bytes, so a
    /// large ChunkSize combined with a high Concurrency cannot exhaust memory.
    /// </summary>
    static constexpr std::uint64_t MaxBufferedDownloadBytes = std::uint64_t{256} * 1024U * 1024U;
};
} // namespace AVEVA::AzureClient::Private
