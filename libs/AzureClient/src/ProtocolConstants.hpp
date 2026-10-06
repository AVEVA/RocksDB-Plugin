#pragma once

#include "AVEVA/AzureClient/Sas.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace AVEVA::AzureClient::Private
{
    // Client-side retry policy (not an Azure service limit).
    struct RetryPolicyConstants
    {
        // Multiplicative jitter range applied to each exponential backoff delay, to de-synchronize retries.
        static constexpr double MinBackoffJitter = 0.8;
        static constexpr double MaxBackoffJitter = 1.3;
        // Keeps `InitialDelay * 2^n` representable even for absurd retry counts.
        static constexpr int MaxBackoffExponent = 62;
    };

    // Azure Blob Storage size limits and client transfer defaults.
    struct BlobTransferLimits
    {
        // Client default: parallel download chunk size when DownloadToOptions::ChunkSize is 0.
        static constexpr std::size_t DefaultDownloadChunkSize = std::size_t{4} * 1024U * 1024U;
        // Client policy: headroom on top of the requested range length for each response body limit.
        static constexpr std::uint64_t ResponseBodySlack = std::uint64_t{64} * 1024U;
    };

    // Shared Access Signature protocol versions.
    struct SasProtocolConstants
    {
        // Signed service version (`sv`) emitted by the SAS builders.
        static constexpr std::string_view Version = AVEVA::AzureClient::Sas::SasVersion;
    };
} // namespace AVEVA::AzureClient::Private
