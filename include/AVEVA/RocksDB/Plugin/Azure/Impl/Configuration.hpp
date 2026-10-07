// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstdint>
#include <chrono>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    struct Configuration
    {
        struct PageBlob
        {
            static const constexpr int64_t PageSize = 512;
            static const constexpr int64_t PageBits = 9;
            static const constexpr int64_t DefaultSize = 128 * PageSize * 2;
            static const constexpr int64_t DefaultBufferSize = 128 * PageSize * 2;
        };

        static const constexpr std::chrono::seconds LeaseLength = std::chrono::seconds(20);
        static const constexpr std::chrono::seconds RenewalDelay = std::chrono::seconds(5);
        // Writes stop this long before the lease can expire, so one already in flight cannot land after it lapsed.
        static const constexpr std::chrono::milliseconds LeaseSafetyMargin = std::chrono::milliseconds(2000);
        // How long the destructor waits for in-flight async reads before warning (it keeps waiting afterwards).
        static const constexpr std::chrono::seconds AsyncReadDrainWarning = std::chrono::seconds(30);
        static const constexpr std::chrono::seconds LogRateLimiterCooldown = std::chrono::seconds(30);
        static const constexpr size_t MaxCacheSize = static_cast<size_t>(1024) * 1024 * 1024; // 1GB
        static const constexpr int MaxClientRetries = 8;
    };
}
