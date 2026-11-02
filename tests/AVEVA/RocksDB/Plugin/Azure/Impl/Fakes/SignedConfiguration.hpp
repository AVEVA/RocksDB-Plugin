// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SignedBytes.hpp"

#include <cstdint>

// The Impl::Configuration constants as the signed 64-bit offsets that file arithmetic and the fake blob use, so the
// tests that compute page offsets need not cast at every use. Each value is derived from the real constant.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests::SignedConfiguration {
namespace PageBlob {
inline constexpr std::int64_t PageSize = SignedBytes(Configuration::PageBlob::PageSize);
inline constexpr std::int64_t DefaultSize = SignedBytes(::AVEVA::RocksDB::Plugin::Azure::Defaults::DataFileInitialSize);
inline constexpr std::int64_t DefaultBufferSize =
    SignedBytes(::AVEVA::RocksDB::Plugin::Azure::Defaults::DataFileBufferSize);
inline constexpr std::int64_t MaxPageBlobSize = SignedBytes(Configuration::PageBlob::MaxPageBlobSize);
} // namespace PageBlob

namespace Transfer {
inline constexpr std::int64_t MaxCopyChunkSize = SignedBytes(Configuration::Transfer::MaxCopyChunkSize);
inline constexpr auto MaxConcurrentPageCopies = Configuration::Transfer::MaxConcurrentPageCopies;
} // namespace Transfer

inline constexpr auto MetadataCacheTtl = Configuration::MetadataCacheTtl;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests::SignedConfiguration
