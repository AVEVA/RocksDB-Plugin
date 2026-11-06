// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/Outcomes.hpp>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Transient when retrying the same request may succeed: 408, 429, 5xx, and transport failures. Delegates to the
/// client's retry policy so the plugin never disagrees with it.
/// </summary>
[[nodiscard]] AzureClient::Transience TransienceOf(const RequestFailedException& error);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
