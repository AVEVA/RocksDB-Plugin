// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/BlobClientOptions.hpp>

#include <type_traits>

// Checks the header is self-contained.
static_assert(std::is_default_constructible_v<AVEVA::AzureClient::BlobClientOptions>);
