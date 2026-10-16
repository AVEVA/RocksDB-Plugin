// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <optional>
#include <string_view>
#include <system_error>

namespace AVEVA::AzureClient {
// A failed token acquisition is reported as a std::error_code, so the HTTP status of the identity endpoint
// and its OAuth `error` value travel inside the code itself (category TokenEndpointErrorCategory). The clients
// copy them into BlobStorageError::StatusCode and ErrorCode. Only a fixed set of well-known `error` values is
// kept; anything else is reported with the status alone. `error_description`, the body and tokens are never
// carried.
struct TokenEndpointFailure {
    unsigned int StatusCode = 0;
    std::string_view ErrorCode; // Empty when the endpoint sent none, or one outside the known set.
};

[[nodiscard]] const std::error_category& TokenEndpointErrorCategory() noexcept;

// The error_code for a non-2xx response from a token endpoint. `oauthError` is the JSON `error` field.
[[nodiscard]] std::error_code MakeTokenEndpointError(unsigned int statusCode, std::string_view oauthError) noexcept;

// The status and OAuth error behind `code`, or nullopt when it did not come from a token endpoint.
[[nodiscard]] std::optional<TokenEndpointFailure> GetTokenEndpointFailure(const std::error_code& code) noexcept;
} // namespace AVEVA::AzureClient