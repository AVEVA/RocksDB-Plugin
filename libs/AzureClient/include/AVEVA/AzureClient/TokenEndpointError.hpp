// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpStatusCode.hpp>

#include <optional>
#include <string_view>
#include <system_error>

namespace AVEVA::AzureClient {
/// <summary>
/// A failed token acquisition is reported as a std::error_code, so the HTTP status of the identity endpoint
/// and its OAuth <c>error</c> value travel inside the code itself (category <c>TokenEndpointErrorCategory</c>). The
/// clients copy them into <c>BlobStorageError::StatusCode</c> and <c>ErrorCode</c>. Only a fixed set of well-known
/// <c>error</c> values is kept; anything else is reported with the status alone. <c>error_description</c>, the
/// body and tokens are never carried.
/// </summary>
struct TokenEndpointFailure {
    /// <summary>The HTTP status the token endpoint answered with.</summary>
    HttpStatusCode StatusCode;
    /// <summary>
    /// The OAuth <c>error</c> value; empty when the endpoint sent none, or one outside the known set.
    /// </summary>
    std::string_view ErrorCode;
};

/// <summary>
/// The error category of codes made by <see cref="MakeTokenEndpointError"/>. Its conditions map server-side
/// statuses, 408 and 429 to <c>std::errc::resource_unavailable_try_again</c> and everything else to
/// <c>std::errc::permission_denied</c>.
/// </summary>
/// <returns>The process-wide category instance.</returns>
[[nodiscard]] const std::error_category& TokenEndpointErrorCategory() noexcept;

/// <summary>
/// The error_code for a non-2xx response from a token endpoint.
/// </summary>
/// <param name="statusCode">The HTTP status of the response; clamped to 1..999 so the code can never be zero, which
/// std::error_code treats as success.</param>
/// <param name="oauthError">The JSON <c>error</c> field. A value outside the known set is dropped.</param>
/// <returns>A code in <see cref="TokenEndpointErrorCategory"/> carrying the status and the recognised error.</returns>
[[nodiscard]] std::error_code MakeTokenEndpointError(HttpStatusCode statusCode, std::string_view oauthError) noexcept;

/// <summary>
/// The status and OAuth error behind a code.
/// </summary>
/// <param name="code">The code to take apart.</param>
/// <returns>The failure; empty when <paramref name="code"/> did not come from a token endpoint.</returns>
[[nodiscard]] std::optional<TokenEndpointFailure> GetTokenEndpointFailure(const std::error_code& code) noexcept;
} // namespace AVEVA::AzureClient