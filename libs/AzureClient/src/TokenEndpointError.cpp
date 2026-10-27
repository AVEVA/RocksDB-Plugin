// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/TokenEndpointError.hpp"

#include <algorithm>
#include <array>
#include <gsl/util>
#include <string>

namespace AVEVA::AzureClient {
namespace {
// Index 0 is "no recognised error". Values are status * Stride + index, so the set may only be appended to.
constexpr int Stride = 100;
constexpr std::array<std::string_view, 15> KnownErrors{
    "",
    "invalid_request",
    "invalid_client",
    "invalid_grant",
    "unauthorized_client",
    "unsupported_grant_type",
    "invalid_scope",
    "access_denied",
    "interaction_required",
    "consent_required",
    "login_required",
    "temporarily_unavailable",
    "server_error",
    "invalid_resource",
    "unauthorized",
};

class TokenEndpointCategory final : public std::error_category {
  public:
    [[nodiscard]] const char* name() const noexcept override { return "aveva.azure-client.token-endpoint"; }

    [[nodiscard]] std::string message(int value) const override {
        if (value < 0) {
            return "Unrecognised token endpoint error " + std::to_string(value);
        }
        std::string text = "Token endpoint returned HTTP " + std::to_string(value / Stride);
        if (const auto index = static_cast<std::size_t>(value % Stride); index > 0 && index < KnownErrors.size()) {
            text += ": ";
            text += KnownErrors[index];
        }
        return text;
    }

    [[nodiscard]] std::error_condition default_error_condition(int value) const noexcept override {
        if (value < 0) {
            return std::errc::permission_denied;
        }
        const int status = value / Stride;
        if (status == 408 || status == 429) {
            return std::errc::resource_unavailable_try_again;
        }
        return status >= 500 ? std::errc::resource_unavailable_try_again : std::errc::permission_denied;
    }
};
} // namespace

const std::error_category& TokenEndpointErrorCategory() noexcept {
    static const TokenEndpointCategory Category;
    return Category;
}

std::error_code MakeTokenEndpointError(const unsigned int statusCode, const std::string_view oauthError) noexcept {
    const auto found = std::ranges::find(KnownErrors, oauthError);
    const auto index = found == KnownErrors.end() ? 0 : gsl::narrow_cast<int>(found - KnownErrors.begin());
    // Status 0 with no recognised error would encode to value 0, which std::error_code treats as success.
    const int status = gsl::narrow_cast<int>(std::clamp(statusCode, 1U, 999U));
    return {status * Stride + index, TokenEndpointErrorCategory()};
}

std::optional<TokenEndpointFailure> GetTokenEndpointFailure(const std::error_code& code) noexcept {
    if (code.category() != TokenEndpointErrorCategory()) {
        return std::nullopt;
    }
    if (code.value() < 0) {
        return std::nullopt;
    }
    const auto index = static_cast<std::size_t>(code.value() % Stride);
    return TokenEndpointFailure{.StatusCode = gsl::narrow_cast<unsigned int>(code.value() / Stride),
                                .ErrorCode = index < KnownErrors.size() ? KnownErrors[index] : std::string_view{}};
}
} // namespace AVEVA::AzureClient