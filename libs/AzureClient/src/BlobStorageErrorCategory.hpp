// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <string>
#include <string_view>
#include <system_error>

namespace AVEVA::AzureClient::Private {
/// <summary>
/// The error category for <see cref="BlobStorageErrorCode"/> values, which also maps them onto portable
/// std::errc conditions.
/// </summary>
class BlobStorageErrorCategory final : public std::error_category {
  public:
    /// <summary>
    /// The category's name.
    /// </summary>
    /// <returns>A static, null-terminated name.</returns>
    [[nodiscard]] const char* name() const noexcept override;

    /// <summary>
    /// Describes a code for display.
    /// </summary>
    /// <param name="value">The <see cref="BlobStorageErrorCode"/> value.</param>
    /// <returns>The code's message text.</returns>
    [[nodiscard]] std::string message(int value) const override;

    /// <summary>
    /// Maps a code onto the portable condition callers compare against.
    /// </summary>
    /// <param name="value">The <see cref="BlobStorageErrorCode"/> value.</param>
    /// <returns>The equivalent condition.</returns>
    [[nodiscard]] std::error_condition default_error_condition(int value) const noexcept override;
};

/// <summary>
/// Maps the value of the "x-ms-error-code" response header (e.g.
/// "ContainerAlreadyExists") to a BlobStorageErrorCode, defaulting to ServiceError for
/// unrecognized codes.
/// </summary>
/// <param name="xMsErrorCode">The header value.</param>
/// <returns>The matching code, or <c>BlobStorageErrorCode::ServiceError</c>.</returns>
[[nodiscard]] BlobStorageErrorCode ParseBlobStorageErrorCode(std::string_view xMsErrorCode);

/// <summary>
/// The shared <see cref="BlobStorageErrorCategory"/> instance.
/// </summary>
/// <returns>A reference to the category.</returns>
[[nodiscard]] const std::error_category& BlobStorageErrorCategoryInstance() noexcept;
} // namespace AVEVA::AzureClient::Private
