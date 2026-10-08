// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <string>
#include <string_view>
#include <system_error>

namespace AVEVA::AzureClient::Private
{
    class BlobStorageErrorCategory final : public std::error_category
    {
      public:
        [[nodiscard]] const char* name() const noexcept override;
        [[nodiscard]] std::string message(int value) const override;
        [[nodiscard]] std::error_condition default_error_condition(int value) const noexcept override;
    };

    // Maps the value of the "x-ms-error-code" response header (e.g.
    // "ContainerAlreadyExists") to a BlobStorageErrorCode, defaulting to ServiceError for
    // unrecognized codes.
    [[nodiscard]] BlobStorageErrorCode ParseBlobStorageErrorCode(std::string_view xMsErrorCode);
    [[nodiscard]] const std::error_category& BlobStorageErrorCategoryInstance() noexcept;
} // namespace AVEVA::AzureClient::Private
