// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Models/BlobModels.hpp>

#include "BlobRequestHelpers.hpp"

#include <cstdint>
#include <format>
#include <span>
#include <string>

namespace AVEVA::AzureClient::Models
{
    std::string EncodeBlockId(std::uint64_t index)
    {
        const std::string value = std::format("{:016}", index);
        return Private::Base64Encode(std::as_bytes(std::span{value.data(), value.size()}));
    }
} // namespace AVEVA::AzureClient::Models
