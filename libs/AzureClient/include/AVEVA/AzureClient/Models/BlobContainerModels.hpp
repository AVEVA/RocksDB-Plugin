// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/Models/BlobModels.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace AVEVA::AzureClient::Models
{
    struct CreateBlobContainerResult
    {
        std::string ETag;
        std::chrono::system_clock::time_point LastModified;
    };
} // namespace AVEVA::AzureClient::Models
