// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include <string>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Core
{
    class ContainerClient
    {
    public:
        ContainerClient() = default;
        virtual ~ContainerClient() = default;

        virtual std::unique_ptr<BlobClient> GetBlobClient(const std::string& path) = 0;
    };
}
