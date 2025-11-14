// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstdint>
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class BlobAttributes
    {
        uint64_t m_size;
        std::string m_name;

    public:
        BlobAttributes(uint64_t size, std::string name);
        [[nodiscard]] uint64_t GetSize() const noexcept;
        [[nodiscard]] const std::string& GetName() const noexcept;
    };
}
