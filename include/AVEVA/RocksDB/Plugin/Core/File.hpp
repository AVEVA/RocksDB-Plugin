// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstdint>
namespace AVEVA::RocksDB::Plugin::Core
{
    class File
    {
    public:
        virtual ~File() = default;

        virtual int64_t Read(char* buffer, int64_t offset, int64_t length) = 0;
    };
}
