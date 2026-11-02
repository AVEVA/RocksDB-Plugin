// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"
namespace AVEVA::RocksDB::Plugin::Core {
class File {
  public:
    virtual ~File() = default;

    // Reads up to `length` bytes starting at `offset` into `buffer` and returns how many were read (fewer at the
    // end of the file). Implementations must allow concurrent calls on one instance.
    virtual ByteCount Read(char* buffer, ByteOffset offset, ByteCount length) = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Core
