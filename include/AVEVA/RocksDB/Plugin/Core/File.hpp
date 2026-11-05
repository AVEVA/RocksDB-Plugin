// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"
#include <span>
namespace AVEVA::RocksDB::Plugin::Core {
class File {
  public:
    virtual ~File() = default;

    // Fills `buffer` with the bytes starting at `offset` and returns how many were read (fewer at the end of the
    // file). Implementations must allow concurrent calls on one instance, because the file cache shares one open file
    // between its readers; no type can prove an implementation thread-safe, so this stays a documented contract.
    virtual ByteCount Read(std::span<char> buffer, ByteOffset offset) = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Core
