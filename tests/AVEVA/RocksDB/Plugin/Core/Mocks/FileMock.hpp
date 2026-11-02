// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Core::Mocks {
class FileMock : public File {
  public:
    FileMock();
    virtual ~FileMock();

    // Tests set expectations on this plain-number method; the typed override adapts to it.
    MOCK_METHOD(int64_t, Read, (char* buffer, int64_t offset, int64_t length));

    ByteCount Read(char* buffer, ByteOffset offset, ByteCount length) override {
        return ByteCount{static_cast<std::uint64_t>(
            Read(buffer, static_cast<int64_t>(offset.Value()), static_cast<int64_t>(length.Value())))};
    }
};
} // namespace AVEVA::RocksDB::Plugin::Core::Mocks
