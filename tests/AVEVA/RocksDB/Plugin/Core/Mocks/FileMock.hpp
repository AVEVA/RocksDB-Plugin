// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/File.hpp"
#include <gmock/gmock.h>

namespace AVEVA::RocksDB::Plugin::Core::Mocks
{
    class FileMock : public File
    {
    public:
        FileMock();
        virtual ~FileMock();

        MOCK_METHOD(int64_t, Read, (char* buffer, int64_t offset, int64_t length), (override));
    };
}
