// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <stdexcept>

using AVEVA::RocksDB::Plugin::Core::LocalFile;

TEST(LocalFileTests, Read_FileThatCouldNotBeOpened_Throws) {
    LocalFile file(std::filesystem::path("this-file-does-not-exist.bin"));
    char buffer[4];

    EXPECT_THROW(file.Read(buffer, 0, 4), std::runtime_error);
}

TEST(LocalFileTests, Read_ReturnsTheBytesAtTheOffset) {
    const auto path = std::filesystem::path("localfile-read-test.bin");
    {
        std::ofstream out(path, std::ios::binary);
        out << "abcdef";
    }
    {
        LocalFile file(path);
        char buffer[3] = {};

        EXPECT_EQ(3, file.Read(buffer, 2, 3));
        EXPECT_EQ(std::string(buffer, 3), "cde");
        // A short read at the end of the file still leaves the handle usable.
        EXPECT_EQ(1, file.Read(buffer, 5, 3));
        EXPECT_EQ(3, file.Read(buffer, 0, 3));
    }
    std::filesystem::remove(path);
}
