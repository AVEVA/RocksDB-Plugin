// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LocalFile.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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

TEST(LocalFileTests, Read_ConcurrentReadsOfDisjointRanges_ReturnTheirOwnBytes) {
    const auto path = std::filesystem::path("localfile-concurrent-test.bin");
    constexpr int kThreads = 8;
    constexpr int kChunk = 4096;
    std::string content(static_cast<size_t>(kThreads) * kChunk, '\0');
    for (size_t i = 0; i < content.size(); ++i) {
        content[i] = static_cast<char>('a' + (i / kChunk));
    }
    {
        std::ofstream out(path, std::ios::binary);
        out << content;
    }
    {
        LocalFile file(path);
        std::atomic<int> failures{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&, t] {
                std::string buffer(kChunk, '\0');
                for (int i = 0; i < 200; ++i) {
                    if (file.Read(buffer.data(), static_cast<int64_t>(t) * kChunk, kChunk) != kChunk ||
                        buffer != std::string(kChunk, static_cast<char>('a' + t))) {
                        ++failures;
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        EXPECT_EQ(0, failures.load());
    }
    std::filesystem::remove(path);
}
