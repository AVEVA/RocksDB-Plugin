#include "AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"

#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::Impl::BufferChunkInfo;
TEST(BufferChunkInfoTests, ChunkSize_ReturnsCorrectTotal) {
    // Arrange
    BufferChunkInfo chunk{ 0, 512, 100, 12, 400 };

    // Act
    const auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size); // 12 + 100 + 400
}

TEST(BufferChunkInfoTests, ChunkSize_WithNoPadding_ReturnsDataLength) {
    // Arrange
    BufferChunkInfo chunk{ 0, 0, 512, 0, 0 };

    // Act
    const auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size);
}

TEST(BufferChunkInfoTests, ChunkSize_WithOnlyPrePadding) {
    // Arrange
    BufferChunkInfo chunk{ 0, 100, 412, 100, 0 };

    // Act
    const auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size); // 100 + 412 + 0
}

TEST(BufferChunkInfoTests, ChunkSize_WithOnlyPostPadding) {
    // Arrange
    BufferChunkInfo chunk{ 0, 0, 256, 0, 256 };

    // Act
    const auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size); // 0 + 256 + 256
}
