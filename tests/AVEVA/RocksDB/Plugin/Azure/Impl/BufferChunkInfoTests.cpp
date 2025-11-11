#include "AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"

#include <gtest/gtest.h>

using namespace AVEVA::RocksDB::Plugin::Azure::Impl;

class BufferChunkInfoTests : public ::testing::Test {};

TEST_F(BufferChunkInfoTests, ChunkSize_ReturnsCorrectTotal) {
    // Arrange
    BufferChunkInfo chunk(0, 512, 100, 12, 400);

    // Act
    auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size); // 12 + 100 + 400
}

TEST_F(BufferChunkInfoTests, ChunkSize_WithNoPadding_ReturnsDataLength) {
    // Arrange
    BufferChunkInfo chunk(0, 0, 512, 0, 0);

    // Act
    auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size);
}

TEST_F(BufferChunkInfoTests, ChunkSize_WithOnlyPrePadding) {
    // Arrange
    BufferChunkInfo chunk(0, 100, 412, 100, 0);

    // Act
    auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size); // 100 + 412 + 0
}

TEST_F(BufferChunkInfoTests, ChunkSize_WithOnlyPostPadding) {
    // Arrange
    BufferChunkInfo chunk(0, 0, 256, 0, 256);

 // Act
    auto size = chunk.ChunkSize();

    // Assert
    EXPECT_EQ(512, size); // 0 + 256 + 256
}
