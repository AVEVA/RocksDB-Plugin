// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstdint>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    struct BufferChunkInfo
    {
        /// <summary>
        /// |----------------------------------------------------------------------------------------------| Buffer
        /// |          |&&&^------------------------***************|&&&&&^------------------*****| ChunkInfoList
        ///            chunk1      chunk2
        /// *This diagram assumes a left to right bump allocator*
        /// 
        /// Either we assume that no chunks overlap OR it's okay that they overlap as long as we process/commit them in order.
        /// </summary>
        BufferChunkInfo(int64_t buffOffset, int64_t fileOffset, int64_t chunkLength, int64_t prepad, int64_t postpad)
            : bufferOffset(buffOffset),
            targetOffset(fileOffset),
            dataLength(chunkLength),
            prePadding(prepad),
            postPadding(postpad) {
        }

        /// <summary>
        /// The start of the pre-padding.
        /// 
        /// This is the offset into the local buffer.
        /// </summary>
        int64_t bufferOffset;

        /// <summary>
        /// Position at which the *real* chunk bytes start in the target page blob.
        /// This does not include the pre or post padding.
        /// 
        /// This is the target offset into the page blob.
        /// </summary>
        int64_t targetOffset;

        /// <summary>
        /// Length of the real data.
        /// 
        /// dataLength + prePadding + postPadding = chunkLength
        /// </summary>
        int64_t dataLength;

        /// <summary>
        /// Number of bytes needed to align to the nearest left page boundary.
        /// </summary>
        int64_t prePadding;

        /// <summary>
        /// Number of bytes needed to align to the nearest right page boundary.
        /// </summary>
        int64_t postPadding;

        /// <returns>The allocated buffer space of the whole chunk.</returns>
        int64_t ChunkSize() const noexcept { return prePadding + dataLength + postPadding; }
    };
}
