// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstdint>
#include <string>
#include <span>
namespace AVEVA::RocksDB::Plugin::Core
{
    class BlobClient
    {
    public:
        virtual ~BlobClient() = default;

        /// <summary>
        /// Returns the size of the blob's representable data.
        /// </summary>
        /// <returns>The size of the blob.</returns>
        virtual int64_t GetSize() = 0;

        /// <summary>
        /// Sets the size of the blob to the specified value.
        /// </summary>
        /// <param name="size">The new size to set, in bytes.</param>
        virtual void SetSize(int64_t size) = 0;

        /// <summary>
        /// Returns the capacity of the blob.
        /// </summary>
        /// <returns>The capacity value.</returns>
        virtual int64_t GetCapacity() = 0;

        /// <summary>
        /// Sets the blob's capacity to the specified value.
        /// </summary>
        /// <param name="capacity">The new capacity value to set, in bytes.</param>
        virtual void SetCapacity(int64_t capacity) = 0;

        /// <summary>
        /// Downloads a portion of data to the specified file path.
        /// </summary>
        /// <param name="path">The destination file path where the data will be saved.</param>
        /// <param name="offset">The starting position (in bytes) from which to begin downloading.</param>
        /// <param name="length">The number of bytes to download from the offset.</param>
        virtual void DownloadTo(const std::string& path, int64_t offset, int64_t length) = 0;

        /// <summary>
        /// Downloads data into the provided buffer starting at the specified offset for the given length.
        /// </summary>
        /// <param name="buffer">A span of bytes where the downloaded data will be stored.</param>
        /// <param name="blobOffset">The starting position (in bytes) in the blob from which to begin downloading.</param>
        /// <param name="length">The number of bytes to download from the offset.</param>
        /// <returns>The number of bytes actually downloaded or -1 if some problem occurred.</returns>
        virtual int64_t DownloadTo(std::span<char> buffer, int64_t blobOffset, int64_t length) = 0;

        /// <summary>
        /// Uploads a sequence of pages to a blob at the specified offset.
        /// </summary>
        /// <param name="buffer">A span containing the page data to upload.</param>
        /// <param name="blobOffset">The offset within the blob where the data should be uploaded.</param>
        virtual void UploadPages(const std::span<char> buffer, int64_t blobOffset) = 0;
    };
}
