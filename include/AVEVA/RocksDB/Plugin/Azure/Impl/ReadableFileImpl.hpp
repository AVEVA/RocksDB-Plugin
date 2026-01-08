// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"


#include <cstdint>
#include <string>
#include <string_view>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class ReadableFileImpl
    {
        std::string m_name;
        std::shared_ptr<Core::BlobClient> m_blobClient;
        std::shared_ptr<Core::FileCache> m_fileCache;
        int64_t m_offset;
        int64_t m_size;

    public:
        ReadableFileImpl(std::string_view name,
            std::shared_ptr<Core::BlobClient> blobClient,
            std::shared_ptr<Core::FileCache> fileCache);

        // NOTE: Increments m_offset
        [[nodiscard]] int64_t SequentialRead(int64_t bytesToRead, char* buffer);

        // NOTE: Random so doesn't affect the sequential reads
        [[nodiscard]] int64_t RandomRead(int64_t offset, int64_t bytesToRead, char* buffer) const;

        int64_t GetOffset() const;
        void Skip(int64_t n);
        int64_t GetSize() const;
    };
}
