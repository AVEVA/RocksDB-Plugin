// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstdint>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>
    /// Computes a 32-bit CRC32C (Castagnoli) checksum.
    /// On x64 the hardware path uses SSE4.2 instructions (_mm_crc32_u64/_u32/_u8) which are
    /// available on all x86-64 processors since Intel Nehalem / AMD Bulldozer (~2010).
    /// A software fallback using boost::crc_32_type is compiled for other architectures.
    /// </summary>
    struct CrcUtil
    {
        /// <summary>Computes CRC32C over a single span.</summary>
        static uint32_t Compute(const char* data, size_t size) noexcept;

        /// <summary>Computes CRC32C over two discontiguous spans as if they were concatenated.</summary>
        static uint32_t Compute(const char* d1, size_t s1, const char* d2, size_t s2) noexcept;
    };
}
