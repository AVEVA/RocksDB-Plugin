// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "CrcUtil.hpp"

#include <boost/crc.hpp>

// SSE4.2 CRC32C intrinsics (x64 only; boost::crc_32_type used as fallback).
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_AMD64))
#  include <intrin.h>
#elif defined(__SSE4_2__)
#  include <nmmintrin.h>
#endif

#include <cstdint>
#include <cstring>

namespace AVEVA::RocksDB::Plugin::Core
{
#if defined(_M_X64) || defined(_M_AMD64) || defined(__x86_64__)
    namespace
    {
        /// <summary>Advances a running CRC32C register over <paramref name="size"/> bytes without the final complement.</summary>
        uint32_t CrcUpdate(uint32_t crc, const char* data, size_t size) noexcept
        {
            const auto* p = reinterpret_cast<const uint8_t*>(data);
            while (size >= 8)
            {
                uint64_t v;
                std::memcpy(&v, p, 8);
                crc = static_cast<uint32_t>(_mm_crc32_u64(crc, v));
                p += 8; size -= 8;
            }
            if (size >= 4)
            {
                uint32_t v;
                std::memcpy(&v, p, 4);
                crc = _mm_crc32_u32(crc, v);
                p += 4; size -= 4;
            }
            while (size--)
                crc = _mm_crc32_u8(crc, *p++);
            return crc;
        }
    }
#endif

    uint32_t CrcUtil::Compute(const char* data, size_t size) noexcept
    {
#if defined(_M_X64) || defined(_M_AMD64) || defined(__x86_64__)
        return ~CrcUpdate(~0u, data, size);
#else
        boost::crc_32_type crc;
        crc.process_bytes(data, size);
        return static_cast<uint32_t>(crc.checksum());
#endif
    }

    uint32_t CrcUtil::Compute(const char* d1, size_t s1, const char* d2, size_t s2) noexcept
    {
#if defined(_M_X64) || defined(_M_AMD64) || defined(__x86_64__)
        return ~CrcUpdate(CrcUpdate(~0u, d1, s1), d2, s2);
#else
        boost::crc_32_type crc;
        crc.process_bytes(d1, s1);
        crc.process_bytes(d2, s2);
        return static_cast<uint32_t>(crc.checksum());
#endif
    }
}
