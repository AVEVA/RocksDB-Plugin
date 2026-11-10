/*
 * Thrift-free replacement for the code generated from
 * cachelib/common/BloomFilter.thrift (CacheLib v2026.02.23.00).
 * Part of the AVEVA cachelib-navy vcpkg overlay port; see FieldRef.h.
 */
#pragma once

#include <cstdint>
#include <vector>

#include "cachelib/thrift_shim/FieldRef.h"

namespace facebook::cachelib::serialization {

struct BloomFilterPersistentData {
  CACHELIB_SHIM_FIELD(int32_t, numFilters, 0)
  CACHELIB_SHIM_FIELD(int64_t, hashTableBitSize, 0)
  CACHELIB_SHIM_FIELD(int64_t, filterByteSize, 0)
  CACHELIB_SHIM_FIELD(int32_t, fragmentSize, 0)
  CACHELIB_SHIM_FIELD(std::vector<int64_t>, seeds)
};

} // namespace facebook::cachelib::serialization
