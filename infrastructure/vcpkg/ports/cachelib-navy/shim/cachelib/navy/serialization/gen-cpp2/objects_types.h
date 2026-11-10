/*
 * Thrift-free replacement for the code generated from
 * cachelib/navy/serialization/objects.thrift (CacheLib v2026.02.23.00).
 * Part of the AVEVA cachelib-navy vcpkg overlay port; see FieldRef.h.
 */
#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <vector>

#include "cachelib/thrift_shim/FieldRef.h"

namespace facebook::cachelib::navy::serialization {

using Int64Map = std::map<int64_t, int64_t>;

struct IndexEntry {
  CACHELIB_SHIM_FIELD(int32_t, key, 0)
  CACHELIB_SHIM_FIELD(int32_t, address, 0)
  CACHELIB_SHIM_FIELD(int16_t, sizeHint, 0)
  CACHELIB_SHIM_FIELD(int8_t, totalHits, 0)
  CACHELIB_SHIM_FIELD(int8_t, currentHits, 0)
};

struct IndexBucket {
  CACHELIB_SHIM_FIELD(int32_t, bucketId, 0)
  CACHELIB_SHIM_FIELD(std::vector<IndexEntry>, entries)
};

struct Region {
  CACHELIB_SHIM_FIELD(int32_t, regionId, 0)
  CACHELIB_SHIM_FIELD(int32_t, lastEntryEndOffset, 0)
  CACHELIB_SHIM_FIELD(int32_t, classId, 0)
  CACHELIB_SHIM_FIELD(int32_t, numItems, 0)
  CACHELIB_SHIM_FIELD(bool, pinned, false)
  CACHELIB_SHIM_FIELD(int32_t, priority, 0)
};

struct RegionData {
  CACHELIB_SHIM_FIELD(std::vector<Region>, regions)
  CACHELIB_SHIM_FIELD(int32_t, regionSize, 0)
};

struct FifoPolicyNodeData {
  CACHELIB_SHIM_FIELD(int32_t, idx, 0)
  CACHELIB_SHIM_FIELD(int64_t, trackTime, 0)
};

struct FifoPolicyData {
  CACHELIB_SHIM_FIELD(std::vector<FifoPolicyNodeData>, queue)
};

struct AccessStats {
  CACHELIB_SHIM_FIELD(int8_t, totalHits, 0)
  CACHELIB_SHIM_FIELD(int8_t, currHits, 0)
  CACHELIB_SHIM_FIELD(int8_t, numReinsertions, 0)
};

using AccessStatsMap = std::map<int64_t, AccessStats>;

struct AccessStatsPair {
  CACHELIB_SHIM_FIELD(int64_t, key, 0)
  CACHELIB_SHIM_FIELD(AccessStats, stats)
};

struct AccessTracker {
  CACHELIB_SHIM_FIELD(AccessStatsMap, deprecated_data)
  CACHELIB_SHIM_FIELD(std::vector<AccessStatsPair>, data)
};

struct BlockCacheConfig {
  CACHELIB_SHIM_FIELD(int64_t, version, 0)
  CACHELIB_SHIM_FIELD(int64_t, cacheBaseOffset, 0)
  CACHELIB_SHIM_FIELD(int64_t, cacheSize, 0)
  CACHELIB_SHIM_FIELD(int32_t, allocAlignSize, 0)
  CACHELIB_SHIM_FIELD(std::set<int32_t>, deprecated_sizeClasses)
  CACHELIB_SHIM_FIELD(bool, checksum, false)
  CACHELIB_SHIM_FIELD(Int64Map, deprecated_sizeDist)
  CACHELIB_SHIM_FIELD(int64_t, holeCount, 0)
  CACHELIB_SHIM_FIELD(int64_t, holeSizeTotal, 0)
  CACHELIB_SHIM_FIELD(bool, reinsertionPolicyEnabled, false)
  CACHELIB_SHIM_FIELD(int64_t, usedSizeBytes, 0)
};

struct ValidBucketCheckerState {
  CACHELIB_SHIM_FIELD(int32_t, numBuckets, 0)
  CACHELIB_SHIM_FIELD(int32_t, numBucketsPerBit, 0)
  CACHELIB_SHIM_FIELD(int32_t, numDisabledBuckets, 0)
  CACHELIB_SHIM_FIELD(std::vector<int8_t>, bytes)
};

struct BigHashPersistentData {
  CACHELIB_SHIM_FIELD(int32_t, version, 0)
  CACHELIB_SHIM_FIELD(int64_t, generationTime, 0)
  CACHELIB_SHIM_FIELD(int64_t, itemCount, 0)
  CACHELIB_SHIM_FIELD(int64_t, bucketSize, 0)
  CACHELIB_SHIM_FIELD(int64_t, cacheBaseOffset, 0)
  CACHELIB_SHIM_FIELD(int64_t, numBuckets, 0)
  CACHELIB_SHIM_FIELD(Int64Map, deprecated_sizeDist)
  CACHELIB_SHIM_FIELD(int64_t, usedSizeBytes, 0)
  CACHELIB_SHIM_FIELD(ValidBucketCheckerState, validBucketCheckerState)
};

struct FixedSizeIndexConfig {
  CACHELIB_SHIM_FIELD(int32_t, version, 0)
  CACHELIB_SHIM_FIELD(int32_t, numChunks, 0)
  CACHELIB_SHIM_FIELD(int8_t, numBucketsPerChunkPower, 0)
  CACHELIB_SHIM_FIELD(int64_t, numBucketsPerShard, 0)
};

} // namespace facebook::cachelib::navy::serialization
