/*
 * Thrift-free replacement for cachelib/shm/gen-cpp2/shm_types.h.
 * Part of the AVEVA cachelib-navy vcpkg overlay port; see FieldRef.h.
 */
#pragma once

#include "cachelib/thrift_shim/FieldRef.h"

#include <cstdint>
#include <map>
#include <string>

namespace facebook::cachelib::serialization {

using StringMap = std::map<std::string, std::string>;

struct ShmManagerObject {
  CACHELIB_SHIM_FIELD(int8_t, shmVal, 0)
  CACHELIB_SHIM_FIELD(StringMap, nameToKeyMap)
};

} // namespace facebook::cachelib::serialization
