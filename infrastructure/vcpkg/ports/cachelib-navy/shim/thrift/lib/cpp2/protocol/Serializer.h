/*
 * Thrift-free replacement for <thrift/lib/cpp2/protocol/Serializer.h>.
 * Part of the AVEVA cachelib-navy vcpkg overlay port; see
 * cachelib/thrift_shim/FieldRef.h. Every serializer throws: persistence and
 * recovery of Navy metadata are not supported by this port.
 */
#pragma once

#include <cstddef>
#include <string>

#include "cachelib/thrift_shim/FieldRef.h"

namespace apache::thrift {

namespace detail::shim {
struct ThrowingSerializer {
  template <typename T, typename Out, typename... Rest>
  static void serialize(const T& /*obj*/, Out* /*out*/, Rest&&... /*rest*/) {
    throw ::facebook::cachelib::thrift_shim::NotSupportedError{};
  }

  template <typename T, typename In>
  static std::size_t deserialize(const In& /*in*/, T& /*obj*/) {
    throw ::facebook::cachelib::thrift_shim::NotSupportedError{};
  }
};
} // namespace detail::shim

struct BinarySerializer : detail::shim::ThrowingSerializer {};
struct CompactSerializer : detail::shim::ThrowingSerializer {};

struct SimpleJSONSerializer {
  template <typename Str, typename T>
  static Str serialize(const T& /*obj*/) {
    return Str{"<thrift serialization not available>"};
  }
};

} // namespace apache::thrift
