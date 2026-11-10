/*
 * Thrift-free replacement for the subset of the fbthrift generated C++ API
 * used by CacheLib Navy. Part of the AVEVA cachelib-navy vcpkg overlay port.
 *
 * Navy only needs thrift for (de)serializing persistent metadata. This port
 * never persists or recovers the cache, so the generated structs are replaced
 * with plain C++ structs that expose the same field accessor API
 * (`obj.field()` returning a field_ref-like proxy) and the serializers throw.
 */
#pragma once

#include <compare>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace facebook::cachelib::thrift_shim {

// Minimal equivalent of apache::thrift::field_ref<T&>.
template <typename T>
class FieldRef {
 public:
  using value_type = std::remove_const_t<T>;

  explicit FieldRef(T& value) noexcept : value_{&value} {}

  template <typename U>
    requires(!std::is_const_v<T> && std::is_assignable_v<T&, U &&>)
  FieldRef& operator=(U&& other) {
    *value_ = std::forward<U>(other);
    return *this;
  }

  T& value() const noexcept { return *value_; }
  T& operator*() const noexcept { return *value_; }
  T* operator->() const noexcept { return value_; }
  T& ensure() const noexcept { return *value_; }

  template <typename I>
  decltype(auto) operator[](I&& index) const {
    return (*value_)[std::forward<I>(index)];
  }

  template <typename U>
  friend bool operator==(const FieldRef& lhs, const U& rhs) {
    return *lhs.value_ == rhs;
  }
  template <typename U>
  friend auto operator<=>(const FieldRef& lhs, const U& rhs) {
    return *lhs.value_ <=> rhs;
  }

  template <typename... Args>
    requires(!std::is_const_v<T>)
  T& emplace(Args&&... args) {
    *value_ = T(std::forward<Args>(args)...);
    return *value_;
  }

  bool has_value() const noexcept { return true; }
  bool is_set() const noexcept { return true; }

 private:
  T* value_;
};

class NotSupportedError : public std::logic_error {
 public:
  NotSupportedError()
      : std::logic_error(
            "CacheLib thrift serialization is not available in the "
            "cachelib-navy port (persistence is not supported)") {}
};

} // namespace facebook::cachelib::thrift_shim

// Declares a thrift-like field `name` of type `type` with accessor overloads.
// `type` must be a single token sequence without top-level commas.
#define CACHELIB_SHIM_FIELD(type, name, ...)                              \
  type __fbthrift_field_##name{__VA_ARGS__};                              \
  ::facebook::cachelib::thrift_shim::FieldRef<type> name() & {            \
    return ::facebook::cachelib::thrift_shim::FieldRef<type>{             \
        __fbthrift_field_##name};                                         \
  }                                                                       \
  ::facebook::cachelib::thrift_shim::FieldRef<const type> name() const& { \
    return ::facebook::cachelib::thrift_shim::FieldRef<const type>{       \
        __fbthrift_field_##name};                                         \
  }                                                                       \
  ::facebook::cachelib::thrift_shim::FieldRef<type> name##_ref() & {      \
    return name();                                                        \
  }                                                                       \
  ::facebook::cachelib::thrift_shim::FieldRef<const type> name##_ref()    \
      const& {                                                            \
    return name();                                                        \
  }
