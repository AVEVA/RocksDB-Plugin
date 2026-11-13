// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Core {
// std::string and const char* convert implicitly to string_view, so one overload covers every key type.
struct StringEqual {
    using is_transparent = void;
    [[nodiscard]] bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs == rhs; }
};

struct StringHash {
    using is_transparent = void;
    using transparent_key_equal = StringEqual;
    [[nodiscard]] std::size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
};
} // namespace AVEVA::RocksDB::Plugin::Core
