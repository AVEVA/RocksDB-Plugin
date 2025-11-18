// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <string>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Core
{
    struct StringEqual
    {
        using is_transparent = void;
        bool operator()(const std::string& lhs, const std::string& rhs) const;
        bool operator()(std::string_view lhs, const std::string& rhs) const;
        bool operator()(const std::string& lhs, std::string_view rhs) const;
        bool operator()(std::string_view lhs, std::string_view rhs) const;
    };

    struct StringHash
    {
        using is_transparent = void;
        using transparent_key_equal = StringEqual;
        std::size_t operator()(const std::string& s) const;
        std::size_t operator()(std::string_view s) const;
    };
}
