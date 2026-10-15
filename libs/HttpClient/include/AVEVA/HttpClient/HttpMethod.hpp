// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <string_view>

namespace AVEVA {
/// <summary>
/// The HTTP request methods a request can carry.
/// </summary>
enum class HttpMethod { Get, Post, Put, Patch, Delete, Head, Options, Trace, Connect };

/// <summary>
/// Returns the uppercase wire name of a method.
/// </summary>
/// <param name="method">The method to name.</param>
/// <returns>The wire name (for example <c>"GET"</c>), or an empty view if <paramref name="method"/> is
/// not one of the enumerators.</returns>
std::string_view ToString(HttpMethod method);
} // namespace AVEVA
