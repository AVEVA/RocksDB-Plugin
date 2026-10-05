// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <string>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Returns the value of the environment variable `name`, or an empty string when it is not set.
/// </summary>
std::string GetEnvironmentValue(const char* name);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
