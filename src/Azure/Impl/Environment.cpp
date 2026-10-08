// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Environment.hpp"

#include <cstdlib>
#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
std::string GetEnvironmentValue(const char* name) {
#ifdef _WIN32
    char* buffer = nullptr;
    std::size_t size = 0;
    if (_dupenv_s(&buffer, &size, name) != 0 || buffer == nullptr) {
        return {};
    }
    const std::unique_ptr<char, decltype(&std::free)> owner{buffer, &std::free};
    return std::string{buffer};
#else
    const char* value = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
    return value == nullptr ? std::string{} : std::string{value};
#endif
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
