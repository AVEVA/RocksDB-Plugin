// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Models/Identifiers.hpp"

#include <string>
#include <string_view>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>A storage account shared key; only the tests sign requests with one.</summary>
using StorageAccountKey = Models::Identifier<struct StorageAccountKeyTag>;

struct StorageAccount {
    [[nodiscard]] static std::string UniquePrefix(const Models::AccountUrl& storageAccountUrl,
                                                  const Models::DbName& dbName);
    [[nodiscard]] static std::pair<std::string_view, std::string_view> StripPrefix(std::string_view filePath);
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl