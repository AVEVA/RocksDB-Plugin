#pragma once
#include <string>
#include <string_view>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    struct StorageAccount
    {
        [[nodiscard]] static std::string UniquePrefix(const std::string& storageAccountUrl, const std::string& dbName);
        [[nodiscard]] static std::pair<std::string_view, std::string_view> StripPrefix(std::string_view filePath);
    };
}
