// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    static const constexpr char g_prefixSeparator = '+';
    std::string StorageAccount::UniquePrefix(const std::string& storageAccountUrl, const std::string& dbName)
    {
        // strip out invalid url portions
        auto secondSlash = storageAccountUrl.find_first_of('/') + 2;
        auto period = storageAccountUrl.find_first_of('.');

        // NOTE: The separator '+' is an illegal character for container names.
        // THIS IS INTENTIONAL. We need a unique prefix for the plugin to be able to distinguish
        // between the same db name pointed at different storage accounts (a legitimate use-case).
        // The storage account portion (including the path separator) will be stripped by the time
        // it reaches the plugin.
        return storageAccountUrl.substr(secondSlash, period - secondSlash) + g_prefixSeparator + dbName;
    }

    std::pair<std::string_view, std::string_view> StorageAccount::StripPrefix(const std::string_view filePath)
    {
        auto separatorIndex = filePath.find_first_of('/');

#ifdef _WIN32
        // NOTE: Paths on windows seem to be given as either forward slashes OR traditional
        // Windows backslashes. We should make sure they match.
        const auto otherSeparatorIndex = filePath.find_first_of('\\');
        if (separatorIndex == std::string::npos && otherSeparatorIndex != std::string::npos)
        {
            separatorIndex = otherSeparatorIndex;
        }
#endif

        if (separatorIndex == std::string::npos)
        {
            // There is no storage account prefix.
            return { filePath, "" };
        }
        else
        {
            return { filePath.substr(0, separatorIndex), filePath.substr(separatorIndex + 1) };
        }
    }
}
