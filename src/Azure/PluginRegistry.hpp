// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include <rocksdb/file_system.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

/// Private to the plugin library: the per-name registration state behind Plugin::Register, split out of Plugin.cpp
/// so the public overloads only have to translate their arguments.
namespace AVEVA::RocksDB::Plugin::Azure::Registry {
using FileSystemFactory = std::function<std::unique_ptr<rocksdb::FileSystem>()>;

/// <summary>
/// Records `create` as the factory for `pluginName`, adding the RocksDB factory that reads it on first use. RocksDB
/// keeps the first factory added under a name, so later registrations only replace the recorded settings.
/// </summary>
/// `owner` identifies the registration that set it, so a shared name can later be cleared only by that owner.
void SetFactory(const std::string& pluginName, FileSystemFactory create, std::string owner = {});

/// <summary>
/// Drops the factory settings (and with them the io_context reference or ownership) for `pluginName`. The RocksDB
/// factory itself stays in the ObjectLibrary, which cannot remove entries, and reports "not registered" until a
/// later SetFactory call fills the state again. When `owner` is given, the state is cleared only if it was last set
/// by that owner.
/// </summary>
void ClearRegistration(const std::string& pluginName, const std::string* owner = nullptr);

/// <summary>
/// Builds the RocksDB registry name for a primary/backup pair. Each part is length-prefixed and the whole is
/// hex-encoded, so distinct (account, database) pairs can never collide ("ab"+"c" vs "a"+"bc") and the result
/// contains only characters that are safe in a RocksDB object-registry identifier.
/// </summary>
template <class StorageInfo>
std::string PluginNameFor(const StorageInfo& primary, const std::optional<StorageInfo>& backup) {
    std::string canonical;
    const auto append = [&canonical](const std::string& part) {
        canonical += std::to_string(part.size());
        canonical += ':';
        canonical += part;
    };
    append(primary.GetStorageAccountUrl());
    append(primary.GetDbName());
    if (backup) {
        append(backup->GetStorageAccountUrl());
        append(backup->GetDbName());
    }

    static constexpr std::string_view digits = "0123456789abcdef";
    std::string name(Plugin::Name);
    name += '-';
    for (const char ch : canonical) {
        const auto c = static_cast<unsigned char>(ch);
        name += digits[c >> 4U];
        name += digits[c & 0x0FU];
    }
    return name;
}

/// <summary>
/// The name earlier versions registered: Name + primary database name + backup database name.
/// </summary>
template <class StorageInfo>
std::string LegacyPluginNameFor(const StorageInfo& primary, const std::optional<StorageInfo>& backup) {
    auto name = std::string(Plugin::Name) + primary.GetDbName();
    if (backup) {
        name += backup->GetDbName();
    }
    return name;
}

/// <summary>
/// An io_context with one worker thread, for callers of the deprecated Register overloads that have no io_context
/// of their own. Shared by every filesystem created that way and kept alive by them.
/// </summary>
class OwnedIoContext {
    boost::asio::io_context m_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_workGuard{
        boost::asio::make_work_guard(m_context)};
    std::thread m_thread;

  public:
    OwnedIoContext();
    ~OwnedIoContext();
    OwnedIoContext(const OwnedIoContext&) = delete;
    OwnedIoContext& operator=(const OwnedIoContext&) = delete;

    [[nodiscard]] boost::asio::io_context& Context() { return m_context; }

    /// <summary>The process-wide instance; recreated when every previous user has released it.</summary>
    static std::shared_ptr<OwnedIoContext> Acquire();
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Registry
