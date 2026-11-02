// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include <rocksdb/file_system.h>

#include <boost/algorithm/hex.hpp>

#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

/// Private to the plugin library: the per-name registration state behind Plugin::Register, split out of Plugin.cpp
/// so the public overloads only have to translate their arguments.
namespace AVEVA::RocksDB::Plugin::Azure::Registry {
using FileSystemFactory = std::function<std::unique_ptr<rocksdb::FileSystem>()>;

/// Identifies the registration that set a name, so a shared name can later be cleared only by that owner. The default
/// owner is anonymous.
class RegistrationOwner {
  public:
    RegistrationOwner() = default;
    explicit RegistrationOwner(std::string name) : m_name(std::move(name)) {}

    [[nodiscard]] const std::string& Name() const noexcept { return m_name; }
    friend auto operator<=>(const RegistrationOwner&, const RegistrationOwner&) = default;

  private:
    std::string m_name;
};

/// <summary>
/// Records `create` as the factory for `pluginName`, adding the RocksDB factory that reads it on first use. RocksDB
/// keeps the first factory added under a name, so later registrations only replace the recorded settings.
/// </summary>
/// `owner` identifies the registration that set it, so a shared name can later be cleared only by that owner.
void SetFactory(const std::string& pluginName, FileSystemFactory create, RegistrationOwner owner = {});

/// <summary>
/// Drops the factory settings (and with them the io_context reference or ownership) for `pluginName`. The RocksDB
/// factory itself stays in the ObjectLibrary, which cannot remove entries, and reports "not registered" until a
/// later SetFactory call fills the state again.
/// </summary>
void ClearRegistration(const std::string& pluginName);

/// <summary>
/// As ClearRegistration, but only if the state was last set by `owner`: a shared name (the legacy alias) may have been
/// re-registered by another owner since, and is then left alone.
/// </summary>
void ClearRegistrationOwnedBy(const std::string& pluginName, const RegistrationOwner& owner);

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

    std::string name(Plugin::Name);
    name += '-';
    name.reserve(name.size() + canonical.size() * 2);
    boost::algorithm::hex_lower(canonical, std::back_inserter(name));
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
} // namespace AVEVA::RocksDB::Plugin::Azure::Registry
