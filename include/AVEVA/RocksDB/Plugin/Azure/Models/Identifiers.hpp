// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <string>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Models {
/// <summary>
/// A string with a meaning, so arguments of different meanings cannot be swapped. It never converts implicitly, and
/// its text can only be read from a named object (Value() is deleted on temporaries), so a view of it cannot silently
/// outlive the string.
/// </summary>
template <class Tag> class Identifier {
    std::string m_value;

  public:
    explicit Identifier(std::string value) : m_value(std::move(value)) {}

    [[nodiscard]] const std::string& Value() const& noexcept { return m_value; }
    const std::string& Value() const&& = delete;

    [[nodiscard]] friend bool operator==(const Identifier&, const Identifier&) = default;
};

/// <summary>The database name, which is also the name of the blob container that holds it.</summary>
using DbName = Identifier<struct DbNameTag>;
/// <summary>A storage account's blob endpoint, e.g. "https://account.blob.core.windows.net".</summary>
using AccountUrl = Identifier<struct AccountUrlTag>;
/// <summary>The application (client) id of a service principal.</summary>
using ClientId = Identifier<struct ClientIdTag>;
/// <summary>The secret of a service principal.</summary>
using ClientSecret = Identifier<struct ClientSecretTag>;
/// <summary>The Microsoft Entra tenant of a service principal.</summary>
using TenantId = Identifier<struct TenantIdTag>;
/// <summary>The client id of a user-assigned managed identity.</summary>
using ManagedIdentityId = Identifier<struct ManagedIdentityIdTag>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Models