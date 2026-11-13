// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Models/Identifiers.hpp"

#include <optional>
namespace AVEVA::RocksDB::Plugin::Azure::Models {
/// <summary>
/// A storage account reached through a chained credential: a service principal, falling back to a managed identity
/// (the user-assigned one named by `managedIdentityId`, else the system-assigned one).
/// </summary>
class ChainedCredentialInfo {
    DbName m_dbName;
    AccountUrl m_storageAccountUrl;
    ClientId m_servicePrincipalId;
    ClientSecret m_servicePrincipalSecret;
    TenantId m_tenantId;
    std::optional<ManagedIdentityId> m_managedIdentityId;

  public:
    ChainedCredentialInfo(DbName dbName, AccountUrl storageAccountUrl, ClientId servicePrincipalId,
                          ClientSecret servicePrincipalSecret, TenantId tenantId,
                          std::optional<ManagedIdentityId> managedIdentityId = {});

    [[nodiscard]] DbName GetDbName() const;
    [[nodiscard]] AccountUrl GetStorageAccountUrl() const;
    [[nodiscard]] ClientId GetServicePrincipalId() const;
    [[nodiscard]] ClientSecret GetServicePrincipalSecret() const;
    [[nodiscard]] TenantId GetTenantId() const;
    [[nodiscard]] std::optional<ManagedIdentityId> GetManagedIdentityId() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Models