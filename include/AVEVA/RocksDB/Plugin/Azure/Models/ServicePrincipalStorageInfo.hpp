// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Models/Identifiers.hpp"

namespace AVEVA::RocksDB::Plugin::Azure::Models {
/// <summary>A storage account reached with a service principal's client secret.</summary>
class ServicePrincipalStorageInfo {
    DbName m_dbName;
    AccountUrl m_storageAccountUrl;
    ClientId m_servicePrincipalId;
    ClientSecret m_servicePrincipalSecret;
    TenantId m_tenantId;

  public:
    ServicePrincipalStorageInfo(DbName dbName, AccountUrl storageAccountUrl, ClientId servicePrincipalId,
                                ClientSecret servicePrincipalSecret, TenantId tenantId);

    [[nodiscard]] DbName GetDbName() const;
    [[nodiscard]] AccountUrl GetStorageAccountUrl() const;
    [[nodiscard]] ClientId GetServicePrincipalId() const;
    [[nodiscard]] ClientSecret GetServicePrincipalSecret() const;
    [[nodiscard]] TenantId GetTenantId() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Models