// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Models {
ServicePrincipalStorageInfo::ServicePrincipalStorageInfo(DbName dbName, AccountUrl storageAccountUrl,
                                                         ClientId servicePrincipalId,
                                                         ClientSecret servicePrincipalSecret, TenantId tenantId)
    : m_dbName(std::move(dbName)), m_storageAccountUrl(std::move(storageAccountUrl)),
      m_servicePrincipalId(std::move(servicePrincipalId)), m_servicePrincipalSecret(std::move(servicePrincipalSecret)),
      m_tenantId(std::move(tenantId)) {}

DbName ServicePrincipalStorageInfo::GetDbName() const { return m_dbName; }

AccountUrl ServicePrincipalStorageInfo::GetStorageAccountUrl() const { return m_storageAccountUrl; }

ClientId ServicePrincipalStorageInfo::GetServicePrincipalId() const { return m_servicePrincipalId; }

ClientSecret ServicePrincipalStorageInfo::GetServicePrincipalSecret() const { return m_servicePrincipalSecret; }

TenantId ServicePrincipalStorageInfo::GetTenantId() const { return m_tenantId; }
} // namespace AVEVA::RocksDB::Plugin::Azure::Models