// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"

#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Models {
ChainedCredentialInfo::ChainedCredentialInfo(DbName dbName, AccountUrl storageAccountUrl, ClientId servicePrincipalId,
                                             ClientSecret servicePrincipalSecret, TenantId tenantId,
                                             std::optional<ManagedIdentityId> managedIdentityId)
    : m_dbName(std::move(dbName)), m_storageAccountUrl(std::move(storageAccountUrl)),
      m_servicePrincipalId(std::move(servicePrincipalId)), m_servicePrincipalSecret(std::move(servicePrincipalSecret)),
      m_tenantId(std::move(tenantId)), m_managedIdentityId(std::move(managedIdentityId)) {}

DbName ChainedCredentialInfo::GetDbName() const { return m_dbName; }

AccountUrl ChainedCredentialInfo::GetStorageAccountUrl() const { return m_storageAccountUrl; }

ClientId ChainedCredentialInfo::GetServicePrincipalId() const { return m_servicePrincipalId; }

ClientSecret ChainedCredentialInfo::GetServicePrincipalSecret() const { return m_servicePrincipalSecret; }

TenantId ChainedCredentialInfo::GetTenantId() const { return m_tenantId; }

std::optional<ManagedIdentityId> ChainedCredentialInfo::GetManagedIdentityId() const { return m_managedIdentityId; }
} // namespace AVEVA::RocksDB::Plugin::Azure::Models