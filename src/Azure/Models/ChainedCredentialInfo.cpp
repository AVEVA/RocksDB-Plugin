// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Models {
ChainedCredentialInfo::ChainedCredentialInfo(std::string dbName, std::string storageAccountUrl,
                                             std::string servicePrincipalId, std::string servicePrincipalSecret,
                                             std::string tenantId, std::optional<std::string> managedIdentityId)
    : m_dbName(std::move(dbName)), m_storageAccountUrl(std::move(storageAccountUrl)),
      m_servicePrincipalId(std::move(servicePrincipalId)), m_servicePrincipalSecret(std::move(servicePrincipalSecret)),
      m_tenantId(std::move(tenantId)), m_managedIdentityId(std::move(managedIdentityId)) {}

std::string ChainedCredentialInfo::GetDbName() const { return m_dbName; }

std::string ChainedCredentialInfo::GetStorageAccountUrl() const { return m_storageAccountUrl; }

std::string ChainedCredentialInfo::GetServicePrincipalId() const { return m_servicePrincipalId; }

std::string ChainedCredentialInfo::GetServicePrincipalSecret() const { return m_servicePrincipalSecret; }

std::string ChainedCredentialInfo::GetTenantId() const { return m_tenantId; }

std::optional<std::string> ChainedCredentialInfo::GetManagedIdentityId() const { return m_managedIdentityId; }
} // namespace AVEVA::RocksDB::Plugin::Azure::Models
