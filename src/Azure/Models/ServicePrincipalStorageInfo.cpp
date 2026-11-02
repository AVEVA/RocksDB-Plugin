// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Models {
ServicePrincipalStorageInfo::ServicePrincipalStorageInfo(const std::string& dbName,
                                                         const std::string& storageAccountUrl,
                                                         const std::string& servicePrincipalId,
                                                         const std::string& servicePrincipalSecret,
                                                         const std::string& tenantId)
    : m_dbName(dbName), m_storageAccountUrl(storageAccountUrl), m_servicePrincipalId(servicePrincipalId),
      m_servicePrincipalSecret(servicePrincipalSecret), m_tenantId(tenantId) {}

std::string ServicePrincipalStorageInfo::GetDbName() const { return m_dbName; }

std::string ServicePrincipalStorageInfo::GetStorageAccountUrl() const { return m_storageAccountUrl; }

std::string ServicePrincipalStorageInfo::GetServicePrincipalId() const { return m_servicePrincipalId; }

std::string ServicePrincipalStorageInfo::GetServicePrincipalSecret() const { return m_servicePrincipalSecret; }

std::string ServicePrincipalStorageInfo::GetTenantId() const { return m_tenantId; }
} // namespace AVEVA::RocksDB::Plugin::Azure::Models
