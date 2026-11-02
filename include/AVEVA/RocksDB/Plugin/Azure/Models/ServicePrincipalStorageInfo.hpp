// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Models {
class ServicePrincipalStorageInfo {
    std::string m_dbName;
    std::string m_storageAccountUrl;
    std::string m_servicePrincipalId;
    std::string m_servicePrincipalSecret;
    std::string m_tenantId;

  public:
    ServicePrincipalStorageInfo(const std::string& dbName, const std::string& storageAccountUrl,
                                const std::string& servicePrincipalId, const std::string& servicePrincipalSecret,
                                const std::string& tenantId);

    [[nodiscard]] std::string GetDbName() const;
    [[nodiscard]] std::string GetStorageAccountUrl() const;
    [[nodiscard]] std::string GetServicePrincipalId() const;
    [[nodiscard]] std::string GetServicePrincipalSecret() const;
    [[nodiscard]] std::string GetTenantId() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Models
