// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <optional>
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Models {
class ChainedCredentialInfo {
    std::string m_dbName;
    std::string m_storageAccountUrl;
    std::string m_servicePrincipalId;
    std::string m_servicePrincipalSecret;
    std::string m_tenantId;
    std::optional<std::string> m_managedIdentityId;

  public:
    ChainedCredentialInfo(std::string dbName, std::string storageAccountUrl, std::string servicePrincipalId,
                          std::string servicePrincipalSecret, std::string tenantId,
                          std::optional<std::string> managedIdentityId = {});

    [[nodiscard]] std::string GetDbName() const;
    [[nodiscard]] std::string GetStorageAccountUrl() const;
    [[nodiscard]] std::string GetServicePrincipalId() const;
    [[nodiscard]] std::string GetServicePrincipalSecret() const;
    [[nodiscard]] std::string GetTenantId() const;
    [[nodiscard]] std::optional<std::string> GetManagedIdentityId() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Models
