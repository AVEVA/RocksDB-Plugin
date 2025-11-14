// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <string>
#include <optional>
namespace AVEVA::RocksDB::Plugin::Azure::Models
{
    class ChainedCredentialInfo
    {
        std::string m_dbName;
        std::string m_storageAccountUrl;
        std::string m_servicePrincipalId;
        std::string m_servicePrincipalSecret;
        std::string m_tenantId;
        std::optional<std::string> m_managedIdentityId;
    public:
        ChainedCredentialInfo(std::string dbName,
            std::string storageAccountUrl,
            std::string servicePrincipalId,
            std::string servicePrincipalSecret,
            std::string tenantId,
            std::optional<std::string> managedIdentityId = {});

        const std::string& GetDbName() const noexcept;
        const std::string& GetStorageAccountUrl() const noexcept;
        const std::string& GetServicePrincipalId() const noexcept;
        const std::string& GetServicePrincipalSecret() const noexcept;
        const std::string& GetTenantId() const noexcept;
        std::optional<std::string_view> GetManagedIdentityId() const noexcept;
    };
}
