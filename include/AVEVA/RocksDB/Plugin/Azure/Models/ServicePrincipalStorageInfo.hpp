#pragma once
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Models
{
    class ServicePrincipalStorageInfo
    {
        std::string m_dbName;
        std::string m_storageAccountUrl;
        std::string m_servicePrincipalId;
        std::string m_servicePrincipalSecret;
        std::string m_tenantId;
    public:
        ServicePrincipalStorageInfo(const std::string& dbName,
            const std::string& storageAccountUrl,
            const std::string& servicePrincipalId,
            const std::string& servicePrincipalSecret,
            const std::string& tenantId);

        const std::string& GetDbName() const noexcept;
        const std::string& GetStorageAccountUrl() const noexcept;
        const std::string& GetServicePrincipalId() const noexcept;
        const std::string& GetServicePrincipalSecret() const noexcept;
        const std::string& GetTenantId() const noexcept;
    };
}
