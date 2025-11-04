#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Models
{
    ServicePrincipalStorageInfo::ServicePrincipalStorageInfo(
        const std::string& dbName,
        const std::string& storageAccountUrl,
        const std::string& servicePrincipalId,
        const std::string& servicePrincipalSecret,
        const std::string& tenantId)
        : m_dbName(dbName),
        m_storageAccountUrl(storageAccountUrl),
        m_servicePrincipalId(servicePrincipalId),
        m_servicePrincipalSecret(servicePrincipalSecret),
        m_tenantId(tenantId)
    {
    }

    const std::string& ServicePrincipalStorageInfo::GetDbName() const noexcept
    {
        return m_dbName;
    }

    const std::string& ServicePrincipalStorageInfo::GetStorageAccountUrl() const noexcept
    {
        return m_storageAccountUrl;
    }

    const std::string& ServicePrincipalStorageInfo::GetServicePrincipalId() const noexcept
    {
        return m_servicePrincipalId;
    }

    const std::string& ServicePrincipalStorageInfo::GetServicePrincipalSecret() const noexcept
    {
        return m_servicePrincipalSecret;
    }

    const std::string& ServicePrincipalStorageInfo::GetTenantId() const noexcept
    {
        return m_tenantId;
    }
}
