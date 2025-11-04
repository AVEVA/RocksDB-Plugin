#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Models
{
    ChainedCredentialInfo::ChainedCredentialInfo(std::string dbName,
        std::string storageAccountUrl,
        std::string servicePrincipalId,
        std::string servicePrincipalSecret,
        std::string tenantId,
        std::optional<std::string> managedIdentityId)
        : m_dbName(std::move(dbName)),
        m_storageAccountUrl(std::move(storageAccountUrl)),
        m_servicePrincipalId(std::move(servicePrincipalId)),
        m_servicePrincipalSecret(std::move(servicePrincipalSecret)),
        m_tenantId(std::move(tenantId)),
        m_managedIdentityId(std::move(managedIdentityId))
    {
    }

    const std::string& ChainedCredentialInfo::GetDbName() const noexcept
    {
        return m_dbName;
    }

    const std::string& ChainedCredentialInfo::GetStorageAccountUrl() const noexcept
    {
        return m_storageAccountUrl;
    }

    const std::string& ChainedCredentialInfo::GetServicePrincipalId() const noexcept
    {
        return m_servicePrincipalId;
    }

    const std::string& ChainedCredentialInfo::GetServicePrincipalSecret() const noexcept
    {
        return m_servicePrincipalSecret;
    }

    const std::string& ChainedCredentialInfo::GetTenantId() const noexcept
    {
        return m_tenantId;
    }

    std::optional<std::string_view> ChainedCredentialInfo::GetManagedIdentityId() const noexcept
    {
        return m_managedIdentityId;
    }
}
