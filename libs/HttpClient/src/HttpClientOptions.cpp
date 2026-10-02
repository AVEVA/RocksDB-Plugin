#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <utility>

namespace AVEVA
{
    HttpClientOptions::HttpClientOptions()
        : m_tlsVersion(TlsVersion::Tls12OrLater), m_verifyPeer(true)
    {
    }

    TlsVersion HttpClientOptions::GetTlsVersion() const noexcept
    {
        return m_tlsVersion;
    }

    bool HttpClientOptions::GetVerifyPeer() const noexcept
    {
        return m_verifyPeer;
    }

    const std::string& HttpClientOptions::GetCaFile() const noexcept
    {
        return m_caFile;
    }

    const std::string& HttpClientOptions::GetCaDirectory() const noexcept
    {
        return m_caDirectory;
    }

    std::size_t HttpClientOptions::GetMaxIdleConnectionsPerHost() const noexcept
    {
        return m_maxIdleConnectionsPerHost;
    }

    std::chrono::seconds HttpClientOptions::GetIdleConnectionTimeout() const noexcept
    {
        return m_idleConnectionTimeout;
    }

    void HttpClientOptions::SetTlsVersion(TlsVersion version) noexcept
    {
        m_tlsVersion = version;
    }

    void HttpClientOptions::SetVerifyPeer(bool verifyPeer) noexcept
    {
        m_verifyPeer = verifyPeer;
    }

    void HttpClientOptions::SetCaFile(std::string caFile)
    {
        m_caFile = std::move(caFile);
    }

    void HttpClientOptions::SetCaDirectory(std::string caDirectory)
    {
        m_caDirectory = std::move(caDirectory);
    }

    void HttpClientOptions::SetMaxIdleConnectionsPerHost(std::size_t maxIdleConnectionsPerHost) noexcept
    {
        m_maxIdleConnectionsPerHost = maxIdleConnectionsPerHost;
    }

    void HttpClientOptions::SetIdleConnectionTimeout(std::chrono::seconds idleConnectionTimeout) noexcept
    {
        m_idleConnectionTimeout = idleConnectionTimeout;
    }
} // namespace AVEVA
