#pragma once

#include <chrono>
#include <cstddef>
#include <string>

namespace AVEVA
{
    enum class TlsVersion
    {
        Tls12OrLater,
        Tls12,
        Tls13
    };

    class HttpClientOptions
    {
      public:
        HttpClientOptions();

        TlsVersion GetTlsVersion() const noexcept;
        bool GetVerifyPeer() const noexcept;
        const std::string& GetCaFile() const noexcept;
        const std::string& GetCaDirectory() const noexcept;
        std::size_t GetMaxIdleConnectionsPerHost() const noexcept;
        std::chrono::seconds GetIdleConnectionTimeout() const noexcept;

        void SetTlsVersion(TlsVersion version) noexcept;
        void SetVerifyPeer(bool verifyPeer) noexcept;
        void SetCaFile(std::string caFile);
        void SetCaDirectory(std::string caDirectory);
        void SetMaxIdleConnectionsPerHost(std::size_t maxIdleConnectionsPerHost) noexcept;
        void SetIdleConnectionTimeout(std::chrono::seconds idleConnectionTimeout) noexcept;

      private:
        TlsVersion m_tlsVersion;
        bool m_verifyPeer;
        std::string m_caFile;
        std::string m_caDirectory;
        std::size_t m_maxIdleConnectionsPerHost = 6;
        std::chrono::seconds m_idleConnectionTimeout{30};
    };
} // namespace AVEVA
