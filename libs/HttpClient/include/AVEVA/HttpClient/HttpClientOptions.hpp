// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

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
        const std::string& GetCaPem() const noexcept;
        std::size_t GetMaxIdleConnectionsPerHost() const noexcept;
        std::chrono::seconds GetIdleConnectionTimeout() const noexcept;
        std::size_t GetMaxConnectionsPerHost() const noexcept;

        void SetTlsVersion(TlsVersion version) noexcept;
        void SetVerifyPeer(bool verifyPeer) noexcept;
        void SetCaFile(std::string caFile);
        void SetCaDirectory(std::string caDirectory);
        // Trusted CA certificates as concatenated PEM text, used in addition to SetCaFile/SetCaDirectory.
        void SetCaPem(std::string caPem);
        void SetMaxIdleConnectionsPerHost(std::size_t maxIdleConnectionsPerHost) noexcept;
        void SetIdleConnectionTimeout(std::chrono::seconds idleConnectionTimeout) noexcept;
        // Caps in-flight requests per origin (scheme, host, port); further requests wait in FIFO order until one
        // finishes, and a waiting request still honors its cancellation slot. 0 (the default) means unlimited.
        void SetMaxConnectionsPerHost(std::size_t maxConnectionsPerHost) noexcept;

      private:
        TlsVersion m_tlsVersion;
        bool m_verifyPeer;
        std::string m_caFile;
        std::string m_caDirectory;
        std::string m_caPem;
        std::size_t m_maxIdleConnectionsPerHost = 6;
        std::chrono::seconds m_idleConnectionTimeout{30};
        std::size_t m_maxConnectionsPerHost = 0;
    };
} // namespace AVEVA
