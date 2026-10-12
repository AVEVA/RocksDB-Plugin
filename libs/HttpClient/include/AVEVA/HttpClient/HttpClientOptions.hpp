// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <chrono>
#include <cstddef>
#include <string>

namespace AVEVA
{
    /// <summary>
    /// The TLS protocol versions a client may negotiate.
    /// </summary>
    enum class TlsVersion
    {
        /// <summary>TLS 1.2 as the floor, letting the server pick any newer version it supports.</summary>
        Tls12OrLater,
        /// <summary>TLS 1.2 exactly; both the minimum and maximum version are pinned.</summary>
        Tls12,
        /// <summary>TLS 1.3 exactly; both the minimum and maximum version are pinned.</summary>
        Tls13
    };

    /// <summary>
    /// Client-wide settings fixed when the client is created: how TLS is negotiated and verified, how idle
    /// keep-alive connections are pooled, and how many requests may be in flight per origin. Per-request
    /// settings live in <see cref="HttpRequestOptions"/> instead.
    /// </summary>
    class HttpClientOptions
    {
      public:
        /// <summary>
        /// Constructs the defaults: <c>Tls12OrLater</c>, peer verification on, no explicit trust roots, 6 idle
        /// connections per host, a 30 second idle timeout, and no per-origin cap.
        /// </summary>
        HttpClientOptions();

        /// <summary>
        /// The TLS version policy applied to HTTPS connections.
        /// </summary>
        /// <returns>The configured policy.</returns>
        TlsVersion GetTlsVersion() const noexcept;

        /// <summary>
        /// Whether the server certificate chain and host name are verified.
        /// </summary>
        /// <returns>True when verification is on.</returns>
        bool GetVerifyPeer() const noexcept;

        /// <summary>
        /// Path to a PEM file of trusted CA certificates.
        /// </summary>
        /// <returns>The path, empty when none was set.</returns>
        const std::string& GetCaFile() const noexcept;

        /// <summary>
        /// Path to an OpenSSL-style hashed directory of trusted CA certificates.
        /// </summary>
        /// <returns>The path, empty when none was set.</returns>
        const std::string& GetCaDirectory() const noexcept;

        /// <summary>
        /// Trusted CA certificates held in memory as PEM text.
        /// </summary>
        /// <returns>The PEM text, empty when none was set.</returns>
        const std::string& GetCaPem() const noexcept;

        /// <summary>
        /// How many idle keep-alive connections may be kept per origin.
        /// </summary>
        /// <returns>The per-origin idle cap; 6 unless set otherwise.</returns>
        std::size_t GetMaxIdleConnectionsPerHost() const noexcept;

        /// <summary>
        /// How long a pooled connection may sit idle before it is closed.
        /// </summary>
        /// <returns>The idle timeout; 30 seconds unless set otherwise.</returns>
        std::chrono::seconds GetIdleConnectionTimeout() const noexcept;

        /// <summary>
        /// The cap on concurrent in-flight requests per origin.
        /// </summary>
        /// <returns>The cap; 0 means unlimited.</returns>
        std::size_t GetMaxConnectionsPerHost() const noexcept;

        /// <summary>
        /// Sets the TLS version policy.
        /// </summary>
        /// <param name="version">The policy to apply to HTTPS connections.</param>
        void SetTlsVersion(TlsVersion version) noexcept;

        /// <summary>
        /// Enables or disables verification of the server certificate chain and host name. Disabling it removes
        /// the protection TLS offers against an impersonating peer, so it is only appropriate for tests.
        /// </summary>
        /// <param name="verifyPeer">True to verify the peer.</param>
        void SetVerifyPeer(bool verifyPeer) noexcept;

        /// <summary>
        /// Trusted CA certificates from a PEM file.
        /// </summary>
        /// <param name="caFile">Path to a PEM file; empty to set none.</param>
        void SetCaFile(std::string caFile);

        /// <summary>
        /// Trusted CA certificates from an OpenSSL-style hashed directory, used in addition to
        /// <see cref="SetCaFile"/> and <see cref="SetCaPem"/>.
        /// </summary>
        /// <param name="caDirectory">Path to a hashed certificate directory; empty to set none.</param>
        void SetCaDirectory(std::string caDirectory);

        /// <summary>
        /// Trusted CA certificates as concatenated PEM text, used in addition to <see cref="SetCaFile"/> and
        /// <see cref="SetCaDirectory"/>.
        /// </summary>
        /// <param name="caPem">Concatenated PEM certificates; empty to set none.</param>
        void SetCaPem(std::string caPem);

        /// <summary>
        /// Caps how many idle keep-alive connections are kept per origin.
        /// </summary>
        /// <param name="maxIdleConnectionsPerHost">The per-origin idle cap; 0 disables pooling entirely.</param>
        void SetMaxIdleConnectionsPerHost(std::size_t maxIdleConnectionsPerHost) noexcept;

        /// <summary>
        /// How long a pooled connection may stay idle before it is closed.
        /// </summary>
        /// <param name="idleConnectionTimeout">The idle timeout.</param>
        void SetIdleConnectionTimeout(std::chrono::seconds idleConnectionTimeout) noexcept;

        /// <summary>
        /// Caps in-flight requests per origin (scheme, host, port); further requests wait in FIFO order until one
        /// finishes, and a waiting request still honors its cancellation slot.
        /// </summary>
        /// <param name="maxConnectionsPerHost">The per-origin cap; 0 (the default) means unlimited.</param>
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
