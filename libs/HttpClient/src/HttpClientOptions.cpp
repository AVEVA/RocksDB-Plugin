// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <stdexcept>
#include <utility>

namespace AVEVA {
TlsVersion HttpClientOptions::GetTlsVersion() const noexcept { return m_tlsVersion; }

PeerVerification HttpClientOptions::GetPeerVerification() const noexcept { return m_peerVerification; }

std::filesystem::path HttpClientOptions::GetCaFile() const { return m_caFile; }

std::filesystem::path HttpClientOptions::GetCaDirectory() const { return m_caDirectory; }

PemText HttpClientOptions::GetCaPem() const { return m_caPem; }

ConnectionCount HttpClientOptions::GetMaxIdleConnectionsPerHost() const noexcept { return m_maxIdleConnectionsPerHost; }

Timeout HttpClientOptions::GetIdleConnectionTimeout() const noexcept { return m_idleConnectionTimeout; }

std::chrono::milliseconds HttpClientOptions::GetIdleProbeThreshold() const noexcept { return m_idleProbeThreshold; }

ConnectionCount HttpClientOptions::GetMaxConnectionsPerHost() const noexcept { return m_maxConnectionsPerHost; }

void HttpClientOptions::SetTlsVersion(TlsVersion version) noexcept { m_tlsVersion = version; }

void HttpClientOptions::SetPeerVerification(PeerVerification peerVerification) noexcept {
    m_peerVerification = peerVerification;
}

void HttpClientOptions::SetCaFile(std::filesystem::path caFile) { m_caFile = std::move(caFile); }

void HttpClientOptions::SetCaDirectory(std::filesystem::path caDirectory) { m_caDirectory = std::move(caDirectory); }

void HttpClientOptions::SetCaPem(PemText caPem) { m_caPem = std::move(caPem); }

void HttpClientOptions::SetMaxIdleConnectionsPerHost(ConnectionCount maxIdleConnectionsPerHost) noexcept {
    m_maxIdleConnectionsPerHost = maxIdleConnectionsPerHost;
}

void HttpClientOptions::SetIdleConnectionTimeout(Timeout idleConnectionTimeout) noexcept {
    m_idleConnectionTimeout = idleConnectionTimeout;
}

void HttpClientOptions::SetIdleProbeThreshold(std::chrono::milliseconds idleProbeThreshold) {
    if (idleProbeThreshold < std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("The idle probe threshold must not be negative.");
    }
    m_idleProbeThreshold = idleProbeThreshold;
}

void HttpClientOptions::SetMaxConnectionsPerHost(ConnectionCount maxConnectionsPerHost) noexcept {
    m_maxConnectionsPerHost = maxConnectionsPerHost;
}

void HttpClientOptions::SetDiagnosticsCallback(HttpDiagnosticsCallback callback) {
    m_diagnosticsCallback = std::move(callback);
}

std::optional<HttpDiagnosticsCallback> HttpClientOptions::GetDiagnosticsCallback() const {
    return m_diagnosticsCallback;
}
} // namespace AVEVA
