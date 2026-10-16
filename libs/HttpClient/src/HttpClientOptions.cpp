// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <stdexcept>
#include <utility>

namespace AVEVA {
TlsVersion HttpClientOptions::GetTlsVersion() const noexcept { return m_tlsVersion; }

bool HttpClientOptions::GetVerifyPeer() const noexcept { return m_verifyPeer; }

const std::string& HttpClientOptions::GetCaFile() const noexcept { return m_caFile; }

const std::string& HttpClientOptions::GetCaDirectory() const noexcept { return m_caDirectory; }

const std::string& HttpClientOptions::GetCaPem() const noexcept { return m_caPem; }

std::size_t HttpClientOptions::GetMaxIdleConnectionsPerHost() const noexcept { return m_maxIdleConnectionsPerHost; }

std::chrono::seconds HttpClientOptions::GetIdleConnectionTimeout() const noexcept { return m_idleConnectionTimeout; }

std::chrono::milliseconds HttpClientOptions::GetIdleProbeThreshold() const noexcept { return m_idleProbeThreshold; }

std::size_t HttpClientOptions::GetMaxConnectionsPerHost() const noexcept { return m_maxConnectionsPerHost; }

void HttpClientOptions::SetTlsVersion(TlsVersion version) noexcept { m_tlsVersion = version; }

void HttpClientOptions::SetVerifyPeer(bool verifyPeer) noexcept { m_verifyPeer = verifyPeer; }

void HttpClientOptions::SetCaFile(std::string caFile) { m_caFile = std::move(caFile); }

void HttpClientOptions::SetCaDirectory(std::string caDirectory) { m_caDirectory = std::move(caDirectory); }

void HttpClientOptions::SetCaPem(std::string caPem) { m_caPem = std::move(caPem); }

void HttpClientOptions::SetMaxIdleConnectionsPerHost(std::size_t maxIdleConnectionsPerHost) noexcept {
    m_maxIdleConnectionsPerHost = maxIdleConnectionsPerHost;
}

void HttpClientOptions::SetIdleConnectionTimeout(std::chrono::seconds idleConnectionTimeout) {
    if (idleConnectionTimeout < std::chrono::seconds::zero()) {
        throw std::invalid_argument("The idle connection timeout must not be negative.");
    }
    m_idleConnectionTimeout = idleConnectionTimeout;
}

void HttpClientOptions::SetIdleProbeThreshold(std::chrono::milliseconds idleProbeThreshold) {
    if (idleProbeThreshold < std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("The idle probe threshold must not be negative.");
    }
    m_idleProbeThreshold = idleProbeThreshold;
}
void HttpClientOptions::SetMaxConnectionsPerHost(std::size_t maxConnectionsPerHost) noexcept {
    m_maxConnectionsPerHost = maxConnectionsPerHost;
}

void HttpClientOptions::SetDiagnosticsCallback(HttpDiagnosticsCallback callback) {
    m_diagnosticsCallback = std::move(callback);
}

const HttpDiagnosticsCallback& HttpClientOptions::GetDiagnosticsCallback() const noexcept {
    return m_diagnosticsCallback;
}
} // namespace AVEVA
