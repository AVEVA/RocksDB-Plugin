// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/HttpClient/HttpClientOptions.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <wincrypt.h>
#include <windows.h>
#endif

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
constexpr std::size_t MaxIdleConnectionsPerHost = 64;

#ifdef _WIN32
bool HasEnvironmentVariable(const char* name) {
    const char* value = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
    return value != nullptr && *value != '\0';
}

// OpenSSL does not consult the Windows certificate store, so unless SSL_CERT_FILE / SSL_CERT_DIR point it at
// a CA bundle, export the trusted root certificates of the current user (which include the machine roots) into
// a PEM string that is handed to the HTTP client in memory.
//
// Limits: only the ROOT store is exported (intermediate CAs and certificates added after the first call are not
// seen) and revocation is not checked. Set SSL_CERT_FILE/SSL_CERT_DIR to override.
std::string BuildWindowsRootCertificatePem() {
    HCERTSTORE store = CertOpenSystemStoreW(0, L"ROOT");
    if (store == nullptr) {
        return {};
    }

    std::string pem;
    PCCERT_CONTEXT certificate = nullptr;
    while ((certificate = CertEnumCertificatesInStore(store, certificate)) != nullptr) {
        DWORD size = 0;
        if (!CryptBinaryToStringA(certificate->pbCertEncoded, certificate->cbCertEncoded, CRYPT_STRING_BASE64HEADER,
                                  nullptr, &size)) {
            continue;
        }

        std::string encoded(size, '\0');
        if (CryptBinaryToStringA(certificate->pbCertEncoded, certificate->cbCertEncoded, CRYPT_STRING_BASE64HEADER,
                                 encoded.data(), &size)) {
            encoded.resize(size);
            pem += encoded;
        }
    }
    CertCloseStore(store, 0);
    return pem;
}

// Enumerating the store is the expensive part, so it happens once per process (thread-safe static).
const std::string& WindowsRootCertificatePem() {
    static const std::string pem = BuildWindowsRootCertificatePem();
    return pem;
}
#endif

std::shared_ptr<::AVEVA::HttpClient> CreateHttpClient(boost::asio::io_context& context) {
    ::AVEVA::HttpClientOptions options;
    // MultiRead waves, directory deletes and per-file uploads run in parallel; the default of 6 idle connections
    // would close and re-handshake most of them after every burst.
    options.SetMaxIdleConnectionsPerHost(MaxIdleConnectionsPerHost);
#ifdef _WIN32
    if (!HasEnvironmentVariable("SSL_CERT_FILE") && !HasEnvironmentVariable("SSL_CERT_DIR")) {
        // Passed in memory: a temporary file could be swapped by another local process between write and read.
        options.SetCaPem(WindowsRootCertificatePem());
    }
#endif

    return std::make_shared<::AVEVA::HttpClient>(context, std::move(options));
}
} // namespace

// The io_context belongs to the host, which keeps running it; only the HTTP client created here is released.
std::shared_ptr<::AVEVA::HttpClient> HttpClientFactory<::AVEVA::HttpClient>::Create(boost::asio::io_context& context) {
    return CreateHttpClient(context);
}

void ClientRuntimeBase::FenceWrites() noexcept { m_writesFenced.store(true); }

bool ClientRuntimeBase::WritesFenced() const noexcept { return m_writesFenced.load(); }

void ClientRuntimeBase::ThrowIfWritesFenced() const {
    if (WritesFenced()) {
        throw WritesFencedException("Writes are disabled because the filesystem can no longer guarantee it holds its "
                                    "locks");
    }
    if (const auto remaining = WriteTimeRemaining();
        remaining && *remaining <= std::chrono::steady_clock::duration::zero()) {
        throw WritesFencedException("Writes are disabled because the lease renewal deadline has passed");
    }
}

void ClientRuntimeBase::SetWriteDeadlineSource(WriteDeadlineSource source) {
    const std::scoped_lock lock(m_deadlineMutex);
    m_deadlineSource = std::move(source);
}

std::optional<std::chrono::steady_clock::duration> ClientRuntimeBase::WriteTimeRemaining() const {
    const std::scoped_lock lock(m_deadlineMutex);
    return m_deadlineSource ? m_deadlineSource() : std::nullopt;
}

HttpRequestOptions BoundByWriteDeadline(const ClientRuntimeBase& runtime, HttpRequestOptions options) {
    const auto remaining = runtime.WriteTimeRemaining();
    if (!remaining) {
        return options;
    }
    // At least 1 ms: a zero timeout may mean "none" to the transport. ThrowIfWritesFenced rejects an expired deadline
    // before a request is built, so this only guards the race where it expires in between.
    const auto budget =
        std::max(std::chrono::milliseconds(1), std::chrono::duration_cast<std::chrono::milliseconds>(*remaining));
    options.SetTimeout(std::min(options.GetTimeout(), budget));
    options.SetTotalDeadline(std::min(options.GetTotalDeadline().value_or(budget), budget));
    return options;
}

void ThrowRequestFailed(const AzureClient::BlobStorageError& error) {
    throw RequestFailedException(error.StatusCode, error.ErrorCode,
                                 error.Message.empty() ? error.Code.message() : error.Message, error.RequestId,
                                 error.Code, error.AuthenticationDetail);
}

HttpRequestOptions RequestOptionsForTransfer(const HttpRequestOptions& defaults, uint64_t bytes) {
    // Assume at least 1 MiB/s of throughput on top of the default timeout.
    static const constexpr uint64_t bytesPerSecond = 1024ULL * 1024ULL;
    static const constexpr uint64_t responseOverhead = 64ULL * 1024ULL;

    HttpRequestOptions options = defaults;
    options.SetResponseBodyLimit(std::max(defaults.GetResponseBodyLimit(), bytes + responseOverhead));
    options.SetTimeout(defaults.GetTimeout() +
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::seconds(static_cast<std::chrono::seconds::rep>(bytes / bytesPerSecond))));
    return options;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
