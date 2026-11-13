// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"

#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/HttpClient/HttpClientOptions.hpp>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <limits>
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
constexpr ::AVEVA::ConnectionCount MaxIdleConnectionsPerHost{128};

#ifdef _WIN32
enum class EnvironmentVariable : std::uint8_t { Unset, Set };

EnvironmentVariable EnvironmentVariableState(const char* name) {
    return ::AVEVA::AzureClient::Private::GetEnvironmentValue(name).empty() ? EnvironmentVariable::Unset
                                                                            : EnvironmentVariable::Set;
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

// Surfaces events the HTTP client would otherwise swallow (stale-connection retries, unpooled connections, swallowed
// exceptions, timeouts by phase). Retries and DNS invalidations are routine, so only the rest is a warning. The HTTP
// client only accepts a receiver that cannot throw, so a logging failure falls back to stderr.
void LogHttpDiagnostic(const ::AVEVA::HttpDiagnostic& event) noexcept {
    using Kind = ::AVEVA::HttpDiagnosticKind;
    try {
        static boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level> logger;
        const auto level = (event.Kind == Kind::StaleConnectionRetry || event.Kind == Kind::DnsInvalidated)
                               ? boost::log::trivial::debug
                               : boost::log::trivial::warning;
        BOOST_LOG_SEV(logger, level) << "HTTP client: " << event.Message.Value() << " [origin=" << event.Origin.Value()
                                     << ", phase=" << static_cast<int>(event.Phase)
                                     << (event.Error ? ", error=" + event.Error.message() : std::string{}) << "]";
    } catch (const std::exception&) {
        std::fputs("HTTP client: failed to log a diagnostic event\n", stderr);
    }
}

std::shared_ptr<::AVEVA::HttpClient> CreateHttpClient(boost::asio::io_context& context) {
    ::AVEVA::HttpClientOptions options;
    // MultiRead waves, directory deletes and per-file uploads run in parallel; the default of 6 idle connections
    // would close and re-handshake most of them after every burst.
    options.SetMaxIdleConnectionsPerHost(MaxIdleConnectionsPerHost);
    options.SetDiagnosticsCallback(::AVEVA::HttpDiagnosticsCallback{LogHttpDiagnostic});
#ifdef _WIN32
    if (EnvironmentVariableState("SSL_CERT_FILE") == EnvironmentVariable::Unset &&
        EnvironmentVariableState("SSL_CERT_DIR") == EnvironmentVariable::Unset) {
        // Passed in memory: a temporary file could be swapped by another local process between write and read.
        options.SetCaPem(::AVEVA::PemText{WindowsRootCertificatePem()});
    }
#endif

    return std::make_shared<::AVEVA::HttpClient>(context, std::move(options));
}
} // namespace

// The io_context belongs to the host, which keeps running it; only the HTTP client created here is released.
std::shared_ptr<::AVEVA::HttpClient> HttpClientFactory<::AVEVA::HttpClient>::Create(boost::asio::io_context& context) {
    return CreateHttpClient(context);
}

AVEVA::AzureClient::AzureRequestOptions BoundByWriteDeadline(const ClientRuntimeBase& runtime,
                                                             AVEVA::AzureClient::AzureRequestOptions options) {
    return BoundByWriteDeadline(runtime.Fence().TimeRemaining(), std::move(options));
}

AVEVA::AzureClient::AzureRequestOptions
BoundByWriteDeadline(const std::optional<std::chrono::steady_clock::duration> remaining,
                     AVEVA::AzureClient::AzureRequestOptions options) {
    if (!remaining) {
        return options;
    }
    // At least 1 ms, because a timeout must be positive. ThrowIfWritesFenced rejects an expired deadline before a
    // request is built, so this only guards the race where it expires in between.
    const auto budget =
        std::max(std::chrono::milliseconds(1), std::chrono::duration_cast<std::chrono::milliseconds>(*remaining));
    options.SetTimeout(std::min(options.GetTimeout(), ::AVEVA::Timeout{budget}));
    options.SetTotalDeadline(std::min(options.GetTotalDeadline().value_or(budget), budget));
    return options;
}

RequestFailedException MakeRequestFailed(const AzureClient::BlobStorageError& error) {
    return RequestFailedException(HttpStatusCode{error.StatusCode.Value()}, ServiceErrorCode{error.ErrorCode},
                                  ErrorMessage{error.Message.empty() ? error.Code.message() : error.Message},
                                  ServiceRequestId{error.RequestId}, error.Code, error.AuthenticationDetail,
                                  error.RetryAfter);
}

void ThrowRequestFailed(const AzureClient::BlobStorageError& error) { throw MakeRequestFailed(error); }

AVEVA::AzureClient::AzureRequestOptions
RequestOptionsForTransfer(const AVEVA::AzureClient::AzureRequestOptions& defaults, uint64_t bytes) {
    // Assume at least 1 MiB/s of throughput on top of the default timeout.
    static const constexpr uint64_t bytesPerSecond = 1024ULL * 1024ULL;
    static const constexpr uint64_t responseOverhead = 64ULL * 1024ULL;

    // A transfer this large can never be served; saturating keeps the limit from wrapping to a tiny value.
    const uint64_t bodyLimit = bytes > std::numeric_limits<uint64_t>::max() - responseOverhead
                                   ? std::numeric_limits<uint64_t>::max()
                                   : bytes + responseOverhead;
    AVEVA::AzureClient::AzureRequestOptions options = defaults;
    options.SetResponseBodyLimit(std::max(defaults.GetResponseBodyLimit(), ::AVEVA::ByteCount{bodyLimit}));
    options.SetTimeout(::AVEVA::Timeout{defaults.GetTimeout().Value() +
                                        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::seconds(
                                            static_cast<std::chrono::seconds::rep>(bytes / bytesPerSecond)))});
    return options;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
