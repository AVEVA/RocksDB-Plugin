// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/HttpClient/HttpClientOptions.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <random>
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
#ifdef _WIN32
bool HasEnvironmentVariable(const char* name) {
    const char* value = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
    return value != nullptr && *value != '\0';
}

// OpenSSL does not consult the Windows certificate store, so unless SSL_CERT_FILE / SSL_CERT_DIR point it at
// a CA bundle, export the trusted root certificates of the current user (which include the machine roots) into
// a temporary PEM file that the HTTP client loads while it is being constructed.
std::optional<std::filesystem::path> ExportWindowsRootCertificates() {
    HCERTSTORE store = CertOpenSystemStoreW(0, L"ROOT");
    if (store == nullptr) {
        return std::nullopt;
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

    if (pem.empty()) {
        return std::nullopt;
    }

    static std::atomic<uint64_t> counter{0};
    std::random_device random;
    const auto path = std::filesystem::temp_directory_path() /
                      ("aveva-rocksdb-plugin-ca-" + std::to_string(GetCurrentProcessId()) + "-" +
                       std::to_string(counter++) + "-" + std::to_string(random()) + ".pem");
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file << pem;
    file.close();
    if (!file) {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        return std::nullopt;
    }

    return path;
}
#endif

std::unique_ptr<::AVEVA::IHttpClient> CreateHttpClient(boost::asio::io_context& context) {
    ::AVEVA::HttpClientOptions options;
#ifdef _WIN32
    if (!HasEnvironmentVariable("SSL_CERT_FILE") && !HasEnvironmentVariable("SSL_CERT_DIR")) {
        if (const auto caFile = ExportWindowsRootCertificates()) {
            options.SetCaFile(caFile->string());
            std::unique_ptr<::AVEVA::IHttpClient> client;
            try {
                // The CA file is read while the client is constructed, so it can be removed straight after.
                client = ::AVEVA::IHttpClient::Create(context, std::move(options));
            } catch (...) {
                std::error_code ignored;
                std::filesystem::remove(*caFile, ignored);
                throw;
            }

            std::error_code ignored;
            std::filesystem::remove(*caFile, ignored);
            return client;
        }
    }
#endif

    return ::AVEVA::IHttpClient::Create(context, std::move(options));
}
} // namespace

ClientRuntime::ClientRuntime(boost::asio::io_context& context) : m_httpClient(CreateHttpClient(context)) {}

// The io_context belongs to the host, which keeps running it; only the HTTP client created here is released.
ClientRuntime::~ClientRuntime() = default;

::AVEVA::IHttpClient& ClientRuntime::HttpClient() const noexcept { return *m_httpClient; }

void ThrowRequestFailed(const AzureClient::BlobStorageError& error) {
    throw RequestFailedException(error.StatusCode, error.ErrorCode,
                                 error.Message.empty() ? error.Code.message() : error.Message, error.RequestId,
                                 error.Code);
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
