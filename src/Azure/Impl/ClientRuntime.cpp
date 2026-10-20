// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <AVEVA/HttpClient/HttpClientOptions.hpp>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

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

// Surfaces events the HTTP client would otherwise swallow (stale-connection retries, unpooled connections, swallowed
// exceptions, timeouts by phase). Retries and DNS invalidations are routine, so only the rest is a warning.
void LogHttpDiagnostic(const ::AVEVA::HttpDiagnostic& event) {
    using Kind = ::AVEVA::HttpDiagnosticKind;
    static boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level> logger;
    const auto level = (event.Kind == Kind::StaleConnectionRetry || event.Kind == Kind::DnsInvalidated)
                           ? boost::log::trivial::debug
                           : boost::log::trivial::warning;
    BOOST_LOG_SEV(logger, level) << "HTTP client: " << event.Message << " [origin=" << event.Origin
                                 << ", phase=" << static_cast<int>(event.Phase)
                                 << (event.Error ? ", error=" + event.Error.message() : std::string{}) << "]";
}

std::shared_ptr<::AVEVA::HttpClient> CreateHttpClient(boost::asio::io_context& context) {
    ::AVEVA::HttpClientOptions options;
    // MultiRead waves, directory deletes and per-file uploads run in parallel; the default of 6 idle connections
    // would close and re-handshake most of them after every burst.
    options.SetMaxIdleConnectionsPerHost(MaxIdleConnectionsPerHost);
    options.SetDiagnosticsCallback(LogHttpDiagnostic);
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

ClientRuntimeBase::~ClientRuntimeBase() {
    if (m_writerPool) {
        m_writerPool->join();
    }
}

boost::asio::thread_pool::executor_type ClientRuntimeBase::DownloadWriter() const {
    std::call_once(m_writerOnce, [this] {
        constexpr std::size_t WriterThreads = 2;
        m_writerPool = std::make_unique<boost::asio::thread_pool>(WriterThreads);
    });
    return m_writerPool->get_executor();
}

void ClientRuntimeBase::FenceWrites() noexcept { m_writesFenced.store(true, std::memory_order_release); }

bool ClientRuntimeBase::WritesFenced() const noexcept { return m_writesFenced.load(std::memory_order_acquire); }

std::optional<std::chrono::steady_clock::duration> ClientRuntimeBase::ThrowIfWritesFenced() const {
    if (WritesFenced()) {
        throw WritesFencedException("Writes are disabled because the filesystem can no longer guarantee it holds its "
                                    "locks");
    }
    const auto remaining = WriteTimeRemaining();
    if (remaining && *remaining <= std::chrono::steady_clock::duration::zero()) {
        throw WritesFencedException("Writes are disabled because the lease renewal deadline has passed");
    }
    return remaining;
}

std::optional<int64_t> ClientRuntimeBase::CachedBlobSize(std::string_view blobUrl) const {
    const std::scoped_lock lock(m_sizeMutex);
    const auto it = m_sizes.find(blobUrl);
    if (it == m_sizes.end()) {
        return std::nullopt;
    }
    if (MetadataNow() - it->second.FetchedAt >= Configuration::MetadataCacheTtl) {
        m_sizes.erase(it);
        return std::nullopt;
    }
    return it->second.Size;
}

std::chrono::steady_clock::time_point ClientRuntimeBase::MetadataNow() const {
    if (m_manualMetadataClock) {
        return std::chrono::steady_clock::time_point(std::chrono::nanoseconds(m_manualMetadataClock->load()));
    }
    return std::chrono::steady_clock::now();
}

void ClientRuntimeBase::SetMetadataClockForTesting(const std::atomic<int64_t>* manualClockNanos) noexcept {
    m_manualMetadataClock = manualClockNanos;
}

uint64_t ClientRuntimeBase::BlobSizeEpoch() const noexcept { return m_sizeEpoch.load(); }

std::optional<ClientRuntimeBase::CachedMetadata> ClientRuntimeBase::CachedBlobMetadata(std::string_view blobUrl) const {
    const std::scoped_lock lock(m_sizeMutex);
    const auto it = m_sizes.find(blobUrl);
    if (it == m_sizes.end()) {
        return std::nullopt;
    }
    if (MetadataNow() - it->second.FetchedAt >= Configuration::MetadataCacheTtl) {
        m_sizes.erase(it);
        return std::nullopt;
    }
    if (!it->second.ETag) {
        return std::nullopt;
    }
    return CachedMetadata{it->second.Size, *it->second.ETag, it->second.FetchedAt};
}

void ClientRuntimeBase::CacheBlobSize(std::string_view blobUrl, const int64_t size, const uint64_t epoch) const {
    StoreBlobSize(blobUrl, size, std::nullopt, epoch);
}

void ClientRuntimeBase::CacheBlobMetadata(std::string_view blobUrl, const int64_t size, std::string etag,
                                          const uint64_t epoch) const {
    StoreBlobSize(blobUrl, size, std::move(etag), epoch);
}

void ClientRuntimeBase::StoreBlobSize(std::string_view blobUrl, const int64_t size, std::optional<std::string> etag,
                                      const uint64_t epoch) const {
    constexpr std::size_t MaxCachedSizes = 4096;
    const auto now = MetadataNow();
    const std::scoped_lock lock(m_sizeMutex);
    // Invalidations bump the epoch under this lock, so a fetch that started before one can never be stored.
    if (m_sizeEpoch.load() != epoch) {
        return;
    }
    if (m_sizes.size() >= MaxCachedSizes) {
        std::erase_if(m_sizes, [&](const auto& entry) {
            return now - entry.second.FetchedAt >= Configuration::MetadataCacheTtl;
        });
        // Still full of live entries: drop one arbitrary entry rather than the whole working set.
        if (m_sizes.size() >= MaxCachedSizes && !m_sizes.contains(blobUrl)) {
            m_sizes.erase(m_sizes.begin());
        }
    }
    if (const auto it = m_sizes.find(blobUrl); it != m_sizes.end()) {
        it->second = CachedSize{size, std::move(etag), now};
    } else {
        m_sizes.emplace(blobUrl, CachedSize{size, std::move(etag), now});
    }
}

void ClientRuntimeBase::InvalidateBlobSize(std::string_view blobUrl) const {
    const std::scoped_lock lock(m_sizeMutex);
    ++m_sizeEpoch;
    if (const auto it = m_sizes.find(blobUrl); it != m_sizes.end()) {
        m_sizes.erase(it);
    }
}

void ClientRuntimeBase::InvalidateAllBlobSizes() const {
    const std::scoped_lock lock(m_sizeMutex);
    ++m_sizeEpoch;
    m_sizes.clear();
}

void ClientRuntimeBase::SetLeaseDeadline(const void* owner,
                                         const std::optional<std::chrono::steady_clock::time_point> deadline) {
    const std::scoped_lock lock(m_leaseMutex);
    const auto it =
        std::ranges::find(m_leaseDeadlines, owner, &std::pair<const void*, std::chrono::steady_clock::rep>::first);
    if (!deadline) {
        if (it != m_leaseDeadlines.end()) {
            m_leaseDeadlines.erase(it);
        }
    } else if (it != m_leaseDeadlines.end()) {
        it->second = deadline->time_since_epoch().count();
    } else {
        m_leaseDeadlines.emplace_back(owner, deadline->time_since_epoch().count());
    }
    auto earliest = kNoDeadline;
    for (const auto& entry : m_leaseDeadlines) {
        earliest = (std::min)(earliest, entry.second);
    }
    m_writeDeadline.store(earliest, std::memory_order_release);
}

std::optional<std::chrono::steady_clock::duration> ClientRuntimeBase::WriteTimeRemaining() const {
    const auto deadline = m_writeDeadline.load(std::memory_order_acquire);
    if (deadline == kNoDeadline) {
        return std::nullopt;
    }
    return std::chrono::steady_clock::time_point(std::chrono::steady_clock::duration(deadline)) -
           std::chrono::steady_clock::now();
}

AVEVA::AzureClient::AzureRequestOptions BoundByWriteDeadline(const ClientRuntimeBase& runtime,
                                                             AVEVA::AzureClient::AzureRequestOptions options) {
    return BoundByWriteDeadline(runtime.WriteTimeRemaining(), std::move(options));
}

AVEVA::AzureClient::AzureRequestOptions
BoundByWriteDeadline(const std::optional<std::chrono::steady_clock::duration> remaining,
                     AVEVA::AzureClient::AzureRequestOptions options) {
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
                                 error.Code, error.AuthenticationDetail, error.RetryAfter);
}

AVEVA::AzureClient::AzureRequestOptions
RequestOptionsForTransfer(const AVEVA::AzureClient::AzureRequestOptions& defaults, uint64_t bytes) {
    // Assume at least 1 MiB/s of throughput on top of the default timeout.
    static const constexpr uint64_t bytesPerSecond = 1024ULL * 1024ULL;
    static const constexpr uint64_t responseOverhead = 64ULL * 1024ULL;

    AVEVA::AzureClient::AzureRequestOptions options = defaults;
    options.SetResponseBodyLimit(std::max(defaults.GetResponseBodyLimit(), bytes + responseOverhead));
    options.SetTimeout(defaults.GetTimeout() +
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::seconds(static_cast<std::chrono::seconds::rep>(bytes / bytesPerSecond))));
    return options;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
