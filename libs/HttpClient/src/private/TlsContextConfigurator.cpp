// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "TlsContextConfigurator.hpp"

#include "AVEVA/HttpClient/HttpClientOptions.hpp"
#include "TlsSessionCache.hpp"

#include <boost/system/system_error.hpp>

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <gsl/util>

#include <filesystem>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace AVEVA::Private {
namespace {
// Keeps the underlying error code and adds which setting failed, so a bad option is easy to find.
void ThrowIfFailed(const boost::system::error_code& error, const std::string& what) {
    if (error) {
        throw boost::system::system_error(error, what);
    }
}

struct BioDeleter {
    void operator()(BIO* bio) const noexcept { BIO_free(bio); }
};

int CountPemCertificates(const std::string& pem) {
    // BIO_new_mem_buf takes an int length, so a larger buffer cannot be parsed.
    if (pem.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return 0;
    }
    const std::unique_ptr<BIO, BioDeleter> bio(BIO_new_mem_buf(pem.data(), gsl::narrow_cast<int>(pem.size())));
    if (!bio) {
        return 0;
    }
    int count = 0;
    while (X509* certificate = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)) {
        X509_free(certificate);
        ++count;
    }
    ERR_clear_error(); // The loop always ends on a read error, normally just the end of the input.
    return count;
}

/// <summary>What a client's SSL_CTX carries for its new-session callback.</summary>
struct ContextState {
    TlsSessionCache cache;
    std::shared_ptr<const DiagnosticsSink> diagnostics;
};

/// <summary>
/// An OpenSSL ex_data slot owning a T. OpenSSL calls the registered free callback when the owning SSL or SSL_CTX is
/// freed; the callback adopts the pointer back into a unique_ptr, so ownership moves to OpenSSL and back without a
/// raw delete anywhere.
/// </summary>
template <typename T> struct ContextSlot {
    static void Free(void* /*parent*/, void* data, CRYPTO_EX_DATA* /*ad*/, int /*index*/, long /*argl*/,
                     void* /*argp*/) {
        const std::unique_ptr<T> owned(static_cast<T*>(data));
    }

    static int Index() {
        static const int index = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, &Free);
        return index;
    }

    // Hands ownership to the context; if OpenSSL refuses, the value is simply destroyed here.
    static void Attach(SSL_CTX* context, std::unique_ptr<T> value) {
        if (SSL_CTX_set_ex_data(context, Index(), value.get()) == 1) {
            static_cast<void>(value.release());
        }
    }

    static T* Find(const SSL_CTX* context) noexcept { return static_cast<T*>(SSL_CTX_get_ex_data(context, Index())); }
};

/// <summary>The SSL counterpart of <see cref="ContextSlot"/>.</summary>
template <typename T> struct ConnectionSlot {
    static void Free(void* /*parent*/, void* data, CRYPTO_EX_DATA* /*ad*/, int /*index*/, long /*argl*/,
                     void* /*argp*/) {
        const std::unique_ptr<T> owned(static_cast<T*>(data));
    }

    static int Index() {
        static const int index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, &Free);
        return index;
    }

    static void Attach(SSL* ssl, std::unique_ptr<T> value) {
        if (SSL_set_ex_data(ssl, Index(), value.get()) == 1) {
            static_cast<void>(value.release());
        }
    }

    static T* Find(const SSL* ssl) noexcept { return static_cast<T*>(SSL_get_ex_data(ssl, Index())); }
};

using StateSlot = ContextSlot<ContextState>;
using OriginSlot = ConnectionSlot<std::string>;

// TLS 1.3 servers send tickets after the handshake, so sessions are captured here rather than after
// async_handshake returns. Returning 0 leaves the reference with OpenSSL; the cache stores its own copy
// because OpenSSL marks a connection's session non-resumable when the connection is freed without a
// clean shutdown (e.g. pooled connections closed by the peer).
int OnNewSession(SSL* ssl, SSL_SESSION* session) {
    auto* const state = StateSlot::Find(SSL_get_SSL_CTX(ssl));
    const auto* const origin = OriginSlot::Find(ssl);
    if (state == nullptr || origin == nullptr) {
        return 0;
    }
    try {
        if (UniqueSession copy{SSL_SESSION_dup(session)}; copy != nullptr) {
            state->cache.Store(*origin, std::move(copy));
        }
    } catch (const std::exception& failure) {
        // Resumption is only an optimization: losing a session costs one full handshake later.
        DiagnosticsSink::Report(state->diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Handshake,
                                {}, {}, {}, failure.what());
    }
    return 0;
}

// Applies the requested protocol range; each failure names the version that could not be configured.
void ApplyTlsVersion(SSL_CTX* nativeContext, TlsVersion version) {
    if (version == TlsVersion::Tls12) {
        if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_2_VERSION) != 1 ||
            SSL_CTX_set_max_proto_version(nativeContext, TLS1_2_VERSION) != 1) {
            throw std::runtime_error("Unable to configure TLS 1.2");
        }
    } else if (version == TlsVersion::Tls13) {
        if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_3_VERSION) != 1 ||
            SSL_CTX_set_max_proto_version(nativeContext, TLS1_3_VERSION) != 1) {
            throw std::runtime_error("Unable to configure TLS 1.3");
        }
    } else if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_2_VERSION) != 1) {
        throw std::runtime_error("Unable to configure minimum TLS version");
    }
}

// Loads every configured trust root, or the system defaults when none is configured.
void ApplyTrustRoots(boost::asio::ssl::context& tlsContext, const HttpClientOptions& options) {
    const auto caFile = options.GetCaFile();
    const auto caDirectory = options.GetCaDirectory();
    const auto caPem = options.GetCaPem();
    if (!caFile.empty()) {
        boost::system::error_code error;
        tlsContext.load_verify_file(caFile.string(), error);
        ThrowIfFailed(error, "Unable to load the TLS CA file '" + caFile.string() + "'");
    }
    if (!caDirectory.empty()) {
        // OpenSSL only looks inside a CA directory when verifying, so a bad path would otherwise surface as a
        // verification failure on the first request.
        if (std::error_code directoryError; !std::filesystem::is_directory(caDirectory, directoryError)) {
            throw boost::system::system_error(
                boost::system::error_code(boost::system::errc::not_a_directory, boost::system::generic_category()),
                "The TLS CA directory '" + caDirectory.string() + "' is not a directory");
        }
        boost::system::error_code error;
        tlsContext.add_verify_path(caDirectory.string(), error);
        ThrowIfFailed(error, "Unable to use the TLS CA directory '" + caDirectory.string() + "'");
    }
    if (const auto& pem = caPem.Value(); !pem.empty()) {
        // add_certificate_authority silently accepts text that holds no certificate at all.
        if (CountPemCertificates(pem) == 0) {
            throw boost::system::system_error(
                boost::system::error_code(boost::system::errc::invalid_argument, boost::system::generic_category()),
                "The TLS CA PEM does not contain a readable certificate");
        }
        boost::system::error_code error;
        tlsContext.add_certificate_authority(boost::asio::buffer(pem.data(), pem.size()), error);
        ThrowIfFailed(error, "Unable to load the TLS CA PEM");
    }
    if (caFile.empty() && caDirectory.empty() && caPem.Value().empty()) {
        boost::system::error_code error;
        tlsContext.set_default_verify_paths(error);
        ThrowIfFailed(error, "Unable to load the default TLS CA paths");
    }
}
} // namespace

void PrepareTlsSessionResumption(SSL* ssl, const TlsConnectionKey& origin) {
    auto* const state = StateSlot::Find(SSL_get_SSL_CTX(ssl));
    if (state == nullptr) {
        return;
    }

    auto originName = std::make_unique<std::string>();
    originName->reserve(origin.host.size() + 1 + origin.service.size());
    originName->append(origin.host).append(1, ':').append(origin.service);
    if (UniqueSession session = state->cache.Take(*originName)) {
        // SSL_set_session takes its own reference. If it is refused, no session is set and the handshake is a
        // full one, which is exactly a cache miss.
        SSL_set_session(ssl, session.get());
    }
    OriginSlot::Attach(ssl, std::move(originName));
}

std::shared_ptr<boost::asio::ssl::context> MakeTlsContext(const HttpClientOptions& options,
                                                          std::shared_ptr<const DiagnosticsSink> diagnostics) {
    auto tlsContext = std::make_shared<boost::asio::ssl::context>(boost::asio::ssl::context::tls_client);
    tlsContext->set_options(boost::asio::ssl::context::default_workarounds | boost::asio::ssl::context::no_sslv2 |
                            boost::asio::ssl::context::no_sslv3);
    const auto nativeContext = tlsContext->native_handle();
    SSL_CTX_set_session_cache_mode(nativeContext, SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
    SSL_CTX_sess_set_new_cb(nativeContext, &OnNewSession);
    auto state = std::make_unique<ContextState>();
    state->diagnostics = std::move(diagnostics);
    StateSlot::Attach(nativeContext, std::move(state));
    ApplyTlsVersion(nativeContext, options.GetTlsVersion());

    if (options.GetPeerVerification() == PeerVerification::Verify) {
        tlsContext->set_verify_mode(boost::asio::ssl::verify_peer);
        ApplyTrustRoots(*tlsContext, options);
    } else {
        tlsContext->set_verify_mode(boost::asio::ssl::verify_none);
    }
    return tlsContext;
}
} // namespace AVEVA::Private
