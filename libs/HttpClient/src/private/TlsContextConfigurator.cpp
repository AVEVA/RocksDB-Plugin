// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "TlsContextConfigurator.hpp"

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <boost/system/system_error.hpp>

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace AVEVA::Private {
namespace {
struct SessionDeleter {
    void operator()(SSL_SESSION* session) const noexcept { SSL_SESSION_free(session); }
};

using UniqueSession = std::unique_ptr<SSL_SESSION, SessionDeleter>;

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
    const std::unique_ptr<BIO, BioDeleter> bio(BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())));
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

// Sessions remembered per origin ("host:service") for one SSL_CTX. Shared by every connection of the
// owning client, so access is serialized.
class TlsSessionCache {
  public:
    // Bounds memory when a client talks to many origins.
    static constexpr std::size_t MaxEntries = 128;

    // Evicts the least recently used origin once full.
    void Store(const std::string& origin, UniqueSession session) {
        const std::scoped_lock lock(m_mutex);
        if (const auto found = m_sessions.find(origin); found != m_sessions.end()) {
            found->second->session = std::move(session);
            m_order.splice(m_order.begin(), m_order, found->second);
            return;
        }
        if (m_sessions.size() >= MaxEntries) {
            m_sessions.erase(m_order.back().origin);
            m_order.pop_back();
        }
        m_order.push_front(Entry{origin, std::move(session)});
        m_sessions.emplace(origin, m_order.begin());
    }

    // A TLS 1.3 ticket must not be presented twice, so it is handed out once; older sessions are reusable.
    [[nodiscard]] UniqueSession Take(const std::string& origin) {
        const std::scoped_lock lock(m_mutex);
        const auto found = m_sessions.find(origin);
        if (found == m_sessions.end()) {
            return nullptr;
        }
        const auto entry = found->second;
        if (SSL_SESSION_get_protocol_version(entry->session.get()) == TLS1_3_VERSION) {
            UniqueSession session = std::move(entry->session);
            m_order.erase(entry);
            m_sessions.erase(found);
            return session;
        }
        m_order.splice(m_order.begin(), m_order, entry);
        SSL_SESSION_up_ref(entry->session.get());
        return UniqueSession{entry->session.get()};
    }

  private:
    struct Entry {
        std::string origin;
        UniqueSession session;
    };

    std::mutex m_mutex;
    std::list<Entry> m_order; // Most recently used first.
    std::unordered_map<std::string, std::list<Entry>::iterator> m_sessions;
};

void FreeCache(void* /*parent*/, void* data, CRYPTO_EX_DATA* /*ad*/, int /*index*/, long /*argl*/, void* /*argp*/) {
    delete static_cast<TlsSessionCache*>(data);
}

void FreeOrigin(void* /*parent*/, void* data, CRYPTO_EX_DATA* /*ad*/, int /*index*/, long /*argl*/, void* /*argp*/) {
    delete static_cast<std::string*>(data);
}

int CacheIndex() {
    static const int index = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, &FreeCache);
    return index;
}

int OriginIndex() {
    static const int index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, &FreeOrigin);
    return index;
}

// TLS 1.3 servers send tickets after the handshake, so sessions are captured here rather than after
// async_handshake returns. Returning 0 leaves the reference with OpenSSL; the cache stores its own copy
// because OpenSSL marks a connection's session non-resumable when the connection is freed without a
// clean shutdown (e.g. pooled connections closed by the peer).
int OnNewSession(SSL* ssl, SSL_SESSION* session) {
    auto* cache = static_cast<TlsSessionCache*>(SSL_CTX_get_ex_data(SSL_get_SSL_CTX(ssl), CacheIndex()));
    const auto* origin = static_cast<const std::string*>(SSL_get_ex_data(ssl, OriginIndex()));
    if (cache == nullptr || origin == nullptr) {
        return 0;
    }
    try {
        if (UniqueSession copy{SSL_SESSION_dup(session)}; copy != nullptr) {
            cache->Store(*origin, std::move(copy));
        }
    } catch (const std::exception&) {
        // Resumption is only an optimization; losing a session is harmless.
    }
    return 0;
}
} // namespace

void PrepareTlsSessionResumption(SSL* ssl, const std::string& host, const std::string& service) {
    auto* cache = static_cast<TlsSessionCache*>(SSL_CTX_get_ex_data(SSL_get_SSL_CTX(ssl), CacheIndex()));
    if (cache == nullptr) {
        return;
    }

    auto origin = std::make_unique<std::string>();
    origin->reserve(host.size() + 1 + service.size());
    origin->append(host).append(1, ':').append(service);
    if (UniqueSession session = cache->Take(*origin)) {
        // SSL_set_session takes its own reference. If it is refused, no session is set and the handshake is a
        // full one, which is exactly a cache miss.
        SSL_set_session(ssl, session.get());
    }
    if (SSL_set_ex_data(ssl, OriginIndex(), origin.get()) == 1) {
        origin.release();
    }
}

void ConfigureTlsContext(boost::asio::ssl::context& tlsContext, const HttpClientOptions& options) {
    const auto nativeContext = tlsContext.native_handle();
    SSL_CTX_set_session_cache_mode(nativeContext, SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
    SSL_CTX_sess_set_new_cb(nativeContext, &OnNewSession);
    auto cache = std::make_unique<TlsSessionCache>();
    if (SSL_CTX_set_ex_data(nativeContext, CacheIndex(), cache.get()) == 1) {
        cache.release();
    }
    if (options.GetTlsVersion() == TlsVersion::Tls12) {
        if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_2_VERSION) != 1 ||
            SSL_CTX_set_max_proto_version(nativeContext, TLS1_2_VERSION) != 1) {
            throw std::runtime_error("Unable to configure TLS 1.2");
        }
    } else if (options.GetTlsVersion() == TlsVersion::Tls13) {
        if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_3_VERSION) != 1 ||
            SSL_CTX_set_max_proto_version(nativeContext, TLS1_3_VERSION) != 1) {
            throw std::runtime_error("Unable to configure TLS 1.3");
        }
    } else if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_2_VERSION) != 1) {
        throw std::runtime_error("Unable to configure minimum TLS version");
    }

    if (options.GetVerifyPeer()) {
        tlsContext.set_verify_mode(boost::asio::ssl::verify_peer);
        if (!options.GetCaFile().empty()) {
            boost::system::error_code error;
            tlsContext.load_verify_file(options.GetCaFile(), error);
            ThrowIfFailed(error, "Unable to load the TLS CA file '" + options.GetCaFile() + "'");
        }
        if (!options.GetCaDirectory().empty()) {
            // OpenSSL only looks inside a CA directory when verifying, so a bad path would otherwise surface as a
            // verification failure on the first request.
            if (std::error_code directoryError;
                !std::filesystem::is_directory(options.GetCaDirectory(), directoryError)) {
                throw boost::system::system_error(
                    boost::system::error_code(boost::system::errc::not_a_directory, boost::system::generic_category()),
                    "The TLS CA directory '" + options.GetCaDirectory() + "' is not a directory");
            }
            boost::system::error_code error;
            tlsContext.add_verify_path(options.GetCaDirectory(), error);
            ThrowIfFailed(error, "Unable to use the TLS CA directory '" + options.GetCaDirectory() + "'");
        }
        if (!options.GetCaPem().empty()) {
            const auto& pem = options.GetCaPem();
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
        if (options.GetCaFile().empty() && options.GetCaDirectory().empty() && options.GetCaPem().empty()) {
            boost::system::error_code error;
            tlsContext.set_default_verify_paths(error);
            ThrowIfFailed(error, "Unable to load the default TLS CA paths");
        }
    } else {
        tlsContext.set_verify_mode(boost::asio::ssl::verify_none);
    }
}
} // namespace AVEVA::Private
