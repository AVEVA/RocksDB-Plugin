// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "TlsContextConfigurator.hpp"

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <openssl/ssl.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace AVEVA::Private
{
    namespace
    {
        struct SessionDeleter
        {
            void operator()(SSL_SESSION* session) const noexcept
            {
                SSL_SESSION_free(session);
            }
        };

        using UniqueSession = std::unique_ptr<SSL_SESSION, SessionDeleter>;

        // Sessions remembered per origin ("host:service") for one SSL_CTX. Shared by every connection of the
        // owning client, so access is serialized.
        class TlsSessionCache
        {
          public:
            // Bounds memory when a client talks to many origins.
            static constexpr std::size_t MaxEntries = 128;

            void Store(const std::string& origin, UniqueSession session)
            {
                const std::scoped_lock lock(m_mutex);
                if (m_sessions.size() >= MaxEntries && !m_sessions.contains(origin))
                {
                    m_sessions.erase(m_sessions.begin());
                }
                m_sessions.insert_or_assign(origin, std::move(session));
            }

            // A TLS 1.3 ticket must not be presented twice, so it is handed out once; older sessions are reusable.
            [[nodiscard]] UniqueSession Take(const std::string& origin)
            {
                const std::scoped_lock lock(m_mutex);
                const auto found = m_sessions.find(origin);
                if (found == m_sessions.end())
                {
                    return nullptr;
                }
                if (SSL_SESSION_get_protocol_version(found->second.get()) == TLS1_3_VERSION)
                {
                    UniqueSession session = std::move(found->second);
                    m_sessions.erase(found);
                    return session;
                }
                SSL_SESSION_up_ref(found->second.get());
                return UniqueSession{found->second.get()};
            }

          private:
            std::mutex m_mutex;
            std::unordered_map<std::string, UniqueSession> m_sessions;
        };

        void FreeCache(void* /*parent*/, void* data, CRYPTO_EX_DATA* /*ad*/, int /*index*/, long /*argl*/, void* /*argp*/)
        {
            delete static_cast<TlsSessionCache*>(data);
        }

        void FreeOrigin(void* /*parent*/, void* data, CRYPTO_EX_DATA* /*ad*/, int /*index*/, long /*argl*/, void* /*argp*/)
        {
            delete static_cast<std::string*>(data);
        }

        int CacheIndex()
        {
            static const int index = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, &FreeCache);
            return index;
        }

        int OriginIndex()
        {
            static const int index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, &FreeOrigin);
            return index;
        }

        // TLS 1.3 servers send tickets after the handshake, so sessions are captured here rather than after
        // async_handshake returns. Returning 0 leaves the reference with OpenSSL; the cache stores its own copy
        // because OpenSSL marks a connection's session non-resumable when the connection is freed without a
        // clean shutdown (e.g. pooled connections closed by the peer).
        int OnNewSession(SSL* ssl, SSL_SESSION* session)
        {
            auto* cache = static_cast<TlsSessionCache*>(SSL_CTX_get_ex_data(SSL_get_SSL_CTX(ssl), CacheIndex()));
            const auto* origin = static_cast<const std::string*>(SSL_get_ex_data(ssl, OriginIndex()));
            if (cache == nullptr || origin == nullptr)
            {
                return 0;
            }
            try
            {
                if (UniqueSession copy{SSL_SESSION_dup(session)}; copy != nullptr)
                {
                    cache->Store(*origin, std::move(copy));
                }
            }
            catch (const std::exception&)
            {
                // Resumption is only an optimization; losing a session is harmless.
            }
            return 0;
        }
    } // namespace

    void PrepareTlsSessionResumption(SSL* ssl, const std::string& host, const std::string& service)
    {
        auto* cache = static_cast<TlsSessionCache*>(SSL_CTX_get_ex_data(SSL_get_SSL_CTX(ssl), CacheIndex()));
        if (cache == nullptr)
        {
            return;
        }

        auto origin = std::make_unique<std::string>(host + ':' + service);
        if (UniqueSession session = cache->Take(*origin))
        {
            // SSL_set_session takes its own reference; a rejected session simply falls back to a full handshake.
            SSL_set_session(ssl, session.get());
        }
        if (SSL_set_ex_data(ssl, OriginIndex(), origin.get()) == 1)
        {
            origin.release();
        }
    }

    void ConfigureTlsContext(boost::asio::ssl::context& tlsContext, const HttpClientOptions& options)
    {
        const auto nativeContext = tlsContext.native_handle();
        SSL_CTX_set_session_cache_mode(nativeContext, SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
        SSL_CTX_sess_set_new_cb(nativeContext, &OnNewSession);
        auto cache = std::make_unique<TlsSessionCache>();
        if (SSL_CTX_set_ex_data(nativeContext, CacheIndex(), cache.get()) == 1)
        {
            cache.release();
        }
        if (options.GetTlsVersion() == TlsVersion::Tls12)
        {
            if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_2_VERSION) != 1 ||
                SSL_CTX_set_max_proto_version(nativeContext, TLS1_2_VERSION) != 1)
            {
                throw std::runtime_error("Unable to configure TLS 1.2");
            }
        }
        else if (options.GetTlsVersion() == TlsVersion::Tls13)
        {
            if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_3_VERSION) != 1 ||
                SSL_CTX_set_max_proto_version(nativeContext, TLS1_3_VERSION) != 1)
            {
                throw std::runtime_error("Unable to configure TLS 1.3");
            }
        }
        else if (SSL_CTX_set_min_proto_version(nativeContext, TLS1_2_VERSION) != 1)
        {
            throw std::runtime_error("Unable to configure minimum TLS version");
        }

        if (options.GetVerifyPeer())
        {
            tlsContext.set_verify_mode(boost::asio::ssl::verify_peer);
            if (!options.GetCaFile().empty())
            {
                tlsContext.load_verify_file(options.GetCaFile());
            }
            if (!options.GetCaDirectory().empty())
            {
                tlsContext.add_verify_path(options.GetCaDirectory());
            }
            if (!options.GetCaPem().empty())
            {
                const auto& pem = options.GetCaPem();
                tlsContext.add_certificate_authority(boost::asio::buffer(pem.data(), pem.size()));
            }
            if (options.GetCaFile().empty() && options.GetCaDirectory().empty() && options.GetCaPem().empty())
            {
                tlsContext.set_default_verify_paths();
            }
        }
        else
        {
            tlsContext.set_verify_mode(boost::asio::ssl::verify_none);
        }
    }
} // namespace AVEVA::Private
