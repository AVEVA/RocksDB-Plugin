// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>

#include <gtest/gtest.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace
{
    namespace asio = boost::asio;
    using Tcp = asio::ip::tcp;

    TEST(HttpClientTls, StalledServerCompletesOnceWithTimeout)
    {
        AVEVA::HttpRequestOptions requestOptions;
        requestOptions.SetTimeout(std::chrono::milliseconds(100));
        auto result = HttpClientTests::Exchange({}, {}, requestOptions, true);

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
    }

    TEST(HttpClientTls, UntrustedCertificateReturnsTlsError)
    {
        asio::io_context context;
        asio::ssl::context serverContext(asio::ssl::context::tls_server);
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"),
            EVP_PKEY_free);
        std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), X509_free);
        ASSERT_TRUE(key);
        ASSERT_TRUE(certificate);
        ASSERT_EQ(X509_set_version(certificate.get(), 2), 1);
        ASSERT_TRUE(ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1));
        ASSERT_TRUE(X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60));
        ASSERT_TRUE(X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 3600));
        ASSERT_EQ(X509_set_pubkey(certificate.get(), key.get()), 1);

        auto* subject = X509_get_subject_name(certificate.get());
        ASSERT_EQ(X509_NAME_add_entry_by_txt(subject,
                      "CN",
                      MBSTRING_ASC,
                      reinterpret_cast<const unsigned char*>("localhost"),
                      -1,
                      -1,
                      0),
            1);
        ASSERT_EQ(X509_set_issuer_name(certificate.get(), subject), 1);
        ASSERT_GT(X509_sign(certificate.get(), key.get(), EVP_sha256()), 0);
        ASSERT_EQ(SSL_CTX_use_certificate(serverContext.native_handle(), certificate.get()), 1);
        ASSERT_EQ(SSL_CTX_use_PrivateKey(serverContext.native_handle(), key.get()), 1);

        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        asio::ssl::stream<Tcp::socket> stream(context, serverContext);
        acceptor.async_accept(stream.next_layer(),
            [&](boost::system::error_code error)
        {
            ASSERT_FALSE(error);
            stream.async_handshake(asio::ssl::stream_base::server, [](boost::system::error_code) {});
        });

        AVEVA::HttpClientOptions clientOptions;
        clientOptions.SetTlsVersion(AVEVA::TlsVersion::Tls12);
        auto client = AVEVA::IHttpClient::Create(context, clientOptions);
        AVEVA::HttpRequest request;
        request.SetUrl("https://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
        int completions = 0;
        client->SendAsync(std::move(request),
            AVEVA::HttpRequestOptions{},
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            ++completions;
            EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
            EXPECT_EQ(response.GetStatus(), 0u);
            boost::system::error_code ignored;
            stream.next_layer().close(ignored);
        });
        context.run();
        EXPECT_EQ(completions, 1);
    }

    // Serves one TLS handshake with a self-signed certificate that the client explicitly trusts through
    // SetCaFile, and returns the error the client reports.
    std::error_code ConnectTrustingCertificateWithAltName(const std::string& subjectAltName,
        bool trustThroughPem = false)
    {
        asio::io_context context;
        asio::ssl::context serverContext(asio::ssl::context::tls_server);
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"),
            EVP_PKEY_free);
        std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), X509_free);
        EXPECT_TRUE(key);
        EXPECT_TRUE(certificate);
        X509_set_version(certificate.get(), 2);
        ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1);
        X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60);
        X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 3600);
        X509_set_pubkey(certificate.get(), key.get());
        auto* subject = X509_get_subject_name(certificate.get());
        X509_NAME_add_entry_by_txt(subject,
            "CN",
            MBSTRING_ASC,
            reinterpret_cast<const unsigned char*>("trusted.example"),
            -1,
            -1,
            0);
        X509_set_issuer_name(certificate.get(), subject);

        X509V3_CTX extensionContext;
        X509V3_set_ctx_nodb(&extensionContext);
        X509V3_set_ctx(&extensionContext, certificate.get(), certificate.get(), nullptr, nullptr, 0);
        std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> altName(
            X509V3_EXT_conf_nid(nullptr, &extensionContext, NID_subject_alt_name, subjectAltName.c_str()),
            X509_EXTENSION_free);
        EXPECT_TRUE(altName);
        X509_add_ext(certificate.get(), altName.get(), -1);
        EXPECT_GT(X509_sign(certificate.get(), key.get(), EVP_sha256()), 0);
        SSL_CTX_use_certificate(serverContext.native_handle(), certificate.get());
        SSL_CTX_use_PrivateKey(serverContext.native_handle(), key.get());

        const auto caFile = std::filesystem::temp_directory_path() /
                            ("aveva-http-client-test-ca-" + std::to_string(std::random_device{}()) + ".pem");
        {
            std::unique_ptr<BIO, decltype(&BIO_free)> file(BIO_new_file(caFile.string().c_str(), "wb"), BIO_free);
            EXPECT_TRUE(file);
            PEM_write_bio_X509(file.get(), certificate.get());
        }

        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        asio::ssl::stream<Tcp::socket> stream(context, serverContext);
        acceptor.async_accept(stream.next_layer(),
            [&](boost::system::error_code error)
        {
            if (!error)
            {
                stream.async_handshake(asio::ssl::stream_base::server,
                    [&](boost::system::error_code)
                {
                    boost::system::error_code ignored;
                    stream.next_layer().close(ignored);
                });
            }
        });

        AVEVA::HttpClientOptions clientOptions;
        clientOptions.SetTlsVersion(AVEVA::TlsVersion::Tls12);
        if (trustThroughPem)
        {
            std::ifstream pemFile(caFile, std::ios::binary);
            clientOptions.SetCaPem(std::string(std::istreambuf_iterator<char>(pemFile), {}));
        }
        else
        {
            clientOptions.SetCaFile(caFile.string());
        }
        auto client = AVEVA::IHttpClient::Create(context, clientOptions);
        AVEVA::HttpRequest request;
        request.SetUrl("https://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
        std::error_code result;
        client->SendAsync(std::move(request),
            AVEVA::HttpRequestOptions{},
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            result = error;
            boost::system::error_code ignored;
            stream.next_layer().close(ignored);
        });
        context.run();
        std::filesystem::remove(caFile);
        return result;
    }

    TEST(HttpClientTls, TrustedCertificateForAnotherHostIsRejected)
    {
        EXPECT_EQ(ConnectTrustingCertificateWithAltName("DNS:other.example"),
            AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
    }

    TEST(HttpClientTls, CaPemSuppliedInMemoryIsTrusted)
    {
        EXPECT_NE(ConnectTrustingCertificateWithAltName("IP:127.0.0.1", true),
            AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
        EXPECT_EQ(ConnectTrustingCertificateWithAltName("DNS:other.example", true),
            AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
    }

    TEST(HttpClientTls, TrustedCertificateForTheRequestedAddressPassesVerification)
    {
        // The handshake succeeds; the server then hangs up, so the failure is no longer a TLS one.
        const auto error = ConnectTrustingCertificateWithAltName("IP:127.0.0.1");
        EXPECT_NE(error, AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
    }

    // A minimal HTTPS server with a self-signed certificate that answers every request with "200 ok".
    class TlsTestServer
    {
      public:
        enum class AfterResponse
        {
            KeepOpen,
            AbortConnection,
            AdvertiseClose,
            EofDelimitedAbort // No Content-Length; the TCP connection closes without a TLS close_notify.
        };

        TlsTestServer(asio::io_context& context, const std::string& subjectAltName, AfterResponse after)
            : m_context(context), m_sslContext(asio::ssl::context::tls_server),
              m_acceptor(context, {asio::ip::make_address("127.0.0.1"), 0}), m_after(after)
        {
            std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
                EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"),
                EVP_PKEY_free);
            std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), X509_free);
            X509_set_version(certificate.get(), 2);
            ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1);
            X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60);
            X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 3600);
            X509_set_pubkey(certificate.get(), key.get());
            auto* subject = X509_get_subject_name(certificate.get());
            X509_NAME_add_entry_by_txt(subject,
                "CN",
                MBSTRING_ASC,
                reinterpret_cast<const unsigned char*>("tls-test"),
                -1,
                -1,
                0);
            X509_set_issuer_name(certificate.get(), subject);
            X509V3_CTX extensionContext;
            X509V3_set_ctx_nodb(&extensionContext);
            X509V3_set_ctx(&extensionContext, certificate.get(), certificate.get(), nullptr, nullptr, 0);
            std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> altName(
                X509V3_EXT_conf_nid(nullptr, &extensionContext, NID_subject_alt_name, subjectAltName.c_str()),
                X509_EXTENSION_free);
            X509_add_ext(certificate.get(), altName.get(), -1);
            X509_sign(certificate.get(), key.get(), EVP_sha256());
            SSL_CTX_use_certificate(m_sslContext.native_handle(), certificate.get());
            SSL_CTX_use_PrivateKey(m_sslContext.native_handle(), key.get());
            SSL_CTX_set_session_id_context(m_sslContext.native_handle(),
                reinterpret_cast<const unsigned char*>("tls-test"),
                8);
            // Same as the SSL_CTX_set_tlsext_servername_callback macro, which uses a C-style cast.
            SSL_CTX_callback_ctrl(m_sslContext.native_handle(),
                SSL_CTRL_SET_TLSEXT_SERVERNAME_CB,
                reinterpret_cast<void (*)()>(&TlsTestServer::OnServerName));
            SSL_CTX_set_tlsext_servername_arg(m_sslContext.native_handle(), this);

            std::unique_ptr<BIO, decltype(&BIO_free)> memory(BIO_new(BIO_s_mem()), BIO_free);
            PEM_write_bio_X509(memory.get(), certificate.get());
            char* data = nullptr;
            const long length = BIO_get_mem_data(memory.get(), &data);
            m_certificatePem.assign(data, static_cast<std::size_t>(length));
            Accept();
        }

        [[nodiscard]] unsigned short Port() const
        {
            return m_acceptor.local_endpoint().port();
        }

        [[nodiscard]] const std::string& CertificatePem() const
        {
            return m_certificatePem;
        }

        [[nodiscard]] int Handshakes() const
        {
            return m_handshakes;
        }

        [[nodiscard]] int Resumed() const
        {
            return m_resumed;
        }

        [[nodiscard]] int Requests() const
        {
            return m_requests;
        }

        [[nodiscard]] const std::string& ServerName() const
        {
            return m_serverName;
        }

        [[nodiscard]] const std::vector<boost::system::error_code>& CloseObservations() const
        {
            return m_closeObservations;
        }

        void SetOnClose(std::function<void()> onClose)
        {
            m_onClose = std::move(onClose);
        }

        void Stop()
        {
            boost::system::error_code ignored;
            m_acceptor.close(ignored);
            for (const auto& session : m_sessions)
            {
                session->Stream.next_layer().close(ignored);
            }
        }

      private:
        struct Session
        {
            Session(asio::io_context& context, asio::ssl::context& sslContext) : Stream(context, sslContext)
            {
            }

            asio::ssl::stream<Tcp::socket> Stream;
            asio::streambuf Buffer;
        };

        static int OnServerName(SSL* ssl, int*, void* argument)
        {
            if (const char* name = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name); name != nullptr)
            {
                static_cast<TlsTestServer*>(argument)->m_serverName = name;
            }
            return SSL_TLSEXT_ERR_OK;
        }

        void Accept()
        {
            auto session = std::make_shared<Session>(m_context, m_sslContext);
            m_sessions.push_back(session);
            m_acceptor.async_accept(session->Stream.next_layer(),
                [this, session](boost::system::error_code error)
            {
                if (error)
                {
                    return;
                }
                Accept();
                session->Stream.async_handshake(asio::ssl::stream_base::server,
                    [this, session](boost::system::error_code handshakeError)
                {
                    if (handshakeError)
                    {
                        return;
                    }
                    ++m_handshakes;
                    if (SSL_session_reused(session->Stream.native_handle()) != 0)
                    {
                        ++m_resumed;
                    }
                    m_negotiatedVersion = SSL_get_version(session->Stream.native_handle());
                    ReadRequest(session);
                });
            });
        }

        void ReadRequest(const std::shared_ptr<Session>& session)
        {
            asio::async_read_until(session->Stream,
                session->Buffer,
                "\r\n\r\n",
                [this, session](boost::system::error_code error, std::size_t bytes)
            {
                if (error)
                {
                    m_closeObservations.push_back(error);
                    if (m_onClose)
                    {
                        m_onClose();
                    }
                    return;
                }
                session->Buffer.consume(bytes);
                ++m_requests;
                auto response = std::make_shared<std::string>(
                    m_after == AfterResponse::EofDelimitedAbort
                        ? std::string{"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nok"}
                        : std::string{"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"} +
                              (m_after == AfterResponse::AdvertiseClose ? "Connection: close\r\n" : "") + "\r\nok");
                asio::async_write(session->Stream,
                    asio::buffer(*response),
                    [this, session, response](boost::system::error_code writeError, std::size_t)
                {
                    if (writeError)
                    {
                        return;
                    }
                    if (m_after == AfterResponse::AbortConnection || m_after == AfterResponse::EofDelimitedAbort)
                    {
                        boost::system::error_code ignored;
                        session->Stream.next_layer().close(ignored);
                        return;
                    }
                    ReadRequest(session);
                });
            });
        }

        asio::io_context& m_context;
        asio::ssl::context m_sslContext;
        Tcp::acceptor m_acceptor;
        AfterResponse m_after;
        std::string m_certificatePem;
        std::string m_serverName;
        std::string m_negotiatedVersion;
        std::function<void()> m_onClose;
        std::vector<std::shared_ptr<Session>> m_sessions;
        std::vector<boost::system::error_code> m_closeObservations;
        int m_handshakes = 0;
        int m_resumed = 0;
        int m_requests = 0;

      public:
        [[nodiscard]] const std::string& NegotiatedVersion() const
        {
            return m_negotiatedVersion;
        }
    };

    struct TlsOutcome
    {
        std::error_code Error;
        unsigned int Status = 0;
        std::string Body;
    };

    // Sends `count` sequential GETs to the server, using the same client so that connections can be pooled.
    // When `stopWhenDone` is false the caller must arrange for the server to be stopped (e.g. via SetOnClose).
    std::vector<TlsOutcome> SendSequentialRequests(asio::io_context& context,
        TlsTestServer& server,
        AVEVA::HttpClientOptions options,
        const std::string& host,
        int count,
        bool stopWhenDone = true)
    {
        options.SetCaPem(server.CertificatePem());
        std::shared_ptr<AVEVA::IHttpClient> client = AVEVA::IHttpClient::Create(context, options);
        const std::string url = "https://" + host + ":" + std::to_string(server.Port()) + "/";
        std::vector<TlsOutcome> outcomes;
        asio::steady_timer guard(context, std::chrono::seconds(20));
        guard.async_wait([&](boost::system::error_code error)
        {
            if (!error)
            {
                server.Stop();
            }
        });

        std::function<void()> sendNext;
        sendNext = [&]()
        {
            AVEVA::HttpRequest request;
            request.SetUrl(url);
            client->SendAsync(std::move(request),
                AVEVA::HttpRequestOptions{},
                [&](std::error_code error, AVEVA::HttpResponse response)
            {
                outcomes.push_back({error, response.GetStatus(), std::string{response.GetBody()}});
                if (error || static_cast<int>(outcomes.size()) >= count)
                {
                    guard.cancel();
                    if (stopWhenDone)
                    {
                        server.Stop();
                    }
                    return;
                }
                sendNext();
            });
        };
        sendNext();
        context.run();
        return outcomes;
    }

    TEST(HttpClientTls, HttpsRequestSucceedsEndToEnd)
    {
        asio::io_context context;
        TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::KeepOpen);
        const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 1);

        ASSERT_EQ(outcomes.size(), 1U);
        EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
        EXPECT_EQ(outcomes[0].Status, 200U);
        EXPECT_EQ(outcomes[0].Body, "ok");
        EXPECT_EQ(server.Handshakes(), 1);
        EXPECT_TRUE(server.ServerName().empty()) << "SNI must not be sent for IP literals";
    }

    TEST(HttpClientTls, PooledTlsConnectionIsReused)
    {
        asio::io_context context;
        TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::KeepOpen);
        const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 3);

        ASSERT_EQ(outcomes.size(), 3U);
        for (const auto& outcome : outcomes)
        {
            EXPECT_FALSE(outcome.Error) << outcome.Error.message();
            EXPECT_EQ(outcome.Status, 200U);
        }
        EXPECT_EQ(server.Requests(), 3);
        EXPECT_EQ(server.Handshakes(), 1);
    }

    TEST(HttpClientTls, StalePooledTlsConnectionIsReplaced)
    {
        asio::io_context context;
        TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::AbortConnection);
        const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 2);

        ASSERT_EQ(outcomes.size(), 2U);
        for (const auto& outcome : outcomes)
        {
            EXPECT_FALSE(outcome.Error) << outcome.Error.message();
            EXPECT_EQ(outcome.Status, 200U);
        }
        EXPECT_EQ(server.Handshakes(), 2);
    }

    TEST(HttpClientTls, EofDelimitedResponseWithoutCloseNotifyCompletes)
    {
        asio::io_context context;
        TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::EofDelimitedAbort);
        const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 1);

        ASSERT_EQ(outcomes.size(), 1U);
        EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
        EXPECT_EQ(outcomes[0].Status, 200U);
        EXPECT_EQ(outcomes[0].Body, "ok");
    }

    TEST(HttpClientTls, ServerNameIsSentForDnsHosts)
    {
        asio::io_context context;
        TlsTestServer server(context, "DNS:localhost", TlsTestServer::AfterResponse::KeepOpen);
        const auto outcomes = SendSequentialRequests(context, server, {}, "localhost", 1);

        ASSERT_EQ(outcomes.size(), 1U);
        EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
        EXPECT_EQ(server.ServerName(), "localhost");
    }

    TEST(HttpClientTls, Tls13CanBeRequired)
    {
        asio::io_context context;
        TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::KeepOpen);
        AVEVA::HttpClientOptions options;
        options.SetTlsVersion(AVEVA::TlsVersion::Tls13);
        const auto outcomes = SendSequentialRequests(context, server, options, "127.0.0.1", 1);

        ASSERT_EQ(outcomes.size(), 1U);
        EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
        EXPECT_EQ(server.NegotiatedVersion(), "TLSv1.3");
    }

    TEST(HttpClientTls, ReplacementConnectionResumesTheEarlierSession)
    {
        asio::io_context context;
        TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::AbortConnection);
        const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 3);

        ASSERT_EQ(outcomes.size(), 3U);
        EXPECT_EQ(server.Handshakes(), 3);
        EXPECT_EQ(server.Resumed(), 2) << "only the first connection needs a full handshake";
    }

    TEST(HttpClientTls, ConnectionCloseSendsTlsCloseNotify)
    {
        asio::io_context context;
        TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::AdvertiseClose);
        server.SetOnClose([&server]()
        {
            server.Stop();
        });
        const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 1, false);

        ASSERT_EQ(outcomes.size(), 1U);
        EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
        ASSERT_EQ(server.CloseObservations().size(), 1U);
        // A graceful TLS shutdown surfaces as a clean EOF; an abrupt TCP close would be stream_truncated.
        EXPECT_EQ(server.CloseObservations()[0], asio::error::eof) << server.CloseObservations()[0].message();
    }
} // namespace