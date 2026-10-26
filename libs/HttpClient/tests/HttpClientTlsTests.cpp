// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"
#include "private/TlsSessionCache.hpp"

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

#include <array>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

namespace AllocationFailure {
extern thread_local int allocationsUntilFailure; // Defined in ConnectionPoolTests.cpp.
}
namespace {
namespace asio = boost::asio;
using HttpClientTests::RunBounded;
using Tcp = asio::ip::tcp;

// The raw acceptors below (and TlsTestServer) are a documented exception to using ScriptedServer: these tests drive
// the server side of the TLS handshake itself (failing it, or stopping after it), which a request/response script
// cannot express.

TEST(HttpClientTls, UntrustedCertificateReturnsTlsError) {
    asio::io_context context;
    asio::ssl::context serverContext(asio::ssl::context::tls_server);
    HttpClientTests::MakeSelfSignedCertificate("localhost", "").InstallOn(serverContext);

    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    asio::ssl::stream<Tcp::socket> stream(context, serverContext);
    acceptor.async_accept(stream.next_layer(), [&](boost::system::error_code error) {
        ASSERT_FALSE(error);
        stream.async_handshake(asio::ssl::stream_base::server, [](boost::system::error_code) {});
    });

    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetTlsVersion(AVEVA::TlsVersion::Tls12);
    auto client = AVEVA::HttpClient::Create(context, clientOptions);
    AVEVA::HttpRequest request;
    request.SetUrl("https://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
    int completions = 0;
    client->SendAsync(std::move(request), AVEVA::HttpRequestOptions{},
                      [&](std::error_code error, AVEVA::HttpResponse response) {
                          ++completions;
                          EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
                          EXPECT_EQ(response.GetStatus(), 0u);
                          EXPECT_TRUE(response.GetCause());
                          boost::system::error_code ignored;
                          stream.next_layer().close(ignored);
                      });
    RunBounded(context);
    EXPECT_EQ(completions, 1);
}

// Serves one TLS handshake with a self-signed certificate that the client explicitly trusts through
// SetCaFile, and returns the error the client reports.
std::error_code ConnectTrustingCertificateWithAltName(const std::string& subjectAltName, bool trustThroughPem = false) {
    asio::io_context context;
    asio::ssl::context serverContext(asio::ssl::context::tls_server);
    const auto certificate = HttpClientTests::MakeSelfSignedCertificate("trusted.example", subjectAltName);
    certificate.InstallOn(serverContext);
    const HttpClientTests::TempFile caFile("aveva-http-client-test-ca-");
    certificate.WriteTo(caFile);

    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    asio::ssl::stream<Tcp::socket> stream(context, serverContext);
    acceptor.async_accept(stream.next_layer(), [&](boost::system::error_code error) {
        if (!error) {
            stream.async_handshake(asio::ssl::stream_base::server, [&](boost::system::error_code) {
                boost::system::error_code ignored;
                stream.next_layer().close(ignored);
            });
        }
    });

    AVEVA::HttpClientOptions clientOptions;
    clientOptions.SetTlsVersion(AVEVA::TlsVersion::Tls12);
    if (trustThroughPem) {
        std::ifstream pemFile(caFile.Path(), std::ios::binary);
        clientOptions.SetCaPem(std::string(std::istreambuf_iterator<char>(pemFile), {}));
    } else {
        clientOptions.SetCaFile(caFile.Path().string());
    }
    auto client = AVEVA::HttpClient::Create(context, clientOptions);
    AVEVA::HttpRequest request;
    request.SetUrl("https://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
    std::error_code result;
    client->SendAsync(std::move(request), AVEVA::HttpRequestOptions{}, [&](std::error_code error, AVEVA::HttpResponse) {
        result = error;
        boost::system::error_code ignored;
        stream.next_layer().close(ignored);
    });
    RunBounded(context);
    return result;
}

TEST(HttpClientTls, TrustedCertificateForAnotherHostIsRejected) {
    EXPECT_EQ(ConnectTrustingCertificateWithAltName("DNS:other.example"),
              AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
}

TEST(HttpClientTls, CaPemSuppliedInMemoryIsTrusted) {
    EXPECT_NE(ConnectTrustingCertificateWithAltName("IP:127.0.0.1", true),
              AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
    EXPECT_EQ(ConnectTrustingCertificateWithAltName("DNS:other.example", true),
              AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
}

TEST(HttpClientTls, TrustedCertificateForTheRequestedAddressPassesVerification) {
    // The handshake succeeds; the server then hangs up, so the failure is no longer a TLS one.
    const auto error = ConnectTrustingCertificateWithAltName("IP:127.0.0.1");
    EXPECT_NE(error, AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
}

// A minimal HTTPS server with a self-signed certificate that answers every request with "200 ok".
class TlsTestServer {
  public:
    enum class AfterResponse {
        KeepOpen,
        AbortConnection,
        AdvertiseClose,
        EofDelimitedAbort, // No Content-Length; the TCP connection closes without a TLS close_notify.
        ShortBodyAbort,    // Content-Length promises more than is sent; closes without a TLS close_notify.
        StallReads         // Completes the handshake and then never reads, so a large upload stalls.
    };

    TlsTestServer(asio::io_context& context, const std::string& subjectAltName, AfterResponse after,
                  const char* bindAddress = "127.0.0.1")
        : m_context(context), m_sslContext(asio::ssl::context::tls_server),
          m_acceptor(context, {asio::ip::make_address(bindAddress), 0}), m_after(after) {
        const auto certificate = HttpClientTests::MakeSelfSignedCertificate("tls-test", subjectAltName);
        certificate.InstallOn(m_sslContext);
        SSL_CTX_set_session_id_context(m_sslContext.native_handle(), reinterpret_cast<const unsigned char*>("tls-test"),
                                       8);
        // Same as the SSL_CTX_set_tlsext_servername_callback macro, which uses a C-style cast.
        SSL_CTX_callback_ctrl(m_sslContext.native_handle(), SSL_CTRL_SET_TLSEXT_SERVERNAME_CB,
                              reinterpret_cast<void (*)()>(&TlsTestServer::OnServerName));
        SSL_CTX_set_tlsext_servername_arg(m_sslContext.native_handle(), this);

        m_certificatePem = certificate.Pem();
        Accept();
    }

    [[nodiscard]] unsigned short Port() const { return m_acceptor.local_endpoint().port(); }

    [[nodiscard]] const std::string& CertificatePem() const { return m_certificatePem; }

    [[nodiscard]] int Handshakes() const { return m_handshakes; }

    [[nodiscard]] int Resumed() const { return m_resumed; }

    [[nodiscard]] int Requests() const { return m_requests; }

    [[nodiscard]] const std::string& ServerName() const { return m_serverName; }

    [[nodiscard]] const std::vector<boost::system::error_code>& CloseObservations() const {
        return m_closeObservations;
    }

    void SetOnClose(std::function<void()> onClose) { m_onClose = std::move(onClose); }

    void Stop() {
        boost::system::error_code ignored;
        m_acceptor.close(ignored);
        for (const auto& session : m_sessions) {
            session->Stream.next_layer().close(ignored);
        }
    }

  private:
    struct Session {
        Session(asio::io_context& context, asio::ssl::context& sslContext) : Stream(context, sslContext) {}

        asio::ssl::stream<Tcp::socket> Stream;
        asio::streambuf Buffer;
    };

    static int OnServerName(SSL* ssl, int*, void* argument) {
        if (const char* name = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name); name != nullptr) {
            static_cast<TlsTestServer*>(argument)->m_serverName = name;
        }
        return SSL_TLSEXT_ERR_OK;
    }

    void Accept() {
        auto session = std::make_shared<Session>(m_context, m_sslContext);
        m_sessions.push_back(session);
        m_acceptor.async_accept(session->Stream.next_layer(), [this, session](boost::system::error_code error) {
            if (error) {
                return;
            }
            Accept();
            session->Stream.async_handshake(asio::ssl::stream_base::server,
                                            [this, session](boost::system::error_code handshakeError) {
                                                if (handshakeError) {
                                                    return;
                                                }
                                                ++m_handshakes;
                                                if (SSL_session_reused(session->Stream.native_handle()) != 0) {
                                                    ++m_resumed;
                                                }
                                                m_negotiatedVersion = SSL_get_version(session->Stream.native_handle());
                                                if (m_after != AfterResponse::StallReads) {
                                                    ReadRequest(session);
                                                }
                                            });
        });
    }

    void ReadRequest(const std::shared_ptr<Session>& session) {
        asio::async_read_until(
            session->Stream, session->Buffer, "\r\n\r\n",
            [this, session](boost::system::error_code error, std::size_t bytes) {
                if (error) {
                    m_closeObservations.push_back(error);
                    if (m_onClose) {
                        m_onClose();
                    }
                    return;
                }
                session->Buffer.consume(bytes);
                ++m_requests;
                auto response = std::make_shared<std::string>(
                    m_after == AfterResponse::ShortBodyAbort
                        ? std::string{"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nok"}
                    : m_after == AfterResponse::EofDelimitedAbort
                        ? std::string{"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nok"}
                        : std::string{"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"} +
                              (m_after == AfterResponse::AdvertiseClose ? "Connection: close\r\n" : "") + "\r\nok");
                asio::async_write(session->Stream, asio::buffer(*response),
                                  [this, session, response](boost::system::error_code writeError, std::size_t) {
                                      if (writeError) {
                                          return;
                                      }
                                      if (m_after == AfterResponse::AbortConnection ||
                                          m_after == AfterResponse::EofDelimitedAbort ||
                                          m_after == AfterResponse::ShortBodyAbort) {
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
    [[nodiscard]] const std::string& NegotiatedVersion() const { return m_negotiatedVersion; }
};

struct TlsOutcome {
    std::error_code Error;
    unsigned int Status = 0;
    std::string Body;
};

// Sends `count` sequential GETs to the server, using the same client so that connections can be pooled.
// When `stopWhenDone` is false the caller must arrange for the server to be stopped (e.g. via SetOnClose).
std::vector<TlsOutcome> SendSequentialRequests(asio::io_context& context, TlsTestServer& server,
                                               AVEVA::HttpClientOptions options, const std::string& host, int count,
                                               bool stopWhenDone = true,
                                               AVEVA::HttpRequestOptions requestOptions = {}) {
    options.SetCaPem(server.CertificatePem());
    std::shared_ptr<AVEVA::HttpClient> client = AVEVA::HttpClient::Create(context, options);
    const std::string url = "https://" + host + ":" + std::to_string(server.Port()) + "/";
    std::vector<TlsOutcome> outcomes;
    asio::steady_timer guard(
        context, std::chrono::seconds(8)); // Must stay below the ctest TIMEOUT of 30 s, leaving headroom to report.
    guard.async_wait([&](boost::system::error_code error) {
        if (!error) {
            server.Stop();
        }
    });

    std::function<void()> sendNext;
    sendNext = [&]() {
        AVEVA::HttpRequest request;
        request.SetUrl(url);
        client->SendAsync(std::move(request), requestOptions, [&](std::error_code error, AVEVA::HttpResponse response) {
            outcomes.push_back({error, response.GetStatus(), std::string{response.GetBody()}});
            if (error || static_cast<int>(outcomes.size()) >= count) {
                guard.cancel();
                if (stopWhenDone) {
                    server.Stop();
                }
                return;
            }
            sendNext();
        });
    };
    sendNext();
    RunBounded(context);
    return outcomes;
}

TEST(HttpClientTls, HttpsRequestSucceedsEndToEnd) {
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

TEST(HttpClientTls, PooledTlsConnectionIsReused) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::KeepOpen);
    const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 3);

    ASSERT_EQ(outcomes.size(), 3U);
    for (const auto& outcome : outcomes) {
        EXPECT_FALSE(outcome.Error) << outcome.Error.message();
        EXPECT_EQ(outcome.Status, 200U);
    }
    EXPECT_EQ(server.Requests(), 3);
    EXPECT_EQ(server.Handshakes(), 1);
}

TEST(HttpClientTls, StalePooledTlsConnectionIsReplaced) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::AbortConnection);
    const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 2);

    ASSERT_EQ(outcomes.size(), 2U);
    for (const auto& outcome : outcomes) {
        EXPECT_FALSE(outcome.Error) << outcome.Error.message();
        EXPECT_EQ(outcome.Status, 200U);
    }
    EXPECT_EQ(server.Handshakes(), 2);
}

TEST(HttpClientTls, EofDelimitedResponseWithoutCloseNotifyFailsByDefault) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::EofDelimitedAbort);
    const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 1);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_TRUE(outcomes[0].Error);
}

TEST(HttpClientTls, EofDelimitedResponseWithoutCloseNotifyCompletesWhenOptedIn) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::EofDelimitedAbort);
    AVEVA::HttpRequestOptions requestOptions;
    requestOptions.SetAcceptTruncatedTlsBody(true);
    const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 1, true, requestOptions);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
    EXPECT_EQ(outcomes[0].Status, 200U);
    EXPECT_EQ(outcomes[0].Body, "ok");
}

TEST(HttpClientTls, FixedLengthBodyTruncatedWithoutCloseNotifyFailsEvenWhenTruncationIsAccepted) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::ShortBodyAbort);
    AVEVA::HttpRequestOptions requestOptions;
    requestOptions.SetAcceptTruncatedTlsBody(true);
    const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 1, true, requestOptions);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_TRUE(outcomes[0].Error);
}

TEST(HttpClientTls, ServerNameIsSentForDnsHosts) {
    asio::io_context context;
    TlsTestServer server(context, "DNS:localhost", TlsTestServer::AfterResponse::KeepOpen);
    const auto outcomes = SendSequentialRequests(context, server, {}, "localhost", 1);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
    EXPECT_EQ(server.ServerName(), "localhost");
}

TEST(HttpClientTls, Ipv6LiteralHostIsVerifiedAgainstItsAddressAndSendsNoServerName) {
    asio::io_context context;
    std::unique_ptr<TlsTestServer> server;
    try {
        server = std::make_unique<TlsTestServer>(context, "IP:0:0:0:0:0:0:0:1", TlsTestServer::AfterResponse::KeepOpen,
                                                 "::1");
    } catch (const boost::system::system_error&) {
        GTEST_SKIP() << "IPv6 loopback is not available";
    }
    const auto outcomes = SendSequentialRequests(context, *server, {}, "[::1]", 1);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
    EXPECT_EQ(outcomes[0].Status, 200U);
    EXPECT_TRUE(server->ServerName().empty()) << "SNI must not be sent for IPv6 literals";
}

TEST(HttpClientTls, Tls13CanBeRequired) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::KeepOpen);
    AVEVA::HttpClientOptions options;
    options.SetTlsVersion(AVEVA::TlsVersion::Tls13);
    const auto outcomes = SendSequentialRequests(context, server, options, "127.0.0.1", 1);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
    EXPECT_EQ(server.NegotiatedVersion(), "TLSv1.3");
}

TEST(HttpClientTls, ReplacementConnectionResumesTheEarlierSession) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::AbortConnection);
    const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 3);

    ASSERT_EQ(outcomes.size(), 3U);
    EXPECT_EQ(server.Handshakes(), 3);
    EXPECT_EQ(server.Resumed(), 2) << "only the first connection needs a full handshake";
}

TEST(HttpClientTls, ConnectionCloseSendsTlsCloseNotify) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::AdvertiseClose);
    server.SetOnClose([&server]() { server.Stop(); });
    const auto outcomes = SendSequentialRequests(context, server, {}, "127.0.0.1", 1, false);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
    ASSERT_EQ(server.CloseObservations().size(), 1U);
    // A graceful TLS shutdown surfaces as a clean EOF; an abrupt TCP close would be stream_truncated.
    EXPECT_EQ(server.CloseObservations()[0], asio::error::eof) << server.CloseObservations()[0].message();
}

TEST(HttpClientTls, ConnectionThatIsNotPooledIsClosedWithCloseNotify) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::KeepOpen);
    server.SetOnClose([&server]() { server.Stop(); });
    AVEVA::HttpClientOptions options;
    options.SetMaxIdleConnectionsPerHost(0);
    const auto outcomes = SendSequentialRequests(context, server, options, "127.0.0.1", 1, false);

    ASSERT_EQ(outcomes.size(), 1U);
    EXPECT_FALSE(outcomes[0].Error) << outcomes[0].Error.message();
    ASSERT_EQ(server.CloseObservations().size(), 1U);
    // Dropping the stream without a TLS shutdown would be seen as stream_truncated instead of a clean EOF.
    EXPECT_EQ(server.CloseObservations()[0], asio::error::eof) << server.CloseObservations()[0].message();
}

TEST(HttpClientTls, RequestStateIsReleasedAtCompletionNotAfterTheTlsShutdownGracePeriod) {
    asio::io_context context;
    // The server never answers the client's close_notify, so the shutdown only ends at the grace timeout.
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::AdvertiseClose);
    AVEVA::HttpClientOptions options;
    options.SetCaPem(server.CertificatePem());
    auto client = AVEVA::HttpClient::Create(context, options);

    auto body = std::make_shared<std::array<std::byte, 4>>();
    std::weak_ptr<const void> weakBody = body;
    AVEVA::HttpRequest request;
    request.SetUrl("https://127.0.0.1:" + std::to_string(server.Port()) + "/");
    request.SetBodyView(std::span<const std::byte>(*body), body);
    body.reset();

    bool completed = false;
    bool releasedPromptly = false;
    asio::steady_timer check(context);
    client->SendAsync(std::move(request), AVEVA::HttpRequestOptions{}, [&](std::error_code error, AVEVA::HttpResponse) {
        EXPECT_FALSE(error) << error.message();
        completed = true;
        check.expires_after(std::chrono::milliseconds(200));
        check.async_wait([&](boost::system::error_code) {
            releasedPromptly = weakBody.expired();
            server.Stop();
        });
    });
    RunBounded(context);

    EXPECT_TRUE(completed);
    EXPECT_TRUE(releasedPromptly);
}

TEST(HttpClientTls, TimeoutOfAStalledRawViewUploadCompletesOnceAndLetsTheCallerFreeTheBody) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::StallReads);
    AVEVA::HttpClientOptions options;
    options.SetCaPem(server.CertificatePem());
    auto client = AVEVA::HttpClient::Create(context, options);

    auto body = std::make_unique<std::vector<std::byte>>(64U * 1024U * 1024U, std::byte{0x41});
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Put);
    request.SetUrl("https://127.0.0.1:" + std::to_string(server.Port()) + "/");
    request.SetBodyView(std::span<const std::byte>(*body));
    AVEVA::HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::milliseconds(300));

    int completions = 0;
    std::error_code observed;
    client->SendAsync(std::move(request), requestOptions, [&](std::error_code error, AVEVA::HttpResponse) {
        ++completions;
        observed = error;
        body.reset(); // A late transport access would now touch freed memory.
        server.Stop();
    });
    RunBounded(context);

    EXPECT_EQ(completions, 1);
    EXPECT_EQ(observed, AVEVA::make_error_code(AVEVA::HttpClientError::TimedOut));
}

TEST(HttpClientTls, CompletionRunsOnceWhenTheTlsShutdownSetupThrows) {
    asio::io_context context;
    TlsTestServer server(context, "IP:127.0.0.1", TlsTestServer::AfterResponse::KeepOpen);
    AVEVA::HttpClientOptions options;
    options.SetCaPem(server.CertificatePem());
    options.SetMaxIdleConnectionsPerHost(1);
    // The second connection is declined by the full pool; the diagnostic fires just before the shutdown setup,
    // so the next allocation (the shared stream holder) fails.
    options.SetDiagnosticsCallback([](const AVEVA::HttpDiagnostic& event) {
        if (event.Kind == AVEVA::HttpDiagnosticKind::ConnectionNotPooled) {
            AllocationFailure::allocationsUntilFailure = 0;
        }
    });
    auto client = AVEVA::HttpClient::Create(context, options);

    int completions = 0;
    int failures = 0;
    for (int i = 0; i < 2; ++i) {
        AVEVA::HttpRequest request;
        request.SetUrl("https://127.0.0.1:" + std::to_string(server.Port()) + "/");
        client->SendAsync(std::move(request), AVEVA::HttpRequestOptions{},
                          [&](std::error_code error, AVEVA::HttpResponse) {
                              ++completions;
                              failures += error ? 1 : 0;
                              if (completions == 2) {
                                  server.Stop();
                              }
                          });
    }
    RunBounded(context);
    AllocationFailure::allocationsUntilFailure = -1;

    EXPECT_EQ(completions, 2);
    EXPECT_EQ(failures, 0);
}

bool failIndexAllocation = false;

template <class T> struct FailingIndexAllocator : std::allocator<T> {
    template <class U> struct rebind {
        using other = FailingIndexAllocator<U>;
    };
    FailingIndexAllocator() = default;
    template <class U> explicit FailingIndexAllocator(const FailingIndexAllocator<U>& /*other*/) noexcept {}
    T* allocate(std::size_t count) {
        if (failIndexAllocation) {
            throw std::bad_alloc();
        }
        return std::allocator<T>::allocate(count);
    }
};

TEST(HttpClientTls, SessionCacheInsertionFailureLeavesNoUnindexedNodeAndKeepsTheBound) {
    using Cache = AVEVA::Private::BasicTlsSessionCache<FailingIndexAllocator>;
    Cache cache;
    const auto makeSession = [] { return AVEVA::Private::UniqueSession{SSL_SESSION_new()}; };

    cache.Store("a:1", makeSession());
    failIndexAllocation = true;
    for (int attempt = 0; attempt < 3; ++attempt) {
        EXPECT_THROW(cache.Store("b:1", makeSession()), std::bad_alloc);
    }
    failIndexAllocation = false;
    EXPECT_EQ(cache.ListSize(), 1U);
    EXPECT_EQ(cache.IndexSize(), 1U);

    cache.Store("b:1", makeSession());
    for (std::size_t i = 0; i < Cache::MaxEntries + 4; ++i) {
        cache.Store("o" + std::to_string(i), makeSession());
    }
    EXPECT_EQ(cache.ListSize(), Cache::MaxEntries);
    EXPECT_EQ(cache.IndexSize(), Cache::MaxEntries);
    EXPECT_EQ(cache.Take("b:1"), nullptr); // Evicted normally, not through an orphaned node.
    EXPECT_NE(cache.Take("o" + std::to_string(Cache::MaxEntries + 3)), nullptr);
}
} // namespace