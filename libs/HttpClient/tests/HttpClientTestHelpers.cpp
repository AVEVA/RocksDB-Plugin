// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "HttpClientTestHelpers.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <gtest/gtest.h>

#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <fstream>
#include <memory>
#include <random>
#include <stdexcept>
#include <utility>

namespace HttpClientTests {
namespace asio = boost::asio;
namespace beast = boost::beast;

void RunBounded(asio::io_context& context, std::chrono::milliseconds limit) {
    context.run_for(limit);
    if (!context.stopped()) {
        context.stop();
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        ADD_FAILURE() << "io_context still had work after " << limit.count() << " ms in "
                      << (info ? info->test_suite_name() : "?") << "." << (info ? info->name() : "?");
    }
}
using Tcp = asio::ip::tcp;

TempFile::TempFile(std::string_view prefix)
    : m_path(std::filesystem::temp_directory_path() /
             (std::string(prefix) + std::to_string(std::random_device{}()) + ".pem")) {}

TempFile::~TempFile() {
    std::error_code ignored;
    std::filesystem::remove(m_path, ignored);
}

namespace {
void Check(bool ok, const char* what) {
    if (!ok) {
        throw std::runtime_error(std::string("OpenSSL call failed: ") + what);
    }
}
} // namespace

SelfSignedCertificate MakeSelfSignedCertificate(const std::string& commonName, const std::string& subjectAltName) {
    SelfSignedCertificate result;
    result.Key.reset(EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"));
    result.Certificate.reset(X509_new());
    Check(result.Key && result.Certificate, "allocation");
    X509* certificate = result.Certificate.get();
    Check(X509_set_version(certificate, 2) == 1, "X509_set_version");
    Check(ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1) == 1, "ASN1_INTEGER_set");
    Check(X509_gmtime_adj(X509_getm_notBefore(certificate), -60) != nullptr, "notBefore");
    Check(X509_gmtime_adj(X509_getm_notAfter(certificate), 3600) != nullptr, "notAfter");
    Check(X509_set_pubkey(certificate, result.Key.get()) == 1, "X509_set_pubkey");
    auto* subject = X509_get_subject_name(certificate);
    Check(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                     reinterpret_cast<const unsigned char*>(commonName.c_str()), -1, -1, 0) == 1,
          "X509_NAME_add_entry_by_txt");
    Check(X509_set_issuer_name(certificate, subject) == 1, "X509_set_issuer_name");

    if (!subjectAltName.empty()) {
        X509V3_CTX extensionContext;
        X509V3_set_ctx_nodb(&extensionContext);
        X509V3_set_ctx(&extensionContext, certificate, certificate, nullptr, nullptr, 0);
        std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> altName(
            X509V3_EXT_conf_nid(nullptr, &extensionContext, NID_subject_alt_name, subjectAltName.c_str()),
            X509_EXTENSION_free);
        Check(altName != nullptr, "X509V3_EXT_conf_nid");
        Check(X509_add_ext(certificate, altName.get(), -1) == 1, "X509_add_ext");
    }
    Check(X509_sign(certificate, result.Key.get(), EVP_sha256()) > 0, "X509_sign");
    return result;
}

std::string SelfSignedCertificate::Pem() const {
    std::unique_ptr<BIO, decltype(&BIO_free)> memory(BIO_new(BIO_s_mem()), BIO_free);
    Check(memory != nullptr, "BIO_new");
    Check(PEM_write_bio_X509(memory.get(), Certificate.get()) == 1, "PEM_write_bio_X509");
    char* data = nullptr;
    const long length = BIO_get_mem_data(memory.get(), &data);
    return std::string(data, static_cast<std::size_t>(length));
}

void SelfSignedCertificate::InstallOn(asio::ssl::context& context) const {
    Check(SSL_CTX_use_certificate(context.native_handle(), Certificate.get()) == 1, "SSL_CTX_use_certificate");
    Check(SSL_CTX_use_PrivateKey(context.native_handle(), Key.get()) == 1, "SSL_CTX_use_PrivateKey");
}

void SelfSignedCertificate::WriteTo(const TempFile& file) const {
    std::ofstream out(file.Path(), std::ios::binary);
    Check(out.is_open(), "open temp file");
    out << Pem();
    out.flush();
    Check(out.good(), "write temp file");
}

struct ScriptedServer::Connection {
    Connection(asio::io_context& context, int connectionIndex)
        : Socket(context), Timer(context), Index(connectionIndex) {}

    Tcp::socket Socket;
    asio::steady_timer Timer;
    beast::flat_buffer Buffer;
    http::request<http::string_body> Request;
    int Index;
};

ScriptedServer::ScriptedServer(asio::io_context& context, Behavior behavior, const std::string& bindAddress)
    : m_context(context), m_acceptor(context, {asio::ip::make_address(bindAddress), 0}),
      m_behavior(std::move(behavior)), m_host(bindAddress.contains(':') ? "[" + bindAddress + "]" : bindAddress) {
    Accept();
}

std::string ScriptedServer::Authority() const {
    return m_host + ":" + std::to_string(m_acceptor.local_endpoint().port());
}

std::string ScriptedServer::Url(const std::string& path) const { return "http://" + Authority() + path; }

void ScriptedServer::Stop() {
    boost::system::error_code ignored;
    m_acceptor.close(ignored);
    for (const auto& connection : m_connections) {
        connection->Socket.shutdown(Tcp::socket::shutdown_both, ignored);
        connection->Socket.close(ignored);
        connection->Timer.cancel();
    }
}

int ScriptedServer::Requests(http::verb method) const {
    int count = 0;
    for (const auto& request : m_requests) {
        count += request.method() == method ? 1 : 0;
    }
    return count;
}

void ScriptedServer::RecordFailure(const std::string& what, boost::system::error_code error) {
    m_failures.push_back(what + ": " + error.message());
}

void ScriptedServer::Accept() {
    auto connection = std::make_shared<Connection>(m_context, m_accepted);
    m_acceptor.async_accept(connection->Socket, [this, connection](boost::system::error_code error) {
        if (error) {
            return;
        }
        ++m_accepted;
        m_connections.push_back(connection);
        ReadNext(connection);
        Accept();
    });
}

void ScriptedServer::ReadNext(const std::shared_ptr<Connection>& connection) {
    connection->Request = {};
    http::async_read(
        connection->Socket, connection->Buffer, connection->Request,
        [this, connection](boost::system::error_code error, std::size_t) {
            if (error) {
                // A peer that closes between requests is normal for keep-alive; anything else is malformed input.
                if (error != http::error::end_of_stream && error != asio::error::operation_aborted &&
                    error != asio::error::connection_reset && error != asio::error::connection_aborted) {
                    RecordFailure("read", error);
                }
                return;
            }
            m_requests.push_back(connection->Request);
            auto action = std::make_shared<ServerAction>(m_behavior(connection->Index, connection->Request));
            if (action->Hold) {
                return;
            }
            if (action->Drop) {
                boost::system::error_code ignored;
                connection->Socket.shutdown(Tcp::socket::shutdown_both, ignored);
                connection->Socket.close(ignored);
                return;
            }
            connection->Timer.expires_after(action->Delay);
            connection->Timer.async_wait([this, connection, action](boost::system::error_code waitError) {
                if (waitError) {
                    return;
                }
                asio::async_write(connection->Socket, asio::buffer(action->Wire),
                                  [this, connection, action](boost::system::error_code writeError, std::size_t) {
                                      if (writeError) {
                                          if (writeError != asio::error::operation_aborted &&
                                              writeError != asio::error::connection_reset &&
                                              writeError != asio::error::connection_aborted &&
                                              writeError != asio::error::broken_pipe) {
                                              RecordFailure("write", writeError);
                                          }
                                          return;
                                      }
                                      if (action->CloseAfterWrite) {
                                          boost::system::error_code ignored;
                                          connection->Socket.shutdown(Tcp::socket::shutdown_both, ignored);
                                          connection->Socket.close(ignored);
                                          return;
                                      }
                                      ReadNext(connection);
                                  });
            });
        });
}

std::string ClosedLocalUrl() {
    // Binding then closing yields a port that nothing listens on right now, unlike a hard-coded port.
    asio::io_context context;
    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    const auto port = acceptor.local_endpoint().port();
    acceptor.close();
    return "http://127.0.0.1:" + std::to_string(port) + "/";
}

ExchangeResult Exchange(AVEVA::HttpRequest request, std::string wireResponse, AVEVA::HttpRequestOptions options,
                        bool holdResponse) {
    asio::io_context context;
    auto client = AVEVA::HttpClient::Create(context);
    // Most callers are not about timeouts, so a stuck exchange should fail in seconds rather than at the 30 s default.
    constexpr std::chrono::seconds ExchangeTimeout{2};
    if (options.GetTimeout() == AVEVA::HttpRequestOptions{}.GetTimeout()) {
        options.SetTimeout(ExchangeTimeout);
    }
    ScriptedServer server(context, [&](int, const http::request<http::string_body>&) {
        ServerAction action;
        action.Wire = std::move(wireResponse);
        action.CloseAfterWrite = true;
        action.Hold = holdResponse;
        return action;
    });
    ExchangeResult result;
    result.authority = server.Authority();
    request.SetUrl("http://" + result.authority + request.GetUrl());
    int completions = 0;
    client->SendAsync(std::move(request), options, [&](std::error_code error, AVEVA::HttpResponse response) {
        ++completions;
        result.error = error;
        result.response = std::move(response);
        server.Stop();
    });
    if (completions != 0) {
        throw std::runtime_error("Completion must not run inline");
    }
    RunBounded(context);
    if (completions != 1) {
        throw std::runtime_error("Completion must run exactly once");
    }
    if (!server.Failures().empty()) {
        throw std::runtime_error("The server saw an unexpected failure: " + server.Failures().front());
    }
    if (!server.Received().empty()) {
        result.received = server.Received().front();
    }
    return result;
}
} // namespace HttpClientTests