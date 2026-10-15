// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <openssl/evp.h>
#include <openssl/x509.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace HttpClientTests {
namespace http = boost::beast::http;

struct ExchangeResult {
    std::error_code error;
    AVEVA::HttpResponse response;
    http::request<http::string_body> received;
    std::string authority;
};

// URL of a loopback port that has no listener.
std::string ClosedLocalUrl();

ExchangeResult Exchange(AVEVA::HttpRequest request, std::string wireResponse, AVEVA::HttpRequestOptions options = {},
                        bool holdResponse = false);

// Deletes the file on destruction so a failing assertion cannot leak it.
class TempFile {
  public:
    explicit TempFile(std::string_view prefix);
    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;
    ~TempFile();

    const std::filesystem::path& Path() const { return m_path; }

  private:
    std::filesystem::path m_path;
};

// A self-signed EC certificate. Throws std::runtime_error when any OpenSSL call fails.
// An empty `subjectAltName` omits the extension; otherwise it uses OpenSSL's config syntax ("DNS:x", "IP:y").
struct SelfSignedCertificate {
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> Key{nullptr, EVP_PKEY_free};
    std::unique_ptr<X509, decltype(&X509_free)> Certificate{nullptr, X509_free};

    std::string Pem() const;
    void InstallOn(boost::asio::ssl::context& context) const;
    // Writes the certificate PEM to `file` so it can be handed to SetCaFile.
    void WriteTo(const TempFile& file) const;
};

SelfSignedCertificate MakeSelfSignedCertificate(const std::string& commonName, const std::string& subjectAltName);

struct ServerAction {
    std::string Wire;
    bool CloseAfterWrite = false;
    // When set, the response is withheld until the delay elapsed.
    std::chrono::milliseconds Delay{0};
    // When true, the request is read but never answered.
    bool Hold = false;
    // When true, the connection is closed right after the request was read, without any response.
    bool Drop = false;
};

// Accepts any number of connections and serves keep-alive requests on each; what to answer is scripted
// by `behavior(connectionIndex, request)`.
class ScriptedServer {
  public:
    using Behavior = std::function<ServerAction(int, const http::request<http::string_body>&)>;

    ScriptedServer(boost::asio::io_context& context, Behavior behavior);

    std::string Url(const std::string& path = "/") const;
    void Stop();
    int Accepted() const { return m_accepted; }
    int Requests(http::verb method) const;
    int TotalRequests() const { return static_cast<int>(m_methods.size()); }

  private:
    struct Connection;

    void Accept();
    void ReadNext(const std::shared_ptr<Connection>& connection);

    boost::asio::io_context& m_context;
    boost::asio::ip::tcp::acceptor m_acceptor;
    Behavior m_behavior;
    int m_accepted = 0;
    std::vector<std::shared_ptr<Connection>> m_connections;
    std::vector<http::verb> m_methods;
};
} // namespace HttpClientTests
