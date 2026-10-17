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

// Runs `context` until it has no more work. If it is still busy after `limit` (a server that never answers, a lost
// completion), the context is stopped and the current test fails with a gtest message instead of hanging until the
// ctest timeout kills the process.
void RunBounded(boost::asio::io_context& context, std::chrono::milliseconds limit = std::chrono::seconds(5));

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

    // `bindAddress` is the loopback address to listen on, "127.0.0.1" or "::1".
    ScriptedServer(boost::asio::io_context& context, Behavior behavior, const std::string& bindAddress = "127.0.0.1");

    std::string Url(const std::string& path = "/") const;
    // "host:port" as it appears in the Host header.
    std::string Authority() const;
    void Stop();
    int Accepted() const { return m_accepted; }
    int Requests(http::verb method) const;
    int TotalRequests() const { return static_cast<int>(m_requests.size()); }
    // The requests the server read, in arrival order. Valid once the io_context is no longer running.
    const std::vector<http::request<http::string_body>>& Received() const { return m_requests; }
    // Unexpected server-side errors (a request that failed to parse, a response that failed to write). A client
    // closing or aborting a connection is expected and not recorded. A test that must notice a misbehaving
    // exchange asserts this is empty, because ASSERT_* inside a server handler cannot fail the test.
    const std::vector<std::string>& Failures() const { return m_failures; }

  private:
    struct Connection;

    void Accept();
    void RecordFailure(const std::string& what, boost::system::error_code error);
    void ReadNext(const std::shared_ptr<Connection>& connection);

    boost::asio::io_context& m_context;
    boost::asio::ip::tcp::acceptor m_acceptor;
    Behavior m_behavior;
    int m_accepted = 0;
    std::vector<std::shared_ptr<Connection>> m_connections;
    std::string m_host;
    std::vector<http::request<http::string_body>> m_requests;
    std::vector<std::string> m_failures;
};
} // namespace HttpClientTests
