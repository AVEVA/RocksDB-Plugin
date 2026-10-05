#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl.hpp>

#include <gtest/gtest.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>

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
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
            EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"),
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
    std::error_code ConnectTrustingCertificateWithAltName(const std::string& subjectAltName)
    {
        asio::io_context context;
        asio::ssl::context serverContext(asio::ssl::context::tls_server);
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
            EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"),
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

        const auto caFile = std::filesystem::temp_directory_path() / "aveva-http-client-test-ca.pem";
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
        clientOptions.SetCaFile(caFile.string());
        auto client = AVEVA::IHttpClient::Create(context, clientOptions);
        AVEVA::HttpRequest request;
        request.SetUrl("https://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
        std::error_code result;
        client->SendAsync(std::move(request),
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

    TEST(HttpClientTls, TrustedCertificateForTheRequestedAddressPassesVerification)
    {
        // The handshake succeeds; the server then hangs up, so the failure is no longer a TLS one.
        const auto error = ConnectTrustingCertificateWithAltName("IP:127.0.0.1");
        EXPECT_NE(error, AVEVA::make_error_code(AVEVA::HttpClientError::TlsFailed));
    }
} // namespace