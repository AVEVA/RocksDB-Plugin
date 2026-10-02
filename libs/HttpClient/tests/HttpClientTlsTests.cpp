#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl.hpp>

#include <gtest/gtest.h>

#include <openssl/evp.h>
#include <openssl/x509.h>

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
} // namespace