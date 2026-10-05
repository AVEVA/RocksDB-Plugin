#include "TlsContextConfigurator.hpp"

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <openssl/ssl.h>

#include <stdexcept>

namespace AVEVA::Private
{
    void ConfigureTlsContext(boost::asio::ssl::context& tlsContext, const HttpClientOptions& options)
    {
        const auto nativeContext = tlsContext.native_handle();
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
