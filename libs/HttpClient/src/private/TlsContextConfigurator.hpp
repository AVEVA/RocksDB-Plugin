#pragma once

#include <boost/asio/ssl/context.hpp>

namespace AVEVA
{
    class HttpClientOptions;
} // namespace AVEVA

namespace AVEVA::Private
{
    // Applies the requested TLS version and peer-verification settings to a fresh SSL context.
    void ConfigureTlsContext(boost::asio::ssl::context& tlsContext, const HttpClientOptions& options);
} // namespace AVEVA::Private
