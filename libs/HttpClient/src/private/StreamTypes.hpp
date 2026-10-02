#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/url.hpp>

namespace AVEVA::Private
{
    namespace asio = boost::asio;
    namespace beast = boost::beast;
    namespace http = beast::http;
    namespace urls = boost::urls;
    using Tcp = asio::ip::tcp;
    using PlainStream = beast::tcp_stream;
    using TlsStream = beast::ssl_stream<PlainStream>;
} // namespace AVEVA::Private
