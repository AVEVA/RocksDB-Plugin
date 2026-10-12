// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

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

    /// <summary>
    /// The stream used for plain HTTP: a TCP socket with Beast's expiry support.
    /// </summary>
    using PlainStream = beast::tcp_stream;

    /// <summary>
    /// The stream used for HTTPS: TLS layered over <see cref="PlainStream"/>.
    /// </summary>
    using TlsStream = beast::ssl_stream<PlainStream>;
} // namespace AVEVA::Private
