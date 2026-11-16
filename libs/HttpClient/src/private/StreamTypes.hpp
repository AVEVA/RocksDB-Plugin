// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/stream_traits.hpp>
#include <boost/beast/core/string.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/error.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/beast/ssl/ssl_stream.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/url_view.hpp>

namespace AVEVA::Private {
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
