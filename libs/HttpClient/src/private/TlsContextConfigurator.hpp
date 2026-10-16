// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/ssl/context.hpp>

#include <openssl/ssl.h>

#include <string>

namespace AVEVA {
class HttpClientOptions;
} // namespace AVEVA

namespace AVEVA::Private {
/// <summary>
/// Applies the requested TLS version and peer-verification settings to a fresh SSL context, and makes it
/// remember the sessions servers issue so later connections to the same origin can resume them.
/// </summary>
/// <param name="tlsContext">The context to configure; must not have been used for a connection yet.</param>
/// <param name="options">The TLS version, verification mode, and trust roots to apply.</param>
/// <exception cref="std::runtime_error">The requested TLS version could not be configured.</exception>
/// <exception cref="boost::system::system_error">A configured CA file, directory or PEM could not be used
/// (a std::runtime_error as well); reported here so a bad trust setting fails client construction, not the first
/// request.</exception>
void ConfigureTlsContext(boost::asio::ssl::context& tlsContext, const HttpClientOptions& options);

/// <summary>
/// Called before a client handshake: tags the connection with its origin (so the session the server issues
/// is filed under it) and offers a remembered session for that origin, which skips the full handshake.
/// </summary>
/// <param name="ssl">The connection about to handshake.</param>
/// <param name="host">The origin's host.</param>
/// <param name="service">The origin's port or service name.</param>
void PrepareTlsSessionResumption(SSL* ssl, const std::string& host, const std::string& service);
} // namespace AVEVA::Private
