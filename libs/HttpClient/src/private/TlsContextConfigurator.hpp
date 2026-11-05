// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"

#include <boost/asio/ssl/context.hpp>

#include <openssl/ssl.h>

#include <memory>

namespace AVEVA {
class HttpClientOptions;
} // namespace AVEVA

namespace AVEVA::Private {
/// <summary>
/// Builds the client's SSL context: the requested TLS version and peer-verification settings, and a cache of the
/// sessions servers issue so later connections to the same origin can resume them. Building the context here, rather
/// than configuring one handed in, guarantees it has never been used for a connection.
/// </summary>
/// <param name="options">The TLS version, verification mode, and trust roots to apply.</param>
/// <param name="diagnostics">Where a session that could not be remembered is reported; null reports nothing.</param>
/// <returns>The configured context.</returns>
/// <exception cref="std::runtime_error">The requested TLS version could not be configured.</exception>
/// <exception cref="boost::system::system_error">A configured CA file, directory or PEM could not be used
/// (a std::runtime_error as well); reported here so a bad trust setting fails client construction, not the first
/// request.</exception>
std::shared_ptr<boost::asio::ssl::context> MakeTlsContext(const HttpClientOptions& options,
                                                          std::shared_ptr<const DiagnosticsSink> diagnostics);

/// <summary>
/// Called before a client handshake: tags the connection with its origin (so the session the server issues
/// is filed under it) and offers a remembered session for that origin, which skips the full handshake.
/// </summary>
/// <param name="ssl">The connection about to handshake.</param>
/// <param name="origin">The origin the connection is for.</param>
void PrepareTlsSessionResumption(SSL* ssl, const TlsConnectionKey& origin);
} // namespace AVEVA::Private
