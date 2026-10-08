// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/ssl/context.hpp>

#include <openssl/ssl.h>

#include <string>

namespace AVEVA
{
    class HttpClientOptions;
} // namespace AVEVA

namespace AVEVA::Private
{
    // Applies the requested TLS version and peer-verification settings to a fresh SSL context, and makes it
    // remember the sessions servers issue so later connections to the same origin can resume them.
    void ConfigureTlsContext(boost::asio::ssl::context& tlsContext, const HttpClientOptions& options);

    // Called before a client handshake: tags the connection with its origin (so the session the server issues is
    // filed under it) and offers a remembered session for that origin, which skips the full handshake.
    void PrepareTlsSessionResumption(SSL* ssl, const std::string& host, const std::string& service);
} // namespace AVEVA::Private
