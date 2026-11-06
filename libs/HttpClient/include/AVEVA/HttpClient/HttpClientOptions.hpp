// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Units.hpp>

#include <chrono>
#include <concepts>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <system_error>
#include <type_traits>
#include <utility>

namespace AVEVA {
/// <summary>
/// The TLS protocol versions a client may negotiate.
/// </summary>
enum class TlsVersion {
    /// <summary>TLS 1.2 as the floor, letting the server pick any newer version it supports.</summary>
    Tls12OrLater,
    /// <summary>TLS 1.2 exactly; both the minimum and maximum version are pinned.</summary>
    Tls12,
    /// <summary>TLS 1.3 exactly; both the minimum and maximum version are pinned.</summary>
    Tls13
};

/// <summary>
/// Whether the server certificate chain and host name are checked. <c>Skip</c> removes the protection TLS offers
/// against an impersonating peer, so it is only appropriate for tests.
/// </summary>
enum class PeerVerification : std::uint8_t { Verify, Skip };

/// <summary>
/// The stage a request was in, reported with a timeout so it can be told whether the wait was DNS, the TCP connect, the
/// TLS handshake, sending the request or reading the response.
/// </summary>
enum class HttpRequestPhase {
    /// <summary>Not yet on the network: queued behind the per-origin cap or still being prepared.</summary>
    Queued,
    Resolve,
    Connect,
    Handshake,
    Write,
    Read
};

/// <summary>
/// What a diagnostic event reports.
/// </summary>
enum class HttpDiagnosticKind {
    /// <summary>A reused connection turned out to be closed by the peer, and the request was retried on a new
    /// one.</summary>
    StaleConnectionRetry,
    /// <summary>A finished connection was closed instead of returned to the pool.</summary>
    ConnectionNotPooled,
    /// <summary>An exception was caught and deliberately not propagated; the message says where.</summary>
    SwallowedException,
    /// <summary>Obsolete: the client no longer caches DNS answers, so this is never reported.</summary>
    DnsInvalidated,
    /// <summary>A request timed out; Phase tells in which stage.</summary>
    Timeout
};

/// <summary>
/// One diagnostic event. The client calls <see cref="HttpClientOptions::SetDiagnosticsCallback"/>'s function with
/// it, on whichever thread observed the event.
/// </summary>
struct HttpDiagnostic {
    HttpDiagnosticKind Kind = HttpDiagnosticKind::SwallowedException;
    /// <summary>The stage the request was in; meaningful for Timeout and retries, Queued otherwise.</summary>
    HttpRequestPhase Phase = HttpRequestPhase::Queued;
    /// <summary>The "host:port" (no IPv6 brackets) the event concerns; empty when none applies.</summary>
    DiagnosticOrigin Origin;
    /// <summary>The underlying error, when there is one.</summary>
    std::error_code Error;
    /// <summary>A short human-readable description; empty if describing the event ran out of memory.</summary>
    DiagnosticMessage Message;
};

/// <summary>
/// Receives diagnostic events. Only a callable that cannot throw is accepted, and the client never runs two calls at
/// once, so the receiver needs no locking of its own. It runs on whichever thread observed the event, so it should
/// be cheap and must not call back into the client.
/// </summary>
class HttpDiagnosticsCallback {
  public:
    /// <summary>
    /// Wraps a receiver.
    /// </summary>
    /// <param name="receiver">A callable taking <c>const HttpDiagnostic&amp;</c> that is declared not to throw.</param>
    template <typename Receiver>
        requires(!std::same_as<std::remove_cvref_t<Receiver>, HttpDiagnosticsCallback> &&
                 std::is_nothrow_invocable_v<std::remove_cvref_t<Receiver>&, const HttpDiagnostic&>)
    explicit HttpDiagnosticsCallback(Receiver&& receiver) : m_receiver(std::forward<Receiver>(receiver)) {}

    /// <summary>
    /// Delivers one event to the receiver.
    /// </summary>
    /// <param name="event">The event.</param>
    void operator()(const HttpDiagnostic& event) const noexcept { m_receiver(event); }

  private:
    std::function<void(const HttpDiagnostic&)> m_receiver;
};

/// <summary>
/// Client-wide settings fixed when the client is created: how TLS is negotiated and verified, how idle
/// keep-alive connections are pooled, and how many requests may be in flight per origin. Per-request
/// settings live in <see cref="HttpRequestOptions"/> instead.
/// </summary>
class HttpClientOptions {
  public:
    /// <summary>
    /// Constructs the defaults: <c>Tls12OrLater</c>, peer verification on, no explicit trust roots, 6 idle
    /// connections per host, a 30 second idle timeout, and no per-origin cap.
    /// </summary>
    HttpClientOptions() = default;

    /// <summary>
    /// The TLS version policy applied to HTTPS connections.
    /// </summary>
    /// <returns>The configured policy.</returns>
    [[nodiscard]] TlsVersion GetTlsVersion() const noexcept;

    /// <summary>
    /// Whether the server certificate chain and host name are verified.
    /// </summary>
    /// <returns>The configured policy; <c>Verify</c> unless set otherwise.</returns>
    [[nodiscard]] PeerVerification GetPeerVerification() const noexcept;

    /// <summary>
    /// Path to a PEM file of trusted CA certificates.
    /// </summary>
    /// <returns>The path, empty when none was set.</returns>
    [[nodiscard]] std::filesystem::path GetCaFile() const;

    /// <summary>
    /// Path to an OpenSSL-style hashed directory of trusted CA certificates.
    /// </summary>
    /// <returns>The path, empty when none was set.</returns>
    [[nodiscard]] std::filesystem::path GetCaDirectory() const;

    /// <summary>
    /// Trusted CA certificates held in memory as PEM text.
    /// </summary>
    /// <returns>The PEM text, empty when none was set.</returns>
    [[nodiscard]] PemText GetCaPem() const;

    /// <summary>
    /// How many idle keep-alive connections may be kept per origin.
    /// </summary>
    /// <returns>The per-origin idle cap; 6 unless set otherwise.</returns>
    [[nodiscard]] ConnectionCount GetMaxIdleConnectionsPerHost() const noexcept;

    /// <summary>
    /// How long a pooled connection may sit idle before it is closed.
    /// </summary>
    /// <returns>The idle timeout; 30 seconds unless set otherwise.</returns>
    [[nodiscard]] Timeout GetIdleConnectionTimeout() const noexcept;

    /// <summary>
    /// How long a pooled connection must have sat idle before it is checked for a peer-side close when it is
    /// reused.
    /// </summary>
    /// <returns>The threshold; 50 milliseconds unless set otherwise.</returns>
    [[nodiscard]] std::chrono::milliseconds GetIdleProbeThreshold() const noexcept;

    /// <summary>
    /// The cap on concurrent in-flight requests per origin.
    /// </summary>
    /// <returns>The cap; 0 means unlimited.</returns>
    [[nodiscard]] ConnectionCount GetMaxConnectionsPerHost() const noexcept;

    /// <summary>
    /// Sets the TLS version policy.
    /// </summary>
    /// <param name="version">The policy to apply to HTTPS connections.</param>
    void SetTlsVersion(TlsVersion version) noexcept;

    /// <summary>
    /// Enables or disables verification of the server certificate chain and host name. Disabling it removes
    /// the protection TLS offers against an impersonating peer, so it is only appropriate for tests.
    /// </summary>
    /// <param name="peerVerification">Whether to verify the peer.</param>
    void SetPeerVerification(PeerVerification peerVerification) noexcept;

    /// <summary>
    /// Trusted CA certificates from a PEM file.
    /// </summary>
    /// <param name="caFile">Path to a PEM file; empty to set none.</param>
    void SetCaFile(std::filesystem::path caFile);

    /// <summary>
    /// Trusted CA certificates from an OpenSSL-style hashed directory, used in addition to
    /// <see cref="SetCaFile"/> and <see cref="SetCaPem"/>.
    /// </summary>
    /// <param name="caDirectory">Path to a hashed certificate directory; empty to set none.</param>
    void SetCaDirectory(std::filesystem::path caDirectory);

    /// <summary>
    /// Trusted CA certificates as concatenated PEM text, used in addition to <see cref="SetCaFile"/> and
    /// <see cref="SetCaDirectory"/>.
    /// On Windows the default OpenSSL verify paths usually find no CA bundle, so supply trust roots here (or via
    /// <see cref="SetCaFile"/>) when connecting to servers that are not covered by another trust source.
    /// </summary>
    /// <param name="caPem">Concatenated PEM certificates; empty to set none.</param>
    void SetCaPem(PemText caPem);

    /// <summary>
    /// Caps how many idle keep-alive connections are kept per origin.
    /// </summary>
    /// <param name="maxIdleConnectionsPerHost">The per-origin idle cap; 0 disables pooling entirely.
    /// Independently of this, at most 256 idle connections are kept across all origins.</param>
    void SetMaxIdleConnectionsPerHost(ConnectionCount maxIdleConnectionsPerHost) noexcept;

    /// <summary>
    /// How long a pooled connection may stay idle before it is closed. Expiry is checked by a periodic sweep, so
    /// an idle socket can linger for up to about twice this timeout.
    /// </summary>
    /// <param name="idleConnectionTimeout">The idle timeout.</param>
    void SetIdleConnectionTimeout(Timeout idleConnectionTimeout) noexcept;

    /// <summary>
    /// Connections idle for less than this are reused without a liveness check (one cheap system call), because
    /// the peer has had no real chance to close them; a request that still hits a closed connection is retried
    /// where that is safe. Zero checks every reused connection.
    /// </summary>
    /// <param name="idleProbeThreshold">The threshold.</param>
    /// <exception cref="std::invalid_argument">Thrown when the threshold is negative.</exception>
    void SetIdleProbeThreshold(std::chrono::milliseconds idleProbeThreshold);

    /// <summary>
    /// Caps in-flight requests per origin (scheme, host, port); further requests wait in FIFO order until one
    /// finishes, and a waiting request still honors its cancellation slot. The request timeout counts from
    /// submission,     so queue time is part of it: a waiting request is woken at its own deadline and completes
    /// with <c>HttpClientError::TimedOut</c> without connecting.
    /// </summary>
    /// <param name="maxConnectionsPerHost">The per-origin cap; 0 (the default) means unlimited.</param>
    void SetMaxConnectionsPerHost(ConnectionCount maxConnectionsPerHost) noexcept;

    /// <summary>
    /// Reports events the client otherwise handles silently: stale-connection retries, connections not returned
    /// to the pool, swallowed exceptions, DNS cache invalidations and the stage a timeout happened in.
    /// </summary>
    /// <param name="callback">The receiver. Without one (the default) nothing is reported.</param>
    void SetDiagnosticsCallback(HttpDiagnosticsCallback callback);

    /// <summary>
    /// The diagnostics receiver.
    /// </summary>
    /// <returns>A copy of the callback; none when none was set.</returns>
    [[nodiscard]] std::optional<HttpDiagnosticsCallback> GetDiagnosticsCallback() const;

  private:
    TlsVersion m_tlsVersion = TlsVersion::Tls12OrLater;
    PeerVerification m_peerVerification = PeerVerification::Verify;
    std::filesystem::path m_caFile;
    std::filesystem::path m_caDirectory;
    PemText m_caPem;
    ConnectionCount m_maxIdleConnectionsPerHost{6};
    Timeout m_idleConnectionTimeout{std::chrono::seconds{30}};
    std::chrono::milliseconds m_idleProbeThreshold{50};
    ConnectionCount m_maxConnectionsPerHost{};
    std::optional<HttpDiagnosticsCallback> m_diagnosticsCallback;
};
} // namespace AVEVA
