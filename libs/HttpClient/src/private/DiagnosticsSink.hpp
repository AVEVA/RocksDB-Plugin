// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <utility>

namespace AVEVA::Private {
/// <summary>
/// The client's diagnostics receiver, shared by everything that can observe an event. Reporting never throws, because
/// it runs on paths that exist precisely because something already went wrong, and calls into the receiver are
/// serialized so it needs no locking of its own.
/// </summary>
class DiagnosticsSink {
  public:
    explicit DiagnosticsSink(HttpDiagnosticsCallback callback) noexcept : m_callback(std::move(callback)) {}

    /// <summary>
    /// Builds a sink for the options' callback, or returns null when none is set so callers can skip the work of
    /// describing an event nobody listens to.
    /// </summary>
    [[nodiscard]] static std::shared_ptr<const DiagnosticsSink> For(const HttpClientOptions& options) {
        auto callback = options.GetDiagnosticsCallback();
        if (!callback) {
            return nullptr;
        }
        return std::make_shared<const DiagnosticsSink>(std::move(*callback));
    }

    /// <summary>
    /// Reports through `sink` if there is one. The origin and message arrive as non-owning pieces and are copied only
    /// here, so a disabled sink never allocates.
    /// </summary>
    static void Report(const std::shared_ptr<const DiagnosticsSink>& sink, HttpDiagnosticKind kind,
                       HttpRequestPhase phase, std::string_view host, std::string_view service, std::error_code error,
                       std::string_view message) noexcept {
        if (!sink) {
            return;
        }
        const auto event = Describe(kind, phase, host, service, error, message);
        const std::scoped_lock lock(sink->m_mutex);
        sink->m_callback(event);
    }

  private:
    /// <summary>
    /// Builds the event. Failing to copy its text (out of memory) still reports the event, just without the
    /// text, since the receiver wants to know about it most when the process is under pressure.
    /// </summary>
    static HttpDiagnostic Describe(HttpDiagnosticKind kind, HttpRequestPhase phase, std::string_view host,
                                   std::string_view service, std::error_code error, std::string_view message) noexcept {
        try {
            std::string origin;
            if (!host.empty() || !service.empty()) {
                origin.reserve(host.size() + 1 + service.size());
                origin.append(host).append(1, ':').append(service);
            }
            return HttpDiagnostic{kind, phase, DiagnosticOrigin{std::move(origin)}, error,
                                  DiagnosticMessage{std::string(message)}};
        } catch (const std::exception&) {
            return HttpDiagnostic{kind, phase, DiagnosticOrigin{}, error, DiagnosticMessage{}};
        }
    }

    HttpDiagnosticsCallback m_callback;
    mutable std::mutex m_mutex;
};
} // namespace AVEVA::Private
