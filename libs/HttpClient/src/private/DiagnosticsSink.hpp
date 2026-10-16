// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <memory>
#include <string>
#include <utility>

namespace AVEVA::Private {
/// <summary>
/// The client's diagnostics receiver, shared by everything that can observe an event. Reporting is best effort: it
/// never throws, because it runs on paths that exist precisely because something already went wrong.
/// </summary>
class DiagnosticsSink {
  public:
    explicit DiagnosticsSink(HttpDiagnosticsCallback callback) : m_callback(std::move(callback)) {}

    /// <summary>
    /// Builds a sink for the options' callback, or returns null when none is set so callers can skip the work of
    /// describing an event nobody listens to.
    /// </summary>
    [[nodiscard]] static std::shared_ptr<const DiagnosticsSink> For(const HttpClientOptions& options) {
        if (!options.GetDiagnosticsCallback()) {
            return nullptr;
        }
        return std::make_shared<const DiagnosticsSink>(options.GetDiagnosticsCallback());
    }

    /// <summary>Reports through `sink` if there is one.</summary>
    static void Report(const std::shared_ptr<const DiagnosticsSink>& sink, HttpDiagnosticKind kind,
                       HttpRequestPhase phase, std::string origin, std::error_code error,
                       std::string message) noexcept {
        if (!sink) {
            return;
        }
        try {
            sink->m_callback(HttpDiagnostic{kind, phase, std::move(origin), error, std::move(message)});
        } catch (...) {
            // A throwing receiver must not disturb the request being reported on.
        }
    }

  private:
    HttpDiagnosticsCallback m_callback;
};
} // namespace AVEVA::Private
