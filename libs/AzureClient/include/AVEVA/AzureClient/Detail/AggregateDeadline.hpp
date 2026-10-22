// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>

#include <chrono>
#include <optional>

namespace AVEVA::AzureClient::Private {
/// <summary>
/// The absolute end of an operation that issues several requests (chunked downloads, batched deletes). The
/// per-request total deadline is relative to each request's own start, so handing the caller's options to every child
/// would let each one spend the whole budget again; this hands every child only what is left.
/// </summary>
class AggregateDeadline {
  public:
    /// <param name="options">The caller's options; their total deadline, if any, is the budget.</param>
    /// <param name="start">When the budget began; defaults to now.</param>
    explicit AggregateDeadline(const AzureRequestOptions& options, std::chrono::steady_clock::time_point start =
                                                                       std::chrono::steady_clock::now()) noexcept {
        if (const auto budget = options.GetTotalDeadline()) {
            m_end = start + *budget;
        }
    }

    /// <summary>
    /// Narrows a child's total deadline to the time that remains.
    /// </summary>
    /// <param name="childOptions">The child's options, copied from the caller's.</param>
    /// <returns>False when the budget is spent and no further child may be admitted.</returns>
    [[nodiscard]] bool Apply(AzureRequestOptions& childOptions) const noexcept {
        if (!m_end) {
            return true;
        }
        const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*m_end - std::chrono::steady_clock::now());
        if (remaining <= std::chrono::milliseconds::zero()) {
            return false;
        }
        childOptions.SetTotalDeadline(remaining);
        return true;
    }

    /// <summary>
    /// Whether the budget is spent; used for work that cannot be interrupted, such as the last disk write, to decide
    /// whether an otherwise successful operation may still be reported as one.
    /// </summary>
    [[nodiscard]] bool Expired() const noexcept {
        return m_end.has_value() && std::chrono::steady_clock::now() >= *m_end;
    }

  private:
    std::optional<std::chrono::steady_clock::time_point> m_end;
};
} // namespace AVEVA::AzureClient::Private
