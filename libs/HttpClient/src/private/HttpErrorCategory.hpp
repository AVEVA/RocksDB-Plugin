// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <system_error>

namespace AVEVA::Private
{
    /// <summary>
    /// The error_category backing <see cref="AVEVA::HttpClientError"/>.
    /// </summary>
    class HttpErrorCategory final : public std::error_category
    {
      public:
        /// <summary>
        /// Returns the category's name.
        /// </summary>
        /// <returns>The literal "aveva.http-client".</returns>
        const char* name() const noexcept override;

        /// <summary>
        /// Describes one of this category's error values.
        /// </summary>
        /// <param name="value">The error value, read as an <see cref="AVEVA::HttpClientError"/>.</param>
        /// <returns>A human-readable description, or a generic message if it is not one of the
        /// enumerators.</returns>
        std::string message(int value) const override;
    };
} // namespace AVEVA::Private
