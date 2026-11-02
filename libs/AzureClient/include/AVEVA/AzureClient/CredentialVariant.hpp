// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/TokenCredential.hpp>

#include <boost/asio/awaitable.hpp>

#include <concepts>
#include <memory>
#include <string>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>

namespace AVEVA::AzureClient {
/// <summary>
/// A closed set of credential types behind one token-credential interface. Dispatch is a std::visit over the
/// alternatives, so no virtual calls or type erasure are involved.
/// </summary>
/// <typeparam name="TCreds">The credential types the variant may hold.</typeparam>
template <class... TCreds> class CredentialVariant {
    std::variant<std::shared_ptr<TCreds>...> m_value;

  public:
    /// <summary>
    /// Holds a credential of one of the listed types.
    /// </summary>
    /// <typeparam name="T">The credential type; must be one of <c>TCreds</c>.</typeparam>
    /// <param name="credential">The credential to share ownership of.</param>
    template <class T>
        requires(std::same_as<T, TCreds> || ...)
    explicit CredentialVariant(std::shared_ptr<T> credential) : m_value(std::move(credential)) {}

    /// <summary>
    /// The dynamic type of the held credential, for diagnostics and tests.
    /// </summary>
    /// <returns>The type information of the held credential.</returns>
    [[nodiscard]] const std::type_info& Type() const noexcept {
        return std::visit([](const auto& credential) -> const std::type_info& { return typeid(*credential); }, m_value);
    }

    /// <summary>
    /// Requests a token from the held credential.
    /// </summary>
    /// <param name="scopes">The scopes the token must cover.</param>
    /// <returns>The held credential's result.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        // Visiting inside the coroutine keeps the held credential (a shared_ptr copy) alive across suspension.
        co_return co_await std::visit(
            [&](const auto& credential) -> boost::asio::awaitable<TokenResult> {
                const auto held = credential;
                co_return co_await held->GetTokenAsync(std::move(scopes));
            },
            m_value);
    }
};
} // namespace AVEVA::AzureClient
