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
// A closed set of credential types behind one token-credential interface. Dispatch is a std::visit over the
// alternatives, so no virtual calls or type erasure are involved.
template <class... TCreds> class CredentialVariant {
    std::variant<std::shared_ptr<TCreds>...> m_value;

  public:
    template <class T>
        requires(std::same_as<T, TCreds> || ...)
    explicit CredentialVariant(std::shared_ptr<T> credential) : m_value(std::move(credential)) {}

    // The dynamic type of the held credential, for diagnostics and tests.
    [[nodiscard]] const std::type_info& Type() const noexcept {
        return std::visit([](const auto& credential) -> const std::type_info& { return typeid(*credential); }, m_value);
    }

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
