// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/TokenCredential.hpp>

#include <boost/asio/async_result.hpp>

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

    template <class Handler> void Begin(std::vector<std::string> scopes, Handler handler) {
        std::visit([&](auto& credential) { credential->GetTokenAsync(std::move(scopes), std::move(handler)); },
                   m_value);
    }

  public:
    template <class T>
        requires(std::same_as<T, TCreds> || ...)
    explicit CredentialVariant(std::shared_ptr<T> credential) : m_value(std::move(credential)) {}

    // The dynamic type of the held credential, for diagnostics and tests.
    [[nodiscard]] const std::type_info& Type() const noexcept {
        return std::visit([](const auto& credential) -> const std::type_info& { return typeid(*credential); }, m_value);
    }

    template <boost::asio::completion_token_for<TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<TokenSignature>(
            [this](auto handler, std::vector<std::string> requestedScopes) {
                Begin(std::move(requestedScopes), std::move(handler));
            },
            token, std::move(scopes));
    }
};
} // namespace AVEVA::AzureClient
