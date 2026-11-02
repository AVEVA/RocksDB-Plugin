// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpBody.hpp"

#include <utility>

namespace AVEVA {
HttpBody::HttpBody() noexcept = default;

HttpBody::HttpBody(std::string content) {
    if (content.empty()) {
        return;
    }
    auto owner = std::make_shared<const std::string>(std::move(content));
    m_bytes = std::as_bytes(std::span{owner->data(), owner->size()});
    m_owner = std::move(owner);
}

HttpBody::HttpBody(std::span<const std::byte> content, gsl::not_null<std::shared_ptr<const void>> owner) noexcept
    : m_bytes(content), m_owner(std::move(owner).get()) {}

ByteCount HttpBody::Size() const noexcept { return ByteCount{m_bytes.size()}; }

std::span<const std::byte> HttpBody::Bytes() const& noexcept { return m_bytes; }
} // namespace AVEVA
