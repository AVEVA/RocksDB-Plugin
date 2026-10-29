// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpRequestOptions.hpp"

namespace AVEVA {
std::chrono::milliseconds HttpRequestOptions::GetTimeout() const noexcept { return m_timeout; }

std::uint64_t HttpRequestOptions::GetResponseBodyLimit() const noexcept { return m_responseBodyLimit; }

void HttpRequestOptions::SetTimeout(std::chrono::milliseconds timeout) noexcept { m_timeout = timeout; }

void HttpRequestOptions::SetResponseBodyLimit(std::uint64_t responseBodyLimit) noexcept {
    m_responseBodyLimit = responseBodyLimit;
}

std::uint32_t HttpRequestOptions::GetResponseHeaderLimit() const noexcept { return m_responseHeaderLimit; }

void HttpRequestOptions::SetResponseHeaderLimit(std::uint32_t responseHeaderLimit) noexcept {
    m_responseHeaderLimit = responseHeaderLimit;
}

bool HttpRequestOptions::GetAcceptTruncatedTlsBody() const noexcept { return m_acceptTruncatedTlsBody; }

void HttpRequestOptions::SetAcceptTruncatedTlsBody(bool accept) noexcept { m_acceptTruncatedTlsBody = accept; }
} // namespace AVEVA
