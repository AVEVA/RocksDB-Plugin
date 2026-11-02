// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpRequestOptions.hpp"

namespace AVEVA {
std::chrono::milliseconds HttpRequestOptions::GetTimeout() const noexcept { return m_timeout; }

ByteCount HttpRequestOptions::GetResponseBodyLimit() const noexcept { return m_responseBodyLimit; }

void HttpRequestOptions::SetTimeout(std::chrono::milliseconds timeout) noexcept { m_timeout = timeout; }

void HttpRequestOptions::SetResponseBodyLimit(ByteCount responseBodyLimit) noexcept {
    m_responseBodyLimit = responseBodyLimit;
}

ByteCount HttpRequestOptions::GetResponseHeaderLimit() const noexcept { return m_responseHeaderLimit; }

void HttpRequestOptions::SetResponseHeaderLimit(ByteCount responseHeaderLimit) noexcept {
    m_responseHeaderLimit = responseHeaderLimit;
}

TruncatedTlsBodyPolicy HttpRequestOptions::GetTruncatedTlsBodyPolicy() const noexcept {
    return m_truncatedTlsBodyPolicy;
}

void HttpRequestOptions::SetTruncatedTlsBodyPolicy(TruncatedTlsBodyPolicy policy) noexcept {
    m_truncatedTlsBodyPolicy = policy;
}
} // namespace AVEVA
