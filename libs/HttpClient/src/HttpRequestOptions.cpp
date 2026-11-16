// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpRequestOptions.hpp"

namespace AVEVA {
Timeout HttpRequestOptions::GetTimeout() const noexcept { return m_timeout; }

ByteCount HttpRequestOptions::GetResponseBodyLimit() const noexcept { return m_responseBodyLimit; }

void HttpRequestOptions::SetTimeout(Timeout timeout) noexcept { m_timeout = timeout; }

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

const std::optional<RetryOptions>& HttpRequestOptions::GetRetryOptions() const noexcept { return m_retry; }

void HttpRequestOptions::SetRetryOptions(RetryOptions retry) noexcept { m_retry = retry; }

void HttpRequestOptions::ClearRetryOptions() noexcept { m_retry.reset(); }
} // namespace AVEVA
