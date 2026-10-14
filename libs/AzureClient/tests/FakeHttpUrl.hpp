// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>

// Request-URL helpers and held-request bookkeeping shared by the test fakes (FakeHttpClient, FakeBlobServer and the
// loopback servers), so each does not carry its own copy.
namespace AVEVA::AzureClient::Tests {
// Decodes %XX escapes; a malformed or truncated escape is kept literally.
[[nodiscard]] inline std::string PercentDecode(std::string_view text) {
    std::string decoded;
    decoded.reserve(text.size());
    for (std::size_t i = 0; i < text.size(); ++i) {
        unsigned int value = 0;
        if (text[i] == '%' && i + 2 < text.size() &&
            std::from_chars(text.data() + i + 1, text.data() + i + 3, value, 16).ec == std::errc{}) {
            decoded += static_cast<char>(value);
            i += 2;
        } else {
            decoded += text[i];
        }
    }
    return decoded;
}

// The decoded path of a URL without its leading '/' and without the query ("container/blob").
[[nodiscard]] inline std::string UrlPath(const std::string& url) {
    const auto scheme = url.find("://");
    const auto start = url.find('/', scheme == std::string::npos ? 0 : scheme + 3);
    if (start == std::string::npos) {
        return {};
    }
    const auto end = url.find('?', start);
    return PercentDecode(std::string_view(url).substr(start + 1, end == std::string::npos ? end : end - start - 1));
}

// The decoded value of the first query parameter called `name`; empty if absent or valueless.
[[nodiscard]] inline std::string QueryValue(const std::string& url, std::string_view name) {
    const auto query = url.find('?');
    if (query == std::string::npos) {
        return {};
    }
    std::string_view rest = std::string_view(url).substr(query + 1);
    while (!rest.empty()) {
        const auto end = rest.find('&');
        const auto pair = rest.substr(0, end);
        const auto equals = pair.find('=');
        if (pair.substr(0, equals) == name) {
            return equals == std::string_view::npos ? std::string() : PercentDecode(pair.substr(equals + 1));
        }
        rest = end == std::string_view::npos ? std::string_view() : rest.substr(end + 1);
    }
    return {};
}

// Requests a fake holds until the test releases them, oldest first. An entry whose request has finished by other
// means (cancelled, timed out) stops counting as held. `Http` is anything with IsPending(requestIndex), and the
// caller serializes access (the fakes use their own mutex).
template <typename TMeta = std::monostate> class HeldRequests {
  public:
    struct Entry {
        std::size_t RequestIndex = 0;
        TMeta Meta{};
    };

    void Add(const std::size_t requestIndex, TMeta meta = {}) { m_entries.push_back({requestIndex, std::move(meta)}); }

    template <class Http> [[nodiscard]] std::size_t Count(const Http& http) {
        Prune(http);
        return m_entries.size();
    }

    // Removes and returns the entry at `position`, or nullopt if there is none.
    template <class Http> [[nodiscard]] std::optional<Entry> Take(const Http& http, const std::size_t position = 0) {
        Prune(http);
        if (position >= m_entries.size()) {
            return std::nullopt;
        }
        const auto it = m_entries.begin() + static_cast<std::ptrdiff_t>(position);
        Entry entry = std::move(*it);
        m_entries.erase(it);
        return entry;
    }

  private:
    template <class Http> void Prune(const Http& http) {
        std::erase_if(m_entries, [&](const Entry& entry) { return !http.IsPending(entry.RequestIndex); });
    }

    std::deque<Entry> m_entries;
};
} // namespace AVEVA::AzureClient::Tests
