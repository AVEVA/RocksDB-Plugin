// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/scheme.hpp>
#include <boost/url/url_view.hpp>
#include <openssl/crypto.h>

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <fstream>
#include <ios>
#include <iterator>
#include <stdexcept>

namespace AVEVA::AzureClient {
namespace Private {
std::string GetEnvironmentValue(gsl::not_null<gsl::czstring> name) {
#ifdef _WIN32
    gsl::owner<char*> buffer = nullptr;
    std::size_t size = 0;
    if (_dupenv_s(&buffer, &size, name.get()) != 0 || buffer == nullptr) {
        return {};
    }
    // `size` includes the terminating NUL; wipe the secret before the CRT buffer is released.
    const auto wipe = [size](char* secret) {
        OPENSSL_cleanse(secret, size);
        std::free(secret);
    };
    const std::unique_ptr<char, decltype(wipe)> owner{buffer, wipe};
    return std::string{buffer};
#else
    const char* value = std::getenv(name.get());
    return value == nullptr ? std::string{} : std::string{value};
#endif
}

std::optional<std::int64_t> ParseInteger(std::string_view text) noexcept {
    std::int64_t value = 0;
    const auto* const end = std::to_address(text.end());
    // Bounded by `end`, so null termination is not required.
    const auto [ptr, ec] = std::from_chars(std::to_address(text.begin()), end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

bool IsAcceptableIdentityEndpoint(std::string_view endpoint) noexcept {
    // Parse rather than prefix-match: prefixes accept hosts such as "127.evil.com" and userinfo tricks
    // such as "127.0.0.1@evil.example".
    const auto parsed = boost::urls::parse_uri(endpoint);
    if (!parsed) {
        return false;
    }
    const boost::urls::url_view url = *parsed;
    if (url.has_userinfo()) {
        return false;
    }
    if (url.scheme_id() == boost::urls::scheme::https) {
        return true;
    }
    if (url.scheme_id() != boost::urls::scheme::http) {
        return false;
    }
    if (IsLoopbackHost(url)) {
        return true;
    }
    // Link-local, for the IMDS endpoint.
    if (url.host_type() == boost::urls::host_type::ipv4) {
        const auto bytes = url.host_ipv4_address().to_bytes();
        return bytes[0] == 169 && bytes[1] == 254;
    }
    return false;
}

namespace {
std::string JsonText(const boost::json::object& object, std::string_view key) {
    const auto it = object.find(key);
    if (it == object.end()) {
        return {};
    }
    if (it->value().is_string()) {
        return std::string{it->value().as_string()};
    }
    if (it->value().is_number()) {
        return boost::json::serialize(it->value());
    }
    return {};
}
} // namespace

std::expected<AccessToken, std::error_code> ParseTokenResponse(std::error_code error, const HttpResponse& response) {
    if (error) {
        return std::unexpected(error);
    }
    constexpr unsigned int MinSuccess = 200;
    constexpr unsigned int MaxSuccess = 299;
    if (response.GetStatus() < MinSuccess || response.GetStatus() > MaxSuccess) {
        std::string oauthError;
        boost::system::error_code ignored;
        if (const auto body = boost::json::parse(std::string_view{response.GetBody()}, ignored);
            !ignored && body.is_object()) {
            oauthError = JsonText(body.as_object(), "error");
        }
        return std::unexpected(MakeTokenEndpointError(response.GetStatus(), oauthError));
    }
    const auto invalid = std::unexpected(make_error_code(BlobStorageErrorCode::InvalidResponse));
    boost::system::error_code parseError;
    const boost::json::value root = boost::json::parse(std::string_view{response.GetBody()}, parseError);
    if (parseError || !root.is_object()) {
        return invalid;
    }
    const boost::json::object& tree = root.as_object();
    AccessToken token;
    token.Token = JsonText(tree, "access_token");
    if (token.Token.empty()) {
        return invalid;
    }
    if (const auto expiresIn = ParseInteger(JsonText(tree, "expires_in"));
        expiresIn.has_value() && *expiresIn >= MinTokenLifetimeSeconds) {
        // Tokens living longer than the cap are accepted but refreshed at the cap.
        const auto lifetime = std::min(*expiresIn, MaxTokenLifetimeSeconds);
        token.ExpiresOn = std::chrono::time_point_cast<Clock::duration>(Clock::now() + std::chrono::seconds{lifetime});
    } else if (const auto expiresOn = ParseInteger(JsonText(tree, "expires_on"));
               expiresOn.has_value() && *expiresOn >= 0) {
        const auto now = Clock::now();
        const std::int64_t nowSeconds =
            std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        const std::int64_t lifetime = *expiresOn - nowSeconds;
        if (lifetime < MinTokenLifetimeSeconds) {
            return invalid;
        }
        token.ExpiresOn = std::chrono::time_point_cast<Clock::duration>(
            now + std::chrono::seconds{std::min(lifetime, MaxTokenLifetimeSeconds)});
    } else {
        return invalid;
    }
    return token;
}

bool IsValidTenantId(std::string_view tenantId) noexcept {
    return !tenantId.empty() && std::ranges::all_of(tenantId, [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.';
    });
}

std::string NormalizeAuthorityHost(std::string_view authorityHost) {
    const std::string_view host = TrimTrailingSlashes(authorityHost);
    const auto parsed = boost::urls::parse_uri(host);
    if (!parsed || parsed->scheme_id() != boost::urls::scheme::https || !parsed->has_authority() ||
        parsed->host().empty()) {
        throw std::invalid_argument("AuthorityHost must be an https URL with a non-empty host.");
    }
    if (parsed->has_userinfo() || !parsed->encoded_path().empty() || parsed->has_query() || parsed->has_fragment() ||
        host.find('\\') != std::string_view::npos) {
        throw std::invalid_argument("AuthorityHost must not contain userinfo, a path, query or fragment.");
    }
    if (parsed->has_port() && parsed->port_number() == 0) {
        throw std::invalid_argument("AuthorityHost has an invalid port.");
    }
    return std::string{host};
}

HttpRequest BuildEntraTokenRequest(std::string_view authorityHost, TenantIdLabel tenantId, std::string body) {
    std::string url = NormalizeAuthorityHost(authorityHost);
    url += '/';
    url += tenantId.Value;
    url += "/oauth2/v2.0/token";

    HttpRequest request;
    request.SetMethod(HttpMethod::Post);
    request.SetUrl(std::move(url));
    request.AddHeader(HttpHeader{"Content-Type", "application/x-www-form-urlencoded"});
    request.SetBody(std::move(body));
    return request;
}

std::optional<std::string> ReadTokenFile(const std::string& path) {
    std::ifstream file{path, std::ios::binary};
    if (!file) {
        return std::nullopt;
    }
    std::string content{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
    constexpr std::string_view Whitespace = " \t\r\n";
    const std::size_t first = content.find_first_not_of(Whitespace);
    if (first == std::string::npos) {
        return std::nullopt;
    }
    const std::size_t last = content.find_last_not_of(Whitespace);
    content = content.substr(first, last - first + 1);
    if (content.find_first_of(Whitespace) != std::string::npos) {
        return std::nullopt;
    }
    return content;
}

void ValidateOidcRequestUri(std::string_view uri) {
    const auto parsed = boost::urls::parse_absolute_uri(uri);
    if (!parsed || parsed->scheme_id() != boost::urls::scheme::https || !parsed->has_authority() ||
        parsed->host().empty() || parsed->has_userinfo() || uri.find('\\') != std::string_view::npos) {
        throw std::invalid_argument("OidcRequestUri must be an https URL with a non-empty host and no fragment.");
    }
}

std::string ParseOidcToken(const HttpResponse& response) {
    boost::system::error_code ec;
    const boost::json::value root = boost::json::parse(response.GetBody(), ec);
    if (ec || !root.is_object()) {
        return {};
    }
    return JsonText(root.as_object(), "oidcToken");
}
} // namespace Private

ClientSecretCredentialOptions ClientSecretCredentialOptions::FromEnvironment() {
    ClientSecretCredentialOptions options;
    options.TenantId = Private::GetEnvironmentValue("AZURE_TENANT_ID");
    options.ClientId = Private::GetEnvironmentValue("AZURE_CLIENT_ID");
    options.ClientSecret = Private::GetEnvironmentValue("AZURE_CLIENT_SECRET");
    if (std::string authorityHost = Private::GetEnvironmentValue("AZURE_AUTHORITY_HOST"); !authorityHost.empty()) {
        options.AuthorityHost = std::move(authorityHost);
    }
    return options;
}

WorkloadIdentityCredentialOptions WorkloadIdentityCredentialOptions::FromEnvironment() {
    WorkloadIdentityCredentialOptions options;
    options.TenantId = Private::GetEnvironmentValue("AZURE_TENANT_ID");
    options.ClientId = Private::GetEnvironmentValue("AZURE_CLIENT_ID");
    options.TokenFilePath = Private::GetEnvironmentValue("AZURE_FEDERATED_TOKEN_FILE");
    if (std::string authorityHost = Private::GetEnvironmentValue("AZURE_AUTHORITY_HOST"); !authorityHost.empty()) {
        options.AuthorityHost = std::move(authorityHost);
    }
    return options;
}

ManagedIdentityCredentialOptions ManagedIdentityCredentialOptions::FromEnvironment() {
    ManagedIdentityCredentialOptions options;
    options.IdentityEndpoint = Private::GetEnvironmentValue("IDENTITY_ENDPOINT");
    options.IdentityHeader = Private::GetEnvironmentValue("IDENTITY_HEADER");
    return options;
}

template class BasicClientSecretCredential<HttpClient>;
template class BasicWorkloadIdentityCredential<HttpClient>;
template class BasicManagedIdentityCredential<HttpClient>;
} // namespace AVEVA::AzureClient
