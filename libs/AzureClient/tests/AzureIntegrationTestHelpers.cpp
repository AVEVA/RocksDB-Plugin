#include "AzureIntegrationTestHelpers.hpp"
#include "AVEVA/AzureClient/ITokenCredential.hpp"

#include <AVEVA/AzureClient/Credentials.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/io_context.hpp>

#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <fileapi.h>
#include <minwindef.h>
#include <processenv.h>
#include <wincrypt.h>
#endif

namespace AVEVA::AzureClient::IntegrationTests
{
    namespace Detail
    {
        std::string GetEnvironmentVariable(const char* name)
        {
#ifdef _WIN32
            // getenv_s avoids the caller-owned malloc'd buffer that _dupenv_s returns.
            std::size_t required = 0;
            if (getenv_s(&required, nullptr, 0, name) != 0 || required == 0)
            {
                return {};
            }

            std::string value(required, '\0');
            if (getenv_s(&required, value.data(), value.size(), name) != 0)
            {
                return {};
            }

            value.resize(required - 1);
            return value;
#else
            const char* value = std::getenv(name);
            return value != nullptr ? std::string{value} : std::string{};
#endif
        }

        std::string Base64Encode(const std::vector<unsigned char>& value)
        {
            static constexpr std::string_view Base64Alphabet =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

            std::string result;
            result.reserve(((value.size() + 2) / 3) * 4);

            std::size_t i = 0;
            for (; i + 2 < value.size(); i += 3)
            {
                const std::uint32_t chunk = (static_cast<std::uint32_t>(value.at(i)) << 16) |
                                            (static_cast<std::uint32_t>(value.at(i + 1)) << 8) | value.at(i + 2);
                result += Base64Alphabet.at((chunk >> 18) & 0x3F);
                result += Base64Alphabet.at((chunk >> 12) & 0x3F);
                result += Base64Alphabet.at((chunk >> 6) & 0x3F);
                result += Base64Alphabet.at(chunk & 0x3F);
            }

            const std::size_t remaining = value.size() - i;
            if (remaining == 1)
            {
                const std::uint32_t chunk = static_cast<std::uint32_t>(value.at(i)) << 16;
                result += Base64Alphabet.at((chunk >> 18) & 0x3F);
                result += Base64Alphabet.at((chunk >> 12) & 0x3F);
                result += "==";
            }
            else if (remaining == 2)
            {
                const std::uint32_t chunk = (static_cast<std::uint32_t>(value.at(i)) << 16) |
                                            (static_cast<std::uint32_t>(value.at(i + 1)) << 8);
                result += Base64Alphabet.at((chunk >> 18) & 0x3F);
                result += Base64Alphabet.at((chunk >> 12) & 0x3F);
                result += Base64Alphabet.at((chunk >> 6) & 0x3F);
                result += "=";
            }

            return result;
        }

        std::string UrlEncodeQueryValue(std::string_view value)
        {
            static constexpr std::string_view HexDigits = "0123456789ABCDEF";
            std::string result;
            for (unsigned char const c : value)
            {
                if ((std::isalnum(c) != 0) || c == '-' || c == '_' || c == '.' || c == '~')
                {
                    result += static_cast<char>(c);
                }
                else
                {
                    result += '%';
                    result += HexDigits.at((c >> 4) & 0xF);
                    result += HexDigits.at(c & 0xF);
                }
            }
            return result;
        }
    } // namespace Detail

    std::optional<AzureTestConfig> LoadAzureTestConfig()
    {
        const std::string accountName = Detail::GetEnvironmentVariable("AZURE_STORAGE_ACCOUNT_NAME");
        const std::string tenantId = Detail::GetEnvironmentVariable("AZURE_TENANT_ID");
        const std::string clientId = Detail::GetEnvironmentVariable("AZURE_SERVICE_PRINCIPAL_ID");
        const std::string clientSecret = Detail::GetEnvironmentVariable("AZURE_SERVICE_PRINCIPAL_SECRET");

        if (accountName.empty() || tenantId.empty() || clientId.empty() || clientSecret.empty())
        {
            return std::nullopt;
        }

        AzureTestConfig config;
        config.AccountName = accountName;
        config.ServiceEndpoint = "https://" + accountName + ".blob.core.windows.net/";
        config.TenantId = tenantId;
        config.ClientId = clientId;
        config.ClientSecret = clientSecret;
        return config;
    }

    namespace Detail
    {
        // Extracts the string value of a top-level JSON field, e.g. "access_token", from
        // a JSON object without needing a full JSON parser. Returns std::nullopt if the
        // field isn't found.
        std::optional<std::string> ExtractJsonStringField(std::string_view json, JsonFieldName fieldName)
        {
            const std::string needle = "\"" + std::string(fieldName.Value) + "\"";
            std::size_t keyPos = json.find(needle);
            if (keyPos == std::string::npos)
            {
                return std::nullopt;
            }

            std::size_t colonPos = json.find(':', keyPos + needle.size());
            if (colonPos == std::string::npos)
            {
                return std::nullopt;
            }

            std::size_t valueStart = json.find('"', colonPos + 1);
            if (valueStart == std::string::npos)
            {
                return std::nullopt;
            }
            ++valueStart;

            std::string value;
            for (std::size_t i = valueStart; i < json.size(); ++i)
            {
                if (json.at(i) == '\\' && i + 1 < json.size())
                {
                    value += json.at(i + 1);
                    ++i;
                    continue;
                }
                if (json.at(i) == '"')
                {
                    return value;
                }
                value += json.at(i);
            }

            return std::nullopt;
        }

        std::string BuildAadTokenRequestBody(const AzureTestConfig& config)
        {
            return std::string("grant_type=client_credentials") + "&client_id=" + UrlEncodeQueryValue(config.ClientId) +
                   "&client_secret=" + UrlEncodeQueryValue(config.ClientSecret) +
                   "&scope=" + UrlEncodeQueryValue("https://storage.azure.com/.default");
        }

        std::optional<std::string> ExtractAadAccessToken(const HttpResponse& response, std::error_code transportError)
        {
            if (transportError || response.GetStatus() != 200)
            {
                return std::nullopt;
            }

            return ExtractJsonStringField(response.GetBody(), "access_token");
        }
    } // namespace Detail

    std::optional<std::string> AcquireAadAccessToken(const AzureTestConfig& config, const std::string& caFile)
    {
        boost::asio::io_context context;
        HttpClientOptions httpClientOptions;
        if (!caFile.empty())
        {
            httpClientOptions.SetCaFile(caFile);
        }
        std::unique_ptr<IHttpClient> const httpClient = IHttpClient::Create(context, httpClientOptions);

        ClientSecretCredentialOptions credentialOptions;
        credentialOptions.TenantId = config.TenantId;
        credentialOptions.ClientId = config.ClientId;
        credentialOptions.ClientSecret = config.ClientSecret;
        ClientSecretCredential credential{*httpClient, std::move(credentialOptions)};

        std::optional<std::string> token;
        credential.GetTokenAsync({"https://storage.azure.com/.default"},
            [&](std::error_code error, AccessToken accessToken)
        {
            if (!error)
            {
                token = std::move(accessToken.Token);
            }
        });
        context.run();
        return token;
    }

    std::string GenerateUniqueContainerName(const std::string& prefix)
    {
        std::random_device randomDevice;
        std::mt19937_64 generator(randomDevice());
        std::uniform_int_distribution<std::uint64_t> distribution;

        std::ostringstream name;
        name << prefix << std::hex
             << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count()
             << "-" << distribution(generator);
        return name.str();
    }

    std::string EncodeBlockId(const std::string& label)
    {
        return Detail::Base64Encode(std::vector<unsigned char>(label.begin(), label.end()));
    }

    TemporaryCaBundle::~TemporaryCaBundle()
    {
        if (!Path.empty())
        {
            std::remove(Path.c_str());
        }
    }

#ifdef _WIN32
    std::optional<TemporaryCaBundle> ExportSystemCaBundle()
    {
        HCERTSTORE store = CertOpenSystemStoreW(0, L"ROOT");
        if (store == nullptr)
        {
            return std::nullopt;
        }

        std::ostringstream pem;
        PCCERT_CONTEXT context = nullptr;
        while ((context = CertEnumCertificatesInStore(store, context)) != nullptr)
        {
            const std::span<const unsigned char> derSpan(context->pbCertEncoded, context->cbCertEncoded);
            const std::vector<unsigned char> derBytes(derSpan.begin(), derSpan.end());
            const std::string base64 = Detail::Base64Encode(derBytes);

            pem << "-----BEGIN CERTIFICATE-----\n";
            for (std::size_t i = 0; i < base64.size(); i += 64)
            {
                pem << base64.substr(i, 64) << "\n";
            }
            pem << "-----END CERTIFICATE-----\n";
        }
        CertCloseStore(store, 0);

        std::array<char, MAX_PATH> tempDirectory{};
        std::array<char, MAX_PATH> tempFilePath{};
        if (GetTempPathA(MAX_PATH, tempDirectory.data()) == 0 ||
            GetTempFileNameA(tempDirectory.data(), "aca", 0, tempFilePath.data()) == 0)
        {
            return std::nullopt;
        }

        std::ofstream file(tempFilePath.data(), std::ios::binary | std::ios::trunc);
        if (!file)
        {
            return std::nullopt;
        }
        file << pem.str();
        file.close();

        return TemporaryCaBundle{std::string(tempFilePath.data())};
    }
#else
    std::optional<TemporaryCaBundle> ExportSystemCaBundle()
    {
        // OpenSSL's default verify paths (e.g. /etc/ssl/certs) work as-is on Linux.
        return std::nullopt;
    }
#endif
} // namespace AVEVA::AzureClient::IntegrationTests
