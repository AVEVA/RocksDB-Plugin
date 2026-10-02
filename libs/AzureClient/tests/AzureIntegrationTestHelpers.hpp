#pragma once

#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace AVEVA::AzureClient::IntegrationTests
{
    // Service principal / storage account identifiers loaded from the environment,
    // used to run live integration tests against a real Azure Blob Storage account
    // using Microsoft Entra ID (Azure AD) authentication. All values come from
    // AZURE_STORAGE_ACCOUNT_NAME, AZURE_TENANT_ID, AZURE_SERVICE_PRINCIPAL_ID, and
    // AZURE_SERVICE_PRINCIPAL_SECRET.
    struct AzureTestConfig
    {
        std::string AccountName;
        std::string ServiceEndpoint;
        std::string TenantId;
        std::string ClientId;
        std::string ClientSecret;
    };

    // Returns std::nullopt (rather than failing) when the required environment
    // variables aren't set, so these tests can be skipped in environments without
    // Azure credentials.
    std::optional<AzureTestConfig> LoadAzureTestConfig();

    // Acquires an OAuth2 access token for the "https://storage.azure.com/.default"
    // scope via the client-credentials flow, using the service principal identified
    // by config.TenantId/ClientId/ClientSecret. Returns std::nullopt if the token
    // request fails. `caFile`, if non-empty, is used to validate the TLS connection to
    // Microsoft Entra ID (see ExportSystemCaBundle).
    std::optional<std::string> AcquireAadAccessToken(const AzureTestConfig& config, const std::string& caFile = {});

    // Generates a short, unique, lowercase container name suitable for a single test
    // run, e.g. "aveva-it-1a2b3c4d5e6f".
    std::string GenerateUniqueContainerName(const std::string& prefix);

    // Base64-encodes a human-readable label into a valid block ID for use with
    // BlockBlobClient::StageBlockAsync / CommitBlockListAsync.
    std::string EncodeBlockId(const std::string& label);

    namespace Detail
    {
        std::string Base64Encode(const std::vector<unsigned char>& value);
        std::string UrlEncodeQueryValue(std::string_view value);

        // Tags the JSON field name with its own type so it's never adjacent-and-same-type
        // with the JSON document parameter (see bugprone-easily-swappable-parameters).
        // Implicitly constructible from a string literal, so call sites are unaffected.
        struct JsonFieldName
        {
            std::string_view Value;

            constexpr JsonFieldName(const char* value) noexcept : Value(value)
            {
            }

            constexpr JsonFieldName(std::string_view value) noexcept : Value(value)
            {
            }
        };

        std::optional<std::string> ExtractJsonStringField(std::string_view json, JsonFieldName fieldName);
        std::string BuildAadTokenRequestBody(const AzureTestConfig& config);
        std::optional<std::string> ExtractAadAccessToken(const HttpResponse& response,
            std::error_code transportError = {});
    } // namespace Detail

    // A PEM certificate bundle file exported from the local trust store, valid for the
    // lifetime of this object. Pass Path to AVEVA::HttpClientOptions::SetCaFile.
    struct TemporaryCaBundle
    {
        std::string Path;
        ~TemporaryCaBundle();
        TemporaryCaBundle(const TemporaryCaBundle&) = delete;
        TemporaryCaBundle& operator=(const TemporaryCaBundle&) = delete;
        TemporaryCaBundle(TemporaryCaBundle&&) noexcept = default;
        TemporaryCaBundle& operator=(TemporaryCaBundle&&) noexcept = default;

        explicit TemporaryCaBundle(std::string path) : Path(std::move(path))
        {
        }
    };

    // On Windows, OpenSSL has no default trust store, so this exports the current
    // machine's trusted root certificates to a temporary PEM file suitable for
    // HttpClientOptions::SetCaFile. Returns std::nullopt on platforms (e.g. Linux)
    // where OpenSSL's default verify paths already work, or if the export fails.
    std::optional<TemporaryCaBundle> ExportSystemCaBundle();
} // namespace AVEVA::AzureClient::IntegrationTests
