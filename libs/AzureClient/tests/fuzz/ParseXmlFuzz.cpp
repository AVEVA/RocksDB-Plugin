// libFuzzer harness for the response parsers (T28). The first input byte selects the parser and the
// rest is its input. Parsers may reject input only by throwing a std::exception; anything else
// (crash, sanitizer report, non-std exception, hang) is a bug.
//
// Built as a fuzzer with -DAVEVA_AZURE_CLIENT_FUZZ=ON (Clang only); the unit tests also link this file
// and replay a seed corpus through it (FuzzHarnessTests.cpp) so it stays compiled on every toolchain.
#include "../FakeHttpClient.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    using namespace AVEVA::AzureClient::Private;

    // ParseTokenResponse is private to Credentials.cpp, so it is reached through a credential whose token
    // endpoint answers 200 with the fuzz input as the JSON body.
    void ParseTokenResponseThroughCredential(std::string_view body)
    {
        AVEVA::AzureClient::Tests::FakeHttpClient httpClient;
        AVEVA::AzureClient::ManagedIdentityCredentialOptions options;
        options.Retry.MaxRetries = 0;
        AVEVA::AzureClient::ManagedIdentityCredential credential{httpClient, std::move(options)};
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200U, {}, std::string{body}});
        credential.GetTokenAsync({"https://storage.azure.com/.default"},
            [](std::error_code /*error*/, AVEVA::AzureClient::AccessToken /*token*/) {});
        httpClient.Poll();
    }

    void ParseOne(std::uint8_t selector, std::string_view input)
    {
        switch (selector % 13U)
        {
        case 0:
            static_cast<void>(ParseListBlobsResultXml(input));
            break;
        case 1:
            static_cast<void>(ParseListBlobContainersResultXml(input));
            break;
        case 2:
            static_cast<void>(ParseGetBlockListResultXml(input));
            break;
        case 3:
            static_cast<void>(ParseGetPageRangesResultXml(input));
            break;
        case 4:
            static_cast<void>(ParseGetBlobTagsResultXml(input));
            break;
        case 5:
            static_cast<void>(ParseFindBlobsByTagsResultXml(input));
            break;
        case 6:
            static_cast<void>(ParseBlobServicePropertiesXml(input));
            break;
        case 7:
            static_cast<void>(ParseUserDelegationKeyXml(input));
            break;
        case 8:
            static_cast<void>(ParseHttpDateHeader(input));
            break;
        case 9:
            static_cast<void>(ParseContentRange(input));
            break;
        case 10:
            static_cast<void>(ParseIso8601Utc(input));
            break;
        case 11:
            ParseTokenResponseThroughCredential(input);
            break;
        default: {
            // Error response: the first line is the x-ms-error-code header, the rest the body.
            const std::size_t newline = input.find('\n');
            std::vector<AVEVA::HttpHeader> headers;
            headers.emplace_back("x-ms-error-code", std::string{input.substr(0, newline)});
            const std::string body{newline == std::string_view::npos ? std::string_view{} : input.substr(newline + 1U)};
            const AVEVA::HttpResponse response{400U + (selector % 200U), std::move(headers), body};
            static_cast<void>(DetermineBlobStorageFailure({}, response));
            break;
        }
        }
    }
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    if (size == 0U)
    {
        return 0;
    }
    const std::span<const std::uint8_t> bytes{data, size};
    const std::span<const std::uint8_t> payload = bytes.subspan(1);
    const std::string_view input{reinterpret_cast<const char*>(payload.data()), payload.size()};
    try
    {
        ParseOne(bytes.front(), input);
    }
    catch (const std::exception& error)
    {
        // Rejecting malformed input is the expected outcome; touch the message so the handler is not empty.
        static_cast<void>(error.what());
    }
    return 0;
}
