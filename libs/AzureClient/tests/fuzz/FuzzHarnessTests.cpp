// Replays a seed corpus through the libFuzzer harness (tests/fuzz/ParseXmlFuzz.cpp) on every
// toolchain, so the harness keeps compiling and its seeds double as regression inputs.
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size);

namespace
{
    int RunHarness(std::uint8_t selector, std::string_view input)
    {
        std::vector<std::uint8_t> bytes;
        bytes.reserve(input.size() + 1U);
        bytes.push_back(selector);
        bytes.insert(bytes.end(), input.begin(), input.end());
        return LLVMFuzzerTestOneInput(bytes.data(), bytes.size());
    }
} // namespace

TEST(FuzzHarnessTests, SeedCorpusIsHandledWithoutEscapingExceptions)
{
    const std::vector<std::string> seeds{
        "",
        "<EnumerationResults><Blobs><Blob><Name>a&amp;b</Name><Properties><Content-Length>5</Content-Length></"
        "Properties></Blob></Blobs></EnumerationResults>",
        "<EnumerationResults><Containers><Container><Name>c</Name></Container></Containers><NextMarker>m</NextMarker></"
        "EnumerationResults>",
        "<BlockList><CommittedBlocks><Block><Name>YQ==</Name><Size>1</Size></Block></CommittedBlocks></BlockList>",
        "<PageList><PageRange><Start>0</Start><End>511</End></PageRange></PageList>",
        "<Tags><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tags>",
        "<StorageServiceProperties><Cors><CorsRule><MaxAgeInSeconds>x</MaxAgeInSeconds></CorsRule></Cors></"
        "StorageServiceProperties>",
        "<UserDelegationKey><SignedStart>2024-01-01T00:00:00Z</SignedStart><Value>dg==</Value></UserDelegationKey>",
        "Tue, 02 Jan 2024 00:00:00 GMT",
        "bytes 0-9/10",
        R"({"access_token":"t","expires_on":"9223372036854775807"})",
        R"({"access_token":"t","expires_in":"3600"})",
        "2024-01-02T03:04:05.1234567Z",
        "BlobNotFound\n<Error><Code>BlobNotFound</Code><Message>m</Message></Error>",
        "<",
        std::string{"\xFF\xFE\x00<A>", 6},
        std::string(10000, '<'),
    };
    for (std::uint8_t selector = 0; selector < 13U; ++selector)
    {
        for (const std::string& seed : seeds)
        {
            EXPECT_EQ(RunHarness(selector, seed), 0);
        }
    }
    EXPECT_EQ(LLVMFuzzerTestOneInput(nullptr, 0U), 0);
}
