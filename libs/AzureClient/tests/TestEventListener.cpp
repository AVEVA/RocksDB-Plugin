#include "FakeHttpClient.hpp"

#include <gtest/gtest.h>

#include <memory>

namespace
{
    class FakeHttpClientEventListener : public testing::EmptyTestEventListener
    {
      public:
        void OnTestEnd(const testing::TestInfo& /*test_info*/) override
        {
            // Poll all known FakeHttpClient instances to run any completions posted to their io_contexts.
            for (auto* client : AVEVA::AzureClient::Tests::FakeHttpClient::Instances())
            {
                // Drain ready handlers without blocking.
                while (client->Poll() > 0)
                {
                }
            }
        }
    };
} // namespace

// This translation unit provides the test binary's entry point instead of linking GTest::gtest_main, so the
// FakeHttpClient listener can be registered after GoogleTest is initialized rather than during dynamic
// initialization (which bugprone-throwing-static-initialization rightly rejects).
int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);

    // TestEventListeners::Append takes ownership of the listener and deletes it at shutdown.
    testing::UnitTest::GetInstance()->listeners().Append(std::make_unique<FakeHttpClientEventListener>().release());

    return RUN_ALL_TESTS();
}