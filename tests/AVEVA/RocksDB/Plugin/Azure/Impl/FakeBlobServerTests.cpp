// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include "FakeBlobEnvironment.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::Impl::Unwrap;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;

namespace {
constexpr int64_t Page = 512;
constexpr unsigned int Ok = 0U;

class FakeBlobServerTests : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_env;

    // Completions may be delivered from the fake transport's pump thread, so results travel through a future.
    template <class TStart> static std::future<unsigned int> Start(TStart&& start) {
        auto promise = std::make_shared<std::promise<unsigned int>>();
        auto future = promise->get_future();
        start([promise](auto result) { promise->set_value(result ? Ok : result.error().StatusCode); });
        return future;
    }

    std::future<unsigned int> Upload(const int64_t offset, const char fill) {
        auto payload = std::make_shared<const std::vector<char>>(static_cast<size_t>(Page), fill);
        return Start([&](auto handler) {
            m_env.Client->UploadPagesAsync(static_cast<uint64_t>(offset), std::move(payload), std::move(handler));
        });
    }

    [[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> Ranges() {
        auto promise = std::make_shared<std::promise<std::vector<std::pair<uint64_t, uint64_t>>>>();
        auto future = promise->get_future();
        m_env.Client->GetPageRangesAsync([promise](auto result) {
            std::vector<std::pair<uint64_t, uint64_t>> ranges;
            for (const auto& range : Unwrap(std::move(result)).Ranges) {
                ranges.emplace_back(range.Offset, range.Length);
            }
            promise->set_value(std::move(ranges));
        });
        return future.get();
    }
};
} // namespace

TEST_F(FakeBlobServerTests, DeferredUploadAfterTheBlobWasShrunk_FailsWith416AndDoesNotRegrowTheBlob) {
    m_env.Blob->SetCapacity(4 * Page);
    m_env.Blob->DeferUploads = true;
    auto status = Upload(3 * Page, 'a');
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());

    m_env.Blob->SetCapacity(2 * Page);
    m_env.Blob->CompleteUpload(0);

    EXPECT_EQ(416U, status.get());
    EXPECT_EQ(static_cast<size_t>(2 * Page), m_env.Blob->Data.size());
}

TEST_F(FakeBlobServerTests, WritingZeros_ReportsTheRange) {
    m_env.Blob->SetCapacity(4 * Page);
    ASSERT_EQ(Ok, Upload(Page, '\0').get());

    EXPECT_EQ((std::vector<std::pair<uint64_t, uint64_t>>{{Page, Page}}), Ranges());
}

TEST_F(FakeBlobServerTests, ShrinkingTheBlob_DropsRangesBeyondTheNewEnd) {
    m_env.Blob->SetCapacity(4 * Page);
    ASSERT_EQ(Ok, Upload(3 * Page, '\0').get());

    m_env.Blob->SetCapacity(2 * Page);
    m_env.Blob->SetCapacity(4 * Page);

    EXPECT_TRUE(Ranges().empty());
}

TEST_F(FakeBlobServerTests, ReleasedLease_CannotBeRenewedWithTheOldIdAfterANewAcquire) {
    AVEVA::AzureClient::AcquireLeaseOptions acquire;
    acquire.ProposedLeaseId = "first";
    acquire.Duration = std::chrono::seconds(60);
    ASSERT_EQ(Ok, Start([&](auto h) { m_env.Client->AcquireLeaseAsync(acquire, std::move(h)); }).get());

    AVEVA::AzureClient::ReleaseLeaseOptions release;
    release.LeaseId = "first";
    ASSERT_EQ(Ok, Start([&](auto h) { m_env.Client->ReleaseLeaseAsync(release, std::move(h)); }).get());

    acquire.ProposedLeaseId = "second";
    ASSERT_EQ(Ok, Start([&](auto h) { m_env.Client->AcquireLeaseAsync(acquire, std::move(h)); }).get());

    AVEVA::AzureClient::RenewLeaseOptions renew;
    renew.LeaseId = "first";
    EXPECT_EQ(409U, Start([&](auto h) { m_env.Client->RenewLeaseAsync(renew, std::move(h)); }).get());

    renew.LeaseId = "second";
    EXPECT_EQ(Ok, Start([&](auto h) { m_env.Client->RenewLeaseAsync(renew, std::move(h)); }).get());
}
