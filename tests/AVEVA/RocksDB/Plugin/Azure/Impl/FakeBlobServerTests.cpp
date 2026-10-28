// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RequestHelpers.hpp"

#include "FakeBlobEnvironment.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::Impl::Block;
using AVEVA::RocksDB::Plugin::Azure::Impl::Call;
using AVEVA::RocksDB::Plugin::Azure::Impl::Spawn;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;

namespace {
constexpr int64_t Page = 512;
constexpr unsigned int Ok = 0U;

class FakeBlobServerTests : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_env;

    // The blob's transport is driven by the environment's pump thread, so results travel through a future.
    std::future<unsigned int> Upload(const int64_t offset, const char fill) {
        auto payload = std::make_shared<const std::vector<char>>(static_cast<size_t>(Page), fill);
        auto result = Spawn(m_env.Client->get_executor(),
                            m_env.Client->UploadPagesAsync(static_cast<uint64_t>(offset), std::move(payload)));
        return std::async(std::launch::deferred, [result = std::move(result)]() mutable {
            const auto outcome = result.get();
            return outcome ? Ok : outcome.error().StatusCode;
        });
    }

    // The HTTP status of the failure, or Ok.
    template <class TAwaitable> [[nodiscard]] unsigned int Status(TAwaitable operation) {
        const auto outcome = Block(*m_env.Client, std::move(operation));
        return outcome ? Ok : outcome.error().StatusCode;
    }

    [[nodiscard]] std::vector<std::pair<uint64_t, uint64_t>> Ranges() {
        std::vector<std::pair<uint64_t, uint64_t>> ranges;
        for (const auto& range : Call(*m_env.Client, m_env.Client->GetPageRangesAsync()).Ranges) {
            ranges.emplace_back(range.Offset, range.Length);
        }
        return ranges;
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
    EXPECT_EQ(static_cast<size_t>(2 * Page), m_env.Blob->Bytes().size());
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
    ASSERT_EQ(Ok, Status(m_env.Client->AcquireLeaseAsync(acquire)));

    AVEVA::AzureClient::ReleaseLeaseOptions release;
    release.LeaseId = "first";
    ASSERT_EQ(Ok, Status(m_env.Client->ReleaseLeaseAsync(release)));

    acquire.ProposedLeaseId = "second";
    ASSERT_EQ(Ok, Status(m_env.Client->AcquireLeaseAsync(acquire)));

    AVEVA::AzureClient::RenewLeaseOptions renew;
    renew.LeaseId = "first";
    EXPECT_EQ(409U, Status(m_env.Client->RenewLeaseAsync(renew)));

    renew.LeaseId = "second";
    EXPECT_EQ(Ok, Status(m_env.Client->RenewLeaseAsync(renew)));
}

TEST_F(FakeBlobServerTests, LeasedBlob_RejectsDeleteAndResizeWithoutTheLeaseId) {
    AVEVA::AzureClient::AcquireLeaseOptions acquire;
    acquire.ProposedLeaseId = "held";
    acquire.Duration = std::chrono::seconds(60);
    ASSERT_EQ(Ok, Status(m_env.Client->AcquireLeaseAsync(acquire)));

    EXPECT_EQ(412U, Status(m_env.Client->DeleteAsync()));
    EXPECT_EQ(412U, Status(m_env.Client->ResizeAsync(2 * Page)));

    AVEVA::AzureClient::DeleteBlobOptions withLease;
    withLease.Conditions.LeaseId = "held";
    EXPECT_EQ(Ok, Status(m_env.Client->DeleteAsync(withLease)));
}

TEST_F(FakeBlobServerTests, LeaseLapsedByTheInjectedClock_NoLongerFencesWrites) {
    auto now = std::make_shared<std::atomic<std::chrono::steady_clock::time_point>>(std::chrono::steady_clock::now());
    m_env.Blob->SetClock([now] { return now->load(); });
    AVEVA::AzureClient::AcquireLeaseOptions acquire;
    acquire.ProposedLeaseId = "held";
    acquire.Duration = std::chrono::seconds(15);
    ASSERT_EQ(Ok, Status(m_env.Client->AcquireLeaseAsync(acquire)));
    ASSERT_EQ(412U, Status(m_env.Client->ResizeAsync(2 * Page)));

    now->store(now->load() + std::chrono::seconds(16));

    EXPECT_EQ(Ok, Status(m_env.Client->ResizeAsync(2 * Page)));
}
