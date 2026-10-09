// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LeaseRenewalLoop.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeHttpClient.hpp"
#include "FakeHttpPump.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

using AVEVA::HttpResponse;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::LeaseRenewalLoop;
using AVEVA::RocksDB::Plugin::Azure::Impl::LockFileImpl;

namespace {
class LockFileImplTests : public ::testing::Test {
  protected:
    void SetUp() override {
        m_httpClient.CompleteInline() = true;
        // Some completions are posted to the fake's own io_context while Lock() blocks on a future.
        m_pump = AVEVA::RocksDB::Plugin::Azure::Impl::Tests::StartFakeHttpPump(m_httpClient);
        m_runtime = std::make_shared<ClientRuntime>(m_context);
        m_logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    }

    std::unique_ptr<LockFileImpl> CreateLock(std::chrono::seconds leaseLength) {
        auto client =
            std::make_unique<AVEVA::AzureClient::PageBlobClient>(m_httpClient, MakeBlobClientOptions("locks", "LOCK"));
        return std::make_unique<LockFileImpl>(m_runtime, std::move(client), leaseLength, m_logger, "LOCK");
    }

    static HttpResponse Acquired() {
        return HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""};
    }

    std::string ProposedLeaseId(std::size_t index) const {
        return FakeHttpClient::FindHeaderValue(m_httpClient.RequestAt(index).Request, "x-ms-proposed-lease-id");
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    std::shared_ptr<ClientRuntime> m_runtime;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    std::jthread m_pump;
};
} // namespace

TEST_F(LockFileImplTests, RetryReusesTheSameProposedLeaseId) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(503, "ServerBusy", "busy", "r1"));
    m_httpClient.EnqueueResponse(Acquired());

    auto lock = CreateLock(std::chrono::seconds(20));
    EXPECT_TRUE(lock->Lock());

    ASSERT_EQ(m_httpClient.RequestCount(), 2U);
    EXPECT_FALSE(ProposedLeaseId(0).empty());
    EXPECT_EQ(ProposedLeaseId(0), ProposedLeaseId(1));
}

TEST_F(LockFileImplTests, ProposedLeaseIdIsAUuid) {
    m_httpClient.EnqueueResponse(Acquired());

    auto lock = CreateLock(std::chrono::seconds(20));
    ASSERT_TRUE(lock->Lock());

    const auto id = ProposedLeaseId(0);
    EXPECT_NO_THROW({
        const auto parsed = boost::uuids::string_generator()(id);
        EXPECT_EQ(parsed.version(), boost::uuids::uuid::version_random_number_based);
    });
}

TEST_F(LockFileImplTests, ForbiddenThrowsWithoutRetrying) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(403, "AuthorizationFailure", "denied", "r1"));
    m_httpClient.EnqueueResponse(Acquired());

    auto lock = CreateLock(std::chrono::seconds(20));
    try {
        (void)lock->Lock();
        FAIL() << "Expected RequestFailedException";
    } catch (const RequestFailedException& ex) {
        EXPECT_EQ(ex.StatusCode, 403U);
        EXPECT_EQ(ex.ErrorCode, "AuthorizationFailure");
    }
    EXPECT_EQ(m_httpClient.RequestCount(), 1U);
}

TEST_F(LockFileImplTests, LeaseStillHeldWhenRetriesRunOutReturnsFalse) {
    // 15s is the shortest lease the service accepts; retries run every 250ms for that long.
    for (int i = 0; i < 100; ++i) {
        m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseAlreadyPresent", "held", "r1"));
    }

    auto lock = CreateLock(std::chrono::seconds(15));
    EXPECT_FALSE(lock->Lock());
    EXPECT_GE(m_httpClient.RequestCount(), 2U);
}

TEST_F(LockFileImplTests, LeaseHeldByAnotherOwnerIsRetried) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseAlreadyPresent", "held", "r1"));
    m_httpClient.EnqueueResponse(Acquired());

    auto lock = CreateLock(std::chrono::seconds(20));
    EXPECT_TRUE(lock->Lock());
    EXPECT_EQ(m_httpClient.RequestCount(), 2U);
}

TEST_F(LockFileImplTests, StateQueriesAreNotBlockedWhileLockIsRetrying) {
    for (int i = 0; i < 6; ++i) {
        m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseAlreadyPresent", "held", "r1"));
    }
    m_httpClient.EnqueueResponse(Acquired());

    auto lock = CreateLock(std::chrono::seconds(20));
    std::atomic<bool> lockResult{false};
    std::jthread locker([&] { lockResult = lock->Lock(); });
    while (m_httpClient.RequestCount() < 2U) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    const auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(lock->RenewIfLocked());
    EXPECT_FALSE(lock->IsRenewalOverdue());
    EXPECT_FALSE(lock->Lock()); // A concurrent acquire is rejected rather than queued behind the retry loop.
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(200));

    locker.join();
    EXPECT_TRUE(lockResult);
}

TEST_F(LockFileImplTests, RenewAndUnlockBeforeLockThrowAndSendNothing) {
    auto lock = CreateLock(std::chrono::seconds(20));
    EXPECT_THROW(lock->Renew(), std::runtime_error);
    EXPECT_THROW(lock->Unlock(), std::runtime_error);
    EXPECT_FALSE(lock->RenewIfLocked());
    EXPECT_EQ(m_httpClient.RequestCount(), 0U);
}

TEST_F(LockFileImplTests, RenewSendsTheAcquiredLeaseId) {
    m_httpClient.EnqueueResponse(Acquired());
    m_httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});

    auto lock = CreateLock(std::chrono::seconds(20));
    ASSERT_TRUE(lock->Lock());
    EXPECT_TRUE(lock->RenewIfLocked());

    ASSERT_EQ(m_httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(m_httpClient.RequestAt(1).Request, "x-ms-lease-id"), ProposedLeaseId(0));
}

TEST_F(LockFileImplTests, RenewThatOutlastsTheLeaseIsCancelledAtTheDeadlineAndTheLeaseIsTreatedAsExpired) {
    m_httpClient.EnqueueResponse(Acquired());
    // Never completed: the renewal hangs until the lease has run out.
    m_httpClient.EnqueueDeferredResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({}), ""});

    // 15s is the shortest lease the service accepts, so this test runs for about that long.
    const auto leaseLength = std::chrono::seconds(15);
    auto lock = CreateLock(leaseLength);
    ASSERT_TRUE(lock->Lock());

    const auto start = std::chrono::steady_clock::now();
    EXPECT_ANY_THROW(lock->Renew());
    const auto elapsed = std::chrono::steady_clock::now() - start;

    // The caller must find out before it would keep writing under a lease the service has already released.
    EXPECT_LT(elapsed, leaseLength);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_TRUE(lock->HasExceededLeaseLength());
    EXPECT_THROW(lock->Renew(), std::runtime_error);
    EXPECT_EQ(m_httpClient.RequestCount(), 2U);
}

TEST_F(LockFileImplTests, UnlockReleasesTheLeaseAndStopsFurtherRenewal) {
    m_httpClient.EnqueueResponse(Acquired());
    m_httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({}), ""});

    auto lock = CreateLock(std::chrono::seconds(20));
    ASSERT_TRUE(lock->Lock());
    EXPECT_NO_THROW(lock->Unlock());

    EXPECT_FALSE(lock->RenewIfLocked());
    EXPECT_THROW(lock->Unlock(), std::runtime_error);
    EXPECT_EQ(m_httpClient.RequestCount(), 2U);
}

namespace {
struct AsyncRenewOutcome {
    std::exception_ptr Error;
    bool Renewed = false;
};

AsyncRenewOutcome RenewAndWait(const LockFileImpl& lock) {
    std::promise<AsyncRenewOutcome> outcome;
    auto future = outcome.get_future();
    lock.RenewAsync([&outcome](std::exception_ptr error, const bool renewed) {
        outcome.set_value(AsyncRenewOutcome{std::move(error), renewed});
    });
    EXPECT_EQ(future.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    return future.get();
}
} // namespace

TEST_F(LockFileImplTests, RenewAsyncBeforeLockSendsNothingAndReportsNoRenewal) {
    auto lock = CreateLock(std::chrono::seconds(20));
    const auto outcome = RenewAndWait(*lock);
    EXPECT_FALSE(outcome.Error);
    EXPECT_FALSE(outcome.Renewed);
    EXPECT_EQ(m_httpClient.RequestCount(), 0U);
}

TEST_F(LockFileImplTests, RenewAsyncSendsTheAcquiredLeaseIdAndReportsSuccess) {
    m_httpClient.EnqueueResponse(Acquired());
    m_httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});

    auto lock = CreateLock(std::chrono::seconds(20));
    ASSERT_TRUE(lock->Lock());
    const auto outcome = RenewAndWait(*lock);

    EXPECT_FALSE(outcome.Error);
    EXPECT_TRUE(outcome.Renewed);
    ASSERT_EQ(m_httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(m_httpClient.RequestAt(1).Request, "x-ms-lease-id"), ProposedLeaseId(0));
}

TEST_F(LockFileImplTests, RenewAsyncReportsAFailedRenewalAsAnError) {
    m_httpClient.EnqueueResponse(Acquired());
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseIdMismatchWithLeaseOperation", "mismatch", "r2"));

    auto lock = CreateLock(std::chrono::seconds(20));
    ASSERT_TRUE(lock->Lock());
    const auto outcome = RenewAndWait(*lock);

    ASSERT_TRUE(outcome.Error);
    EXPECT_THROW(std::rethrow_exception(outcome.Error), RequestFailedException);
}
namespace {
// Drives a LeaseRenewalLoop on its own io_context thread, separate from the fake HTTP client's.
class LeaseRenewalLoopTests : public LockFileImplTests {
  protected:
    void SetUp() override {
        LockFileImplTests::SetUp();
        m_guard.emplace(boost::asio::make_work_guard(m_loopContext));
        m_loopThread = std::jthread([this] { m_loopContext.run(); });
    }

    void TearDown() override {
        if (m_loop) {
            m_loop->Stop();
        }
        m_guard.reset();
    }

    std::shared_ptr<LockFileImpl> AcquireLock() {
        m_httpClient.EnqueueResponse(Acquired());
        std::shared_ptr<LockFileImpl> lock = CreateLock(std::chrono::seconds(20));
        EXPECT_TRUE(lock->Lock());
        return lock;
    }

    void StartLoop(const std::shared_ptr<LockFileImpl>& lock) {
        m_loop = LeaseRenewalLoop::Start(
            m_loopContext.get_executor(), m_logger, [lock] { return std::vector{lock}; }, [this] { ++m_fatalCount; },
            std::chrono::milliseconds(20), std::chrono::milliseconds(5));
    }

    bool WaitForRequests(const std::size_t count) const {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (m_httpClient.RequestCount() < count && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return m_httpClient.RequestCount() >= count;
    }

    static HttpResponse Renewed() {
        return HttpResponse{200, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""};
    }

    boost::asio::io_context m_loopContext;
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> m_guard;
    std::jthread m_loopThread;
    std::shared_ptr<LeaseRenewalLoop> m_loop;
    std::atomic<int> m_fatalCount{0};
};
} // namespace

TEST_F(LeaseRenewalLoopTests, RenewsTheLeaseOnEveryInterval) {
    auto lock = AcquireLock();
    for (int i = 0; i < 50; ++i) {
        m_httpClient.EnqueueResponse(Renewed());
    }
    StartLoop(lock);

    EXPECT_TRUE(WaitForRequests(4)); // the acquire plus at least three renewals
    EXPECT_EQ(m_fatalCount, 0);
}

TEST_F(LeaseRenewalLoopTests, StopHaltsFurtherRenewals) {
    auto lock = AcquireLock();
    for (int i = 0; i < 50; ++i) {
        m_httpClient.EnqueueResponse(Renewed());
    }
    StartLoop(lock);
    ASSERT_TRUE(WaitForRequests(2));

    m_loop->Stop();
    const auto afterStop = m_httpClient.RequestCount();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    EXPECT_EQ(m_httpClient.RequestCount(), afterStop);
    EXPECT_TRUE(m_loop->IsStopped());
    EXPECT_EQ(m_fatalCount, 0);
}

TEST_F(LeaseRenewalLoopTests, ConflictIsFatalAndStopsTheLoop) {
    auto lock = AcquireLock();
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseIdMismatchWithLeaseOperation", "mismatch", "r2"));
    StartLoop(lock);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (m_fatalCount == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    EXPECT_EQ(m_fatalCount, 1);
    EXPECT_TRUE(m_loop->IsStopped());
}

TEST_F(LeaseRenewalLoopTests, TransientFailureIsRetriedWithoutBeingFatal) {
    auto lock = AcquireLock();
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(500, "InternalError", "boom", "r2"));
    for (int i = 0; i < 50; ++i) {
        m_httpClient.EnqueueResponse(Renewed());
    }
    StartLoop(lock);

    EXPECT_TRUE(WaitForRequests(4));
    EXPECT_EQ(m_fatalCount, 0);
    EXPECT_FALSE(m_loop->IsStopped());
}