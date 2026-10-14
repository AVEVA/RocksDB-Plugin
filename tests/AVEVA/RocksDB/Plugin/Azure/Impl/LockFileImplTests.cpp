// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LeaseRenewalLoop.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <algorithm>
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
using AVEVA::RocksDB::Plugin::Azure::Impl::BasicLeaseRenewalLoop;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntimeBase;
// The lock over the library's page blob client on the fake transport.
using LockFileImpl = AVEVA::RocksDB::Plugin::Azure::Impl::BasicLockFileImpl<AVEVA::AzureClient::Tests::PageBlobClient>;

namespace {
class LockFileImplTests : public ::testing::Test {
  protected:
    void SetUp() override {
        m_httpClient.CompleteInline() = true;
        // Some completions are posted to the fake's own io_context while Lock() blocks on a future.
        m_httpClient.StartBackgroundPump();
        m_runtime = std::make_shared<ClientRuntimeBase>();
        m_logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    }

    std::unique_ptr<LockFileImpl> CreateLock(std::chrono::seconds leaseLength,
                                             std::optional<std::chrono::milliseconds> acquireTimeout = std::nullopt) {
        auto client = std::make_unique<AVEVA::AzureClient::Tests::PageBlobClient>(
            m_httpClient, MakeBlobClientOptions("locks", "LOCK"));
        return std::make_unique<LockFileImpl>(m_runtime, std::move(client), leaseLength, m_logger, "LOCK",
                                              acquireTimeout);
    }

    static HttpResponse Acquired() {
        return HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""};
    }

    std::string ProposedLeaseId(std::size_t index) const {
        return FakeHttpClient::FindHeaderValue(m_httpClient.RequestAt(index).Request, "x-ms-proposed-lease-id");
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    std::shared_ptr<ClientRuntimeBase> m_runtime;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
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
    // A short acquire timeout (the lease is still the service minimum of 15s) keeps the retry loop brief.
    for (int i = 0; i < 100; ++i) {
        m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseAlreadyPresent", "held", "r1"));
    }

    auto lock = CreateLock(std::chrono::seconds(15), std::chrono::milliseconds(600));
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

    // 15s is the shortest lease the service accepts. The lease clock is simulated so that only ~200ms of real time
    // remain before the renewal deadline, which the client enforces with a real timer.
    const auto leaseLength = std::chrono::seconds(15);
    const auto margin = std::min<std::chrono::milliseconds>(AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::LeaseSafetyMargin,
                                                            std::chrono::milliseconds(leaseLength) / 10);
    auto lock = CreateLock(leaseLength);
    auto offset = std::make_shared<std::atomic<std::chrono::steady_clock::duration>>(
        std::chrono::steady_clock::duration::zero());
    lock->SetClockForTesting([offset] { return std::chrono::steady_clock::now() + offset->load(); });
    ASSERT_TRUE(lock->Lock());

    *offset = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::milliseconds(leaseLength) - margin - std::chrono::milliseconds(200));
    ASSERT_FALSE(lock->HasExceededLeaseLength());

    const auto start = std::chrono::steady_clock::now();
    EXPECT_ANY_THROW(lock->Renew());
    const auto elapsed = std::chrono::steady_clock::now() - start;

    // The caller must find out at the renewal deadline, not hang until the lease has run out.
    EXPECT_LT(elapsed, std::chrono::seconds(5));
    *offset = leaseLength;
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
  public:
    struct FixedSnapshot {
        std::shared_ptr<LockFileImpl> Lock;
        std::vector<std::shared_ptr<LockFileImpl>> operator()() const { return {Lock}; }
    };
    struct CountFatal {
        std::atomic<int>* Count;
        void operator()() const { ++*Count; }
    };
    using Loop = BasicLeaseRenewalLoop<FixedSnapshot, CountFatal, LockFileImpl>;

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
        m_loop = Loop::Start(m_loopContext.get_executor(), m_logger, FixedSnapshot{lock}, CountFatal{&m_fatalCount},
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
    std::shared_ptr<Loop> m_loop;
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
    // Stop() is synchronous (timer cancelled, in-flight rounds drained), so nothing can be sent after it returns.
    const auto afterStop = m_httpClient.RequestCount();
    std::this_thread::yield();

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

TEST_F(LeaseRenewalLoopTests, ForbiddenRenewalFencesOnTheFirstRoundWithoutRetrying) {
    auto lock = AcquireLock();
    const auto before = m_httpClient.RequestCount();
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(403, "AuthorizationFailure", "denied", "r2"));
    // Would be consumed only by a retry.
    for (int i = 0; i < 5; ++i) {
        m_httpClient.EnqueueResponse(Renewed());
    }
    StartLoop(lock);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (m_fatalCount == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    m_loop->Stop();

    EXPECT_EQ(m_fatalCount, 1);
    EXPECT_EQ(m_httpClient.RequestCount(), before + 1);
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