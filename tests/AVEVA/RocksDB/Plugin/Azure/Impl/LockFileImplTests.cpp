// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LeaseRenewalLoop.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/TestLogger.hpp"

#include "BackgroundPump.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
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
        m_runtime = std::make_shared<ClientRuntimeBase>();
        m_logger = AVEVA::RocksDB::Plugin::Tests::MakeTestLogger();
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

    // The fake transport has no timers: a request bounded by a total deadline is timed out by hand once the deadline
    // has passed, as a real transport would.
    std::jthread TimeOutRequestAfter(std::size_t index, std::chrono::milliseconds delay) {
        return std::jthread([this, index, delay] {
            const auto giveUp = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!m_httpClient.IsPending(index) && std::chrono::steady_clock::now() < giveUp) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            std::this_thread::sleep_for(delay);
            (void)m_httpClient.FailRequest(index, std::make_error_code(std::errc::timed_out));
        });
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    AVEVA::RocksDB::Plugin::Azure::Impl::Tests::BackgroundPump m_pump{m_httpClient};
    std::shared_ptr<ClientRuntimeBase> m_runtime;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
};
} // namespace

TEST_F(LockFileImplTests, UnlockDuringInFlightRenewalMakesTheRejectedRenewalBenign) {
    m_httpClient.EnqueueResponse(Acquired());
    auto lock = CreateLock(std::chrono::seconds(20));
    ASSERT_TRUE(lock->Lock());

    // The service rejects the renewal because the release won; it reaches the completion before Unlock returns.
    m_httpClient.EnqueueDeferredResponse(MakeAzureErrorResponse(409, "LeaseIdMismatchWithLeaseOperation", "m", "r2"));
    m_httpClient.EnqueueDeferredResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({}), ""});

    std::promise<std::pair<std::exception_ptr, bool>> outcome;
    lock->RenewAsync([&outcome](std::exception_ptr error, bool renewed) { outcome.set_value({error, renewed}); });
    ASSERT_TRUE(m_httpClient.WaitForRequest(2));

    std::thread unlocker([&lock] { lock->Unlock(); });
    ASSERT_TRUE(m_httpClient.WaitForRequest(3));
    // The rejected renewal completes while the release is still in flight.
    ASSERT_TRUE(m_httpClient.CompleteNext());
    ASSERT_TRUE(m_httpClient.CompleteNext());
    unlocker.join();

    const auto [error, renewed] = outcome.get_future().get();
    EXPECT_FALSE(error);
    EXPECT_FALSE(renewed);
}

TEST_F(LockFileImplTests, InvalidLeaseLengthOrAcquireTimeoutThrowsAtConstruction) {
    EXPECT_THROW(CreateLock(std::chrono::seconds(0)), std::invalid_argument);
    EXPECT_THROW(CreateLock(std::chrono::seconds(-1)), std::invalid_argument);
    EXPECT_THROW(CreateLock(std::chrono::seconds(14)), std::invalid_argument);
    EXPECT_THROW(CreateLock(std::chrono::seconds(61)), std::invalid_argument);
    EXPECT_THROW(CreateLock(std::chrono::seconds(20), std::chrono::milliseconds(0)), std::invalid_argument);
    EXPECT_THROW(CreateLock(std::chrono::seconds(20), std::chrono::milliseconds(-5)), std::invalid_argument);
}

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
    ASSERT_TRUE(m_httpClient.WaitForRequestCount(2U));

    // None of these may queue behind the retry loop; if one did, it would block until the lock finished.
    EXPECT_FALSE(lock->RenewIfLocked());
    EXPECT_FALSE(lock->IsRenewalOverdue());
    EXPECT_FALSE(lock->Lock()); // A concurrent acquire is rejected rather than queued behind the retry loop.

    locker.join();
    EXPECT_TRUE(lockResult);
}

TEST_F(LockFileImplTests, RenewBeforeLockThrowsAndUnlockIsANoOpSendingNothing) {
    auto lock = CreateLock(std::chrono::seconds(20));
    EXPECT_THROW(lock->Renew(), std::runtime_error);
    EXPECT_NO_THROW(lock->Unlock());
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
    const auto margin =
        std::min<std::chrono::milliseconds>(AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::LeaseSafetyMargin,
                                            std::chrono::milliseconds(leaseLength) / 10);
    auto lock = CreateLock(leaseLength);
    ASSERT_TRUE(lock->Lock());

    lock->SetClockOffsetForTesting(std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::milliseconds(leaseLength) - margin - std::chrono::milliseconds(200)));
    ASSERT_FALSE(lock->HasExceededLeaseLength());

    // The caller must find out at the renewal deadline, not hang until the lease has run out.
    auto deadline = TimeOutRequestAfter(1, std::chrono::milliseconds(250));
    EXPECT_ANY_THROW(lock->Renew());
    deadline.join();
    lock->SetClockOffsetForTesting(leaseLength);
    EXPECT_TRUE(lock->HasExceededLeaseLength());
    EXPECT_THROW(lock->Renew(), std::runtime_error);
    EXPECT_EQ(m_httpClient.RequestCount(), 2U);
}

TEST_F(LockFileImplTests, UnlockThatOutlastsTheLeaseIsCancelledAtTheLeaseEnd) {
    m_httpClient.EnqueueResponse(Acquired());
    // Never completed: the release hangs, but the lease lapses on its own.
    m_httpClient.EnqueueDeferredResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({}), ""});

    const auto leaseLength = std::chrono::seconds(15);
    auto lock = CreateLock(leaseLength);
    ASSERT_TRUE(lock->Lock());
    lock->SetClockOffsetForTesting(
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(leaseLength - std::chrono::milliseconds(200)));

    const auto start = std::chrono::steady_clock::now();
    auto deadline = TimeOutRequestAfter(1, std::chrono::milliseconds(250));
    EXPECT_ANY_THROW(lock->Unlock());
    deadline.join();
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(5));
    // Local state is cleared even though the release did not complete.
    EXPECT_FALSE(lock->IsRenewalOverdue());
    EXPECT_NO_THROW(lock->Unlock());
}

TEST_F(LockFileImplTests, UnlockReleasesTheLeaseAndStopsFurtherRenewal) {
    m_httpClient.EnqueueResponse(Acquired());
    m_httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({}), ""});

    auto lock = CreateLock(std::chrono::seconds(20));
    ASSERT_TRUE(lock->Lock());
    EXPECT_NO_THROW(lock->Unlock());

    EXPECT_FALSE(lock->RenewIfLocked());
    EXPECT_NO_THROW(lock->Unlock());
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
// Counts fatal callbacks and lets a test block until one arrives instead of polling.
class FatalCounter {
  public:
    void Increment() {
        {
            std::scoped_lock lock(m_mutex);
            ++m_count;
        }
        m_cv.notify_all();
    }
    [[nodiscard]] int Count() const {
        std::scoped_lock lock(m_mutex);
        return m_count;
    }
    [[nodiscard]] bool WaitForAtLeast(const int count) {
        std::unique_lock lock(m_mutex);
        return m_cv.wait_for(lock, std::chrono::seconds(10), [&] { return m_count >= count; });
    }

  private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    int m_count = 0;
};
} // namespace

namespace {
// Drives a LeaseRenewalLoop on its own io_context thread, separate from the fake HTTP client's.
class LeaseRenewalLoopTests : public LockFileImplTests {
  public:
    struct FixedSnapshot {
        std::shared_ptr<LockFileImpl> Lock;
        std::vector<std::shared_ptr<LockFileImpl>> operator()() const { return {Lock}; }
    };
    struct CountFatal {
        FatalCounter* Count;
        void operator()() const { Count->Increment(); }
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

    bool WaitForRequests(const std::size_t count) { return m_httpClient.WaitForRequestCount(count); }

    static HttpResponse Renewed() {
        return HttpResponse{200, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""};
    }

    boost::asio::io_context m_loopContext;
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> m_guard;
    std::jthread m_loopThread;
    std::shared_ptr<Loop> m_loop;
    FatalCounter m_fatalCount;
};
} // namespace

TEST_F(LeaseRenewalLoopTests, RenewsTheLeaseOnEveryInterval) {
    auto lock = AcquireLock();
    for (int i = 0; i < 50; ++i) {
        m_httpClient.EnqueueResponse(Renewed());
    }
    StartLoop(lock);

    EXPECT_TRUE(WaitForRequests(4)); // the acquire plus at least three renewals
    EXPECT_EQ(m_fatalCount.Count(), 0);
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
    EXPECT_EQ(m_fatalCount.Count(), 0);
}

TEST_F(LeaseRenewalLoopTests, ConflictIsFatalAndStopsTheLoop) {
    auto lock = AcquireLock();
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseIdMismatchWithLeaseOperation", "mismatch", "r2"));
    StartLoop(lock);

    EXPECT_TRUE(m_fatalCount.WaitForAtLeast(1));

    EXPECT_EQ(m_fatalCount.Count(), 1);
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

    EXPECT_TRUE(m_fatalCount.WaitForAtLeast(1));
    m_loop->Stop();

    EXPECT_EQ(m_fatalCount.Count(), 1);
    EXPECT_EQ(m_httpClient.RequestCount(), before + 1);
}

TEST_F(LeaseRenewalLoopTests, NotImplementedRenewalFencesOnTheFirstRoundWithoutRetrying) {
    auto lock = AcquireLock();
    const auto before = m_httpClient.RequestCount();
    // 501 is a 5xx the client's retry policy treats as permanent.
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(501, "NotImplemented", "nope", "r2"));
    for (int i = 0; i < 5; ++i) {
        m_httpClient.EnqueueResponse(Renewed());
    }
    StartLoop(lock);

    EXPECT_TRUE(m_fatalCount.WaitForAtLeast(1));
    m_loop->Stop();

    EXPECT_EQ(m_fatalCount.Count(), 1);
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
    EXPECT_EQ(m_fatalCount.Count(), 0);
    EXPECT_FALSE(m_loop->IsStopped());
}
namespace {
// A lock whose RenewAsync starts normally except on the Nth call across all instances, where it throws. Started
// renewals complete only when the test releases them, which models a renewal still on the wire after the loop gave up.
struct ScriptedLock {
    struct Shared {
        std::mutex Mutex;
        std::size_t Calls = 0;
        std::size_t ThrowOnCall = 0;
        std::vector<std::function<void()>> Completions;
        std::condition_variable Started;
    };
    std::shared_ptr<Shared> State;

    void RenewAsync(std::function<void(std::exception_ptr, bool)> callback) const {
        std::scoped_lock lock(State->Mutex);
        if (++State->Calls == State->ThrowOnCall) {
            throw std::runtime_error("cannot start renewal");
        }
        State->Completions.emplace_back([callback = std::move(callback)] { callback(nullptr, true); });
        State->Started.notify_all();
    }
    [[nodiscard]] bool IsRenewalOverdue() const { return false; }
    [[nodiscard]] std::optional<std::chrono::steady_clock::duration> RenewalTimeRemaining() const { return Remaining; }
    std::optional<std::chrono::steady_clock::duration> Remaining;
};

struct ScriptedSnapshot {
    std::vector<std::shared_ptr<ScriptedLock>> Locks;
    std::vector<std::shared_ptr<ScriptedLock>> operator()() const { return Locks; }
};

struct ScriptedFatal {
    FatalCounter* Count;
    void operator()() const { Count->Increment(); }
};
} // namespace

TEST(LeaseRenewalLoopThrowTests, ThrowOnTheNthRenewAsyncIsFatalOnceAndLateCompletionsAreHarmless) {
    boost::asio::io_context context;
    auto guard = boost::asio::make_work_guard(context);
    std::jthread thread([&context] { context.run(); });
    auto logger =
        std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::BasicLeaseRenewalLoop<ScriptedSnapshot, ScriptedFatal,
                                                                                    ScriptedLock>::Logger>();

    auto state = std::make_shared<ScriptedLock::Shared>();
    state->ThrowOnCall = 3; // the first two renewals are started, the third throws
    ScriptedSnapshot snapshot;
    for (int i = 0; i < 4; ++i) {
        snapshot.Locks.push_back(std::make_shared<ScriptedLock>(ScriptedLock{state}));
    }
    FatalCounter fatal;

    auto loop = BasicLeaseRenewalLoop<ScriptedSnapshot, ScriptedFatal, ScriptedLock>::Start(
        context.get_executor(), logger, snapshot, ScriptedFatal{&fatal}, std::chrono::milliseconds(5));
    ASSERT_TRUE(fatal.WaitForAtLeast(1));

    // The abandoned round must not keep Stop() waiting, even though two renewals never completed.
    loop->Stop();
    EXPECT_EQ(fatal.Count(), 1);
    EXPECT_TRUE(loop->IsStopped());

    // Completing the renewals that were already started must neither fire onFatal again nor restart the loop.
    std::vector<std::function<void()>> completions;
    {
        std::scoped_lock lock(state->Mutex);
        completions.swap(state->Completions);
    }
    ASSERT_EQ(completions.size(), 2u);
    for (auto& complete : completions) {
        complete();
    }
    guard.reset();
    thread.join();
    EXPECT_EQ(fatal.Count(), 1);
    EXPECT_TRUE(loop->IsStopped());
    EXPECT_EQ(state->Calls, 3u);
}

namespace {
// A lock whose renewals always fail with a transient error, recording when each attempt happened.
struct FailingLock {
    struct Shared {
        std::mutex Mutex;
        std::vector<std::chrono::steady_clock::time_point> Calls;
        std::optional<std::chrono::milliseconds> RetryAfter;
        bool ThrowOnDeadlineQuery = false;
        std::condition_variable Attempted;
    };
    std::shared_ptr<Shared> State;
    std::optional<std::chrono::steady_clock::duration> Remaining;

    void RenewAsync(std::function<void(std::exception_ptr, bool)> callback) const {
        std::optional<std::chrono::milliseconds> hint;
        {
            std::scoped_lock lock(State->Mutex);
            State->Calls.push_back(std::chrono::steady_clock::now());
            hint = State->RetryAfter;
            State->Attempted.notify_all();
        }
        callback(std::make_exception_ptr(RequestFailedException(503, "ServerBusy", "busy", "id",
                                                                std::make_error_code(std::errc::io_error), {}, hint)),
                 false);
    }
    [[nodiscard]] bool IsRenewalOverdue() const { return false; }
    [[nodiscard]] std::optional<std::chrono::steady_clock::duration> RenewalTimeRemaining() const {
        if (State->ThrowOnDeadlineQuery) {
            throw std::runtime_error("deadline unavailable");
        }
        return Remaining;
    }
};

struct FailingSnapshot {
    std::vector<std::shared_ptr<FailingLock>> Locks;
    std::vector<std::shared_ptr<FailingLock>> operator()() const { return Locks; }
};

struct FailingLoopRun {
    std::shared_ptr<FailingLock::Shared> State = std::make_shared<FailingLock::Shared>();
    FatalCounter Fatal;

    // Runs one wake-up whose renewals keep failing, and returns the gaps between the attempts.
    std::vector<std::chrono::milliseconds> Run(const std::optional<std::chrono::steady_clock::duration> remaining,
                                               const std::chrono::milliseconds retryDelay) {
        using Loop = BasicLeaseRenewalLoop<FailingSnapshot, ScriptedFatal, FailingLock>;
        boost::asio::io_context context;
        auto guard = boost::asio::make_work_guard(context);
        std::jthread thread([&context] { context.run(); });
        auto logger = std::make_shared<Loop::Logger>();
        FailingSnapshot snapshot{{std::make_shared<FailingLock>(FailingLock{State, remaining})}};

        auto loop = Loop::Start(context.get_executor(), logger, snapshot, ScriptedFatal{&Fatal},
                                std::chrono::milliseconds(500), retryDelay, 5);
        {
            std::unique_lock lock(State->Mutex);
            State->Attempted.wait_for(lock, std::chrono::seconds(10), [&] { return State->Calls.size() >= 5; });
        }
        loop->Stop();
        guard.reset();
        thread.join();

        std::vector<std::chrono::milliseconds> gaps;
        for (size_t i = 1; i < State->Calls.size(); ++i) {
            gaps.push_back(
                std::chrono::duration_cast<std::chrono::milliseconds>(State->Calls[i] - State->Calls[i - 1]));
        }
        return gaps;
    }
};
} // namespace

TEST(LeaseRenewalLoopBackoffTests, RetryDelayGrowsExponentiallyWithJitter) {
    FailingLoopRun run;
    // The wake-up fires after 10 s, so shorten the test by making the first round start immediately via interval.
    const auto gaps = run.Run(std::nullopt, std::chrono::milliseconds(40));

    ASSERT_GE(gaps.size(), 4u);
    // Gap k lies in [step/2, step] with step = 40 ms * 2^k.
    for (size_t k = 0; k < 4; ++k) {
        const auto step = 40 * (1 << k);
        EXPECT_GE(gaps[k].count(), step / 2 - 5) << "gap " << k;
    }
    EXPECT_GE(gaps[3].count(), 75); // a fixed 40 ms delay would fail this
    EXPECT_EQ(run.Fatal.Count(), 0);
}

TEST(LeaseRenewalLoopBackoffTests, RetryAfterHintRaisesTheDelay) {
    FailingLoopRun run;
    run.State->RetryAfter = std::chrono::milliseconds(150);
    const auto gaps = run.Run(std::nullopt, std::chrono::milliseconds(5));

    ASSERT_GE(gaps.size(), 1u);
    EXPECT_GE(gaps[0].count(), 145);
}

TEST(LeaseRenewalLoopBackoffTests, RetryDelayNeverOutlastsTheRenewalDeadline) {
    FailingLoopRun run;
    const auto gaps = run.Run(std::chrono::milliseconds(10), std::chrono::milliseconds(500));

    ASSERT_GE(gaps.size(), 4u);
    for (const auto& gap : gaps) {
        EXPECT_LT(gap.count(), 450); // loose bound: only the 500 ms deadline must cap an otherwise longer delay
    }
}
TEST(LeaseRenewalLoopBackoffTests, WakeUpAfterRetriesRunOutNeverOutlastsTheRenewalDeadline) {
    using Loop = BasicLeaseRenewalLoop<FailingSnapshot, ScriptedFatal, FailingLock>;
    boost::asio::io_context context;
    auto guard = boost::asio::make_work_guard(context);
    std::jthread thread([&context] { context.run(); });
    auto logger = std::make_shared<Loop::Logger>();
    auto state = std::make_shared<FailingLock::Shared>();
    FatalCounter fatal;
    FailingSnapshot snapshot{{std::make_shared<FailingLock>(FailingLock{state, std::chrono::milliseconds(20)})}};

    // Five attempts per round, then a regular wake-up that must be pulled in to the 20 ms deadline, not 5 s.
    auto loop = Loop::Start(context.get_executor(), logger, snapshot, ScriptedFatal{&fatal},
                            std::chrono::milliseconds(1500), std::chrono::milliseconds(1), 5);
    {
        std::unique_lock lock(state->Mutex);
        EXPECT_TRUE(state->Attempted.wait_for(lock, std::chrono::seconds(8), [&] { return state->Calls.size() >= 6; }));
    }
    loop->Stop();
    guard.reset();
    thread.join();
    ASSERT_GE(state->Calls.size(), 6u);
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(state->Calls[5] - state->Calls[4]).count(), 700);
}

TEST(LeaseRenewalLoopThrowTests, ThrowWhileSchedulingTheRetryIsFatalOnceAndDoesNotBlockStop) {
    using Loop = BasicLeaseRenewalLoop<FailingSnapshot, ScriptedFatal, FailingLock>;
    boost::asio::io_context context;
    auto guard = boost::asio::make_work_guard(context);
    std::jthread thread([&context] { context.run(); });
    auto logger = std::make_shared<Loop::Logger>();
    auto state = std::make_shared<FailingLock::Shared>();
    state->ThrowOnDeadlineQuery = true;
    FatalCounter fatal;
    FailingSnapshot snapshot{{std::make_shared<FailingLock>(FailingLock{state, std::nullopt})}};

    auto loop = Loop::Start(context.get_executor(), logger, snapshot, ScriptedFatal{&fatal},
                            std::chrono::milliseconds(5), std::chrono::milliseconds(1), 5);
    ASSERT_TRUE(fatal.WaitForAtLeast(1));

    // The round was released before fencing, so Stop must return promptly and the host thread must have survived.
    loop->Stop(std::chrono::seconds(10));
    guard.reset();
    thread.join();
    EXPECT_EQ(fatal.Count(), 1);
    EXPECT_TRUE(loop->IsStopped());
    EXPECT_EQ(state->Calls.size(), 1u);
}

TEST(LeaseRenewalLoopThrowTests, StopGivesUpOnARoundThatNeverCompletes) {
    boost::asio::io_context context;
    auto guard = boost::asio::make_work_guard(context);
    std::jthread thread([&context] { context.run(); });
    using Loop = BasicLeaseRenewalLoop<ScriptedSnapshot, ScriptedFatal, ScriptedLock>;
    auto logger = std::make_shared<Loop::Logger>();

    auto state = std::make_shared<ScriptedLock::Shared>();
    ScriptedSnapshot snapshot;
    snapshot.Locks.push_back(std::make_shared<ScriptedLock>(ScriptedLock{state}));
    FatalCounter fatal;

    auto loop =
        Loop::Start(context.get_executor(), logger, snapshot, ScriptedFatal{&fatal}, std::chrono::milliseconds(5));
    {
        std::unique_lock lock(state->Mutex);
        ASSERT_TRUE(
            state->Started.wait_for(lock, std::chrono::seconds(10), [&] { return !state->Completions.empty(); }));
    }

    // The renewal is started but never completed: Stop must return after its timeout instead of hanging.
    loop->Stop(std::chrono::milliseconds(150));
    EXPECT_TRUE(loop->IsStopped());

    guard.reset();
    thread.join();
}