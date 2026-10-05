// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"

#include "FakeHttpPump.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

using AVEVA::HttpResponse;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
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

TEST_F(LockFileImplTests, ForbiddenFailsWithoutRetrying) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(403, "AuthorizationFailure", "denied", "r1"));
    m_httpClient.EnqueueResponse(Acquired());

    auto lock = CreateLock(std::chrono::seconds(20));
    EXPECT_FALSE(lock->Lock());
    EXPECT_EQ(m_httpClient.RequestCount(), 1U);
}

TEST_F(LockFileImplTests, LeaseHeldByAnotherOwnerIsRetried) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "LeaseAlreadyPresent", "held", "r1"));
    m_httpClient.EnqueueResponse(Acquired());

    auto lock = CreateLock(std::chrono::seconds(20));
    EXPECT_TRUE(lock->Lock());
    EXPECT_EQ(m_httpClient.RequestCount(), 2U);
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
