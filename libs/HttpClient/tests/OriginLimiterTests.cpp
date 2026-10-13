// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/OriginLimiter.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <system_error>
#include <vector>

namespace {
struct Recorder {
    std::vector<int> Started;
    std::vector<int> Aborted;
    std::vector<std::error_code> Errors;
};

class RecordingSink {
  public:
    explicit RecordingSink(std::shared_ptr<Recorder> recorder) : m_recorder(std::move(recorder)) {}

    void Start(int&& pending) const { m_recorder->Started.push_back(pending); }
    void Abort(int&& pending, std::error_code error) const {
        m_recorder->Aborted.push_back(pending);
        m_recorder->Errors.push_back(error);
    }

  private:
    std::shared_ptr<Recorder> m_recorder;
};

using Limiter = AVEVA::Private::OriginLimiter<int, RecordingSink>;
} // namespace

TEST(OriginLimiterTests, RunAfterCloseAbortsInsteadOfQueueingForever) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    limiter.Close();
    limiter.Run("origin", limiter.NextTicket(), 1);

    EXPECT_TRUE(recorder->Started.empty());
    ASSERT_EQ(recorder->Aborted.size(), 1U);
    EXPECT_EQ(recorder->Errors.front(), std::make_error_code(std::errc::operation_canceled));
}

TEST(OriginLimiterTests, CloseAbortsQueuedWaitersAndLeavesRunningOnesAlone) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    limiter.Run("origin", limiter.NextTicket(), 1);
    limiter.Run("origin", limiter.NextTicket(), 2);
    limiter.Close();
    limiter.Release("origin");

    EXPECT_EQ(recorder->Started, std::vector<int>{1});
    EXPECT_EQ(recorder->Aborted, std::vector<int>{2});
}
