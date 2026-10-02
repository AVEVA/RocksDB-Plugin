#pragma once

#include <boost/asio/cancellation_signal.hpp>

#include <chrono>
#include <cstdint>

namespace AVEVA
{
    class HttpRequestOptions
    {
      public:
        std::chrono::milliseconds GetTimeout() const noexcept;
        std::uint64_t GetResponseBodyLimit() const noexcept;
        boost::asio::cancellation_slot GetCancellationSlot() const noexcept;
        void SetTimeout(std::chrono::milliseconds timeout) noexcept;
        void SetResponseBodyLimit(std::uint64_t responseBodyLimit) noexcept;
        void SetCancellationSlot(boost::asio::cancellation_slot cancellationSlot) noexcept;

      private:
        std::chrono::milliseconds m_timeout{30000};
        std::uint64_t m_responseBodyLimit = 8 * 1024 * 1024;
        boost::asio::cancellation_slot m_cancellationSlot;
    };
} // namespace AVEVA
