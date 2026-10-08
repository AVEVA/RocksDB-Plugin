// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

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
        std::uint32_t GetResponseHeaderLimit() const noexcept;
        boost::asio::cancellation_slot GetCancellationSlot() const noexcept;
        void SetTimeout(std::chrono::milliseconds timeout) noexcept;
        void SetResponseBodyLimit(std::uint64_t responseBodyLimit) noexcept;
        // Maximum size in bytes of the response status line and headers; larger responses fail with ResponseTooLarge.
        void SetResponseHeaderLimit(std::uint32_t responseHeaderLimit) noexcept;
        // Asio does not synchronize signal emission with slot clearing. The signal must therefore be emitted on
        // the completion executor, or otherwise serialized with the completion of the request.
        void SetCancellationSlot(boost::asio::cancellation_slot cancellationSlot) noexcept;

      private:
        std::chrono::milliseconds m_timeout{30000};
        std::uint64_t m_responseBodyLimit = 8 * 1024 * 1024;
        std::uint32_t m_responseHeaderLimit = 64 * 1024;
        boost::asio::cancellation_slot m_cancellationSlot;
    };
} // namespace AVEVA
