#include "AVEVA/HttpClient/HttpRequestOptions.hpp"

namespace AVEVA
{
    std::chrono::milliseconds HttpRequestOptions::GetTimeout() const noexcept
    {
        return m_timeout;
    }

    std::uint64_t HttpRequestOptions::GetResponseBodyLimit() const noexcept
    {
        return m_responseBodyLimit;
    }

    boost::asio::cancellation_slot HttpRequestOptions::GetCancellationSlot() const noexcept
    {
        return m_cancellationSlot;
    }

    void HttpRequestOptions::SetTimeout(std::chrono::milliseconds timeout) noexcept
    {
        m_timeout = timeout;
    }

    void HttpRequestOptions::SetResponseBodyLimit(std::uint64_t responseBodyLimit) noexcept
    {
        m_responseBodyLimit = responseBodyLimit;
    }

    void HttpRequestOptions::SetCancellationSlot(boost::asio::cancellation_slot cancellationSlot) noexcept
    {
        m_cancellationSlot = cancellationSlot;
    }
} // namespace AVEVA
