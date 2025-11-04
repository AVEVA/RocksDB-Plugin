#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    LoggerImpl::LoggerImpl(std::unique_ptr<WriteableFileImpl> file, int logLevel)
        : m_file(std::move(file)),
        m_logLevel(logLevel),
        m_buffer(4096)
    {
    }

    void LoggerImpl::Logv(const int logLevel, const char* format, ...)
    {
        va_list va;
        va_start(va, format);
        Logv(logLevel, format, va);
        va_end(va);
    }

    void LoggerImpl::Logv(const int logLevel, const char* format, va_list ap)
    {
        if (logLevel < m_logLevel)
        {
            return;
        }

        // RFC 3339 format UTC time
        // See: https://en.cppreference.com/w/cpp/chrono/c/strftime
        const auto now = std::time(nullptr);
        std::tm tm = {};
#ifdef _WIN32
        gmtime_s(&tm, &now);
#else
        gmtime_r(&now, &tm);
#endif

        const auto offset = strftime(m_buffer.data(), m_buffer.size(), "%Y-%m-%dT%H:%M:%SZ", &tm);

        // Copy va_list because vsnprintf may consume it
        va_list apCopy;
        va_copy(apCopy, ap);

        const int result = vsnprintf(m_buffer.data() + offset, m_buffer.size() - offset, format, apCopy);
        va_end(apCopy);
        if (result < 0)
        {
            throw std::runtime_error("Unable to format log message");
        }

        const auto totalBufferOffset = static_cast<size_t>(result) + offset;
        if (totalBufferOffset > m_buffer.size())
        {
            m_buffer.resize(totalBufferOffset);
            const int newResult = vsnprintf(m_buffer.data() + offset, m_buffer.size() - offset, format, ap);
            if (newResult < 0)
            {
                throw std::runtime_error("Unable to format log message");
            }
        }

        m_file->Append(m_buffer.data(), totalBufferOffset);
        va_end(ap);
    }

    void LoggerImpl::Flush()
    {
        m_file->Sync();
    }
}
