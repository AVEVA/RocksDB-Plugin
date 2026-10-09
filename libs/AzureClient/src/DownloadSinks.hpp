// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace AVEVA::AzureClient::Private
{
    // Destination of the downloaded bytes, always written in offset order.
    class DownloadSink
    {
      public:
        DownloadSink() = default;
        DownloadSink(const DownloadSink&) = delete;
        DownloadSink& operator=(const DownloadSink&) = delete;
        DownloadSink(DownloadSink&&) = delete;
        DownloadSink& operator=(DownloadSink&&) = delete;
        virtual ~DownloadSink() = default;

        virtual void Reserve(std::uint64_t /*size*/)
        {
        }

        // True when Write is cheap and never blocks, so it can run on the I/O thread.
        [[nodiscard]] virtual bool WritesInline() const noexcept
        {
            return false;
        }

        [[nodiscard]] virtual bool Write(std::string data) = 0;
    };

    class StreamSink final : public DownloadSink
    {
      public:
        explicit StreamSink(std::ostream& stream) : m_stream(stream)
        {
        }

        [[nodiscard]] bool Write(std::string data) override
        {
            m_stream.write(data.data(), static_cast<std::streamsize>(data.size()));
            return m_stream.good();
        }

      private:
        std::ostream& m_stream;
    };

    class FileSink final : public DownloadSink
    {
      public:
        explicit FileSink(std::shared_ptr<std::ofstream> file) : m_file(std::move(file))
        {
        }

        [[nodiscard]] bool Write(std::string data) override
        {
            m_file->write(data.data(), static_cast<std::streamsize>(data.size()));
            return m_file->good();
        }

      private:
        std::shared_ptr<std::ofstream> m_file;
    };

    // Copies into caller-owned memory, so a download into a fixed buffer needs no intermediate string. A blob
    // range larger than the buffer fails the download instead of writing past the end.
    class SpanSink final : public DownloadSink
    {
      public:
        explicit SpanSink(std::span<char> destination) : m_destination(destination)
        {
        }

        void Reserve(std::uint64_t size) override
        {
            if (size > m_destination.size())
            {
                throw std::length_error("The download is larger than the destination buffer.");
            }
        }

        [[nodiscard]] bool WritesInline() const noexcept override
        {
            return true;
        }

        [[nodiscard]] bool Write(std::string data) override
        {
            if (data.size() > m_destination.size() - m_position)
            {
                return false;
            }
            std::memcpy(m_destination.data() + m_position, data.data(), data.size());
            m_position += data.size();
            return true;
        }

      private:
        std::span<char> m_destination;
        std::size_t m_position = 0;
    };

    class StringSink final : public DownloadSink
    {
      public:
        void Reserve(std::uint64_t size) override
        {
            if (size > m_data.max_size())
            {
                throw std::length_error("Blob is too large to download into memory.");
            }
            m_data.reserve(static_cast<std::size_t>(size));
        }

        [[nodiscard]] bool WritesInline() const noexcept override
        {
            return true;
        }

        [[nodiscard]] bool Write(std::string data) override
        {
            if (m_data.empty() && m_data.capacity() < data.size())
            {
                m_data = std::move(data);
            }
            else
            {
                m_data.append(data);
            }
            return true;
        }

        [[nodiscard]] std::string Take() noexcept
        {
            return std::move(m_data);
        }

      private:
        std::string m_data;
    };
} // namespace AVEVA::AzureClient::Private
