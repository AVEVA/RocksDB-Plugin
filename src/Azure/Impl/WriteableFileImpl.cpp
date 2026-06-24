// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"

#include <boost/log/trivial.hpp>

#include <algorithm>
using namespace ::Azure::Storage;
using namespace ::Azure::Storage::Blobs;
using namespace ::Azure::Core::IO;
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    AVEVA::RocksDB::Plugin::Azure::Impl::WriteableFileImpl::WriteableFileImpl(const std::string_view name,
        std::shared_ptr<Core::BlobClient> blobClient,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        const int64_t bufferSize)
        : m_name(name),
        m_bufferSize(bufferSize),
        m_blobClient(std::move(blobClient)),
        m_fileCache(std::move(fileCache)),
        m_logger(std::move(logger)),
        m_lastPageOffset(0),
        m_size(m_blobClient->GetSize()),
        m_capacity(m_blobClient->GetCapacity()),
        m_bufferOffset(0),
        m_closed(false),
        m_flushed(true)
    {
        if (m_bufferSize < Configuration::PageBlob::PageSize)
        {
            throw std::invalid_argument("Buffer size cannot be smaller than a page");
        }

        assert(m_bufferSize > 0);
        m_buffer.resize(static_cast<size_t>(m_bufferSize));
        if (m_size > 0) // Existing file with data
        {
            int64_t lastPageBytes;
            int64_t lastPageOffset;
            std::tie(lastPageBytes, lastPageOffset) = BlobHelpers::RoundToBeginningOfNearestPage(m_size);
            m_lastPageOffset = lastPageOffset;
            if (lastPageBytes > 0) // There is a partially filled page
            {
                const auto bytesDownoaded = m_blobClient->DownloadTo(m_buffer, m_lastPageOffset, lastPageBytes);
                assert(bytesDownoaded == lastPageBytes);
                m_bufferOffset = lastPageBytes;
                m_flushed = false;  // We have existing partial page data in buffer
            }
        }
    }

    WriteableFileImpl::~WriteableFileImpl()
    {
        for (int i = 0; i < 5; i++)
        {
            if (i > 0)
            {
                BOOST_LOG_SEV(*m_logger, debug) << "Retrying to close file '" << m_name << "'. Attempt " << i << " of 5";
            }
            try
            {
                Close();
                break;
            }
            catch (const std::exception& e)
            {
                BOOST_LOG_SEV(*m_logger, warning) << "Failed to close file '" << m_name << "' on attempt " << i;
                BOOST_LOG_SEV(*m_logger, warning) << "Exception details: " << e.what();
            }
            catch (...)
            {
                BOOST_LOG_SEV(*m_logger, warning) << "Failed to close file '" << m_name << "' on attempt " << i;
            }
        }
    }

    WriteableFileImpl::WriteableFileImpl(WriteableFileImpl&& other) noexcept
        : m_name(std::move(other.m_name)),
        m_bufferSize(other.m_bufferSize),
        m_blobClient(std::move(other.m_blobClient)),
        m_fileCache(std::move(other.m_fileCache)),
        m_logger(std::move(other.m_logger)),
        m_lastPageOffset(other.m_lastPageOffset),
        m_size(other.m_size),
        m_capacity(other.m_capacity),
        m_bufferOffset(other.m_bufferOffset),
        m_closed(std::exchange(other.m_closed, true)),
        m_flushed(other.m_flushed),
        m_buffer(std::move(other.m_buffer))
    {
    }

    WriteableFileImpl& WriteableFileImpl::operator=(WriteableFileImpl&& other) noexcept
    {
        m_name = std::move(other.m_name);
        m_bufferSize = other.m_bufferSize;
        m_blobClient = std::move(other.m_blobClient);
        m_fileCache = std::move(other.m_fileCache);
        m_logger = std::move(other.m_logger);
        m_lastPageOffset = other.m_lastPageOffset;
        m_size = other.m_size;
        m_capacity = other.m_capacity;
        m_bufferOffset = other.m_bufferOffset;
        m_closed = std::exchange(other.m_closed, true);
        m_flushed = other.m_flushed;
        m_buffer = std::move(other.m_buffer);
        return *this;
    }

    void WriteableFileImpl::Close()
    {
        if (!m_closed)
        {
            Sync();
            m_closed = true;
        }
    }

    void WriteableFileImpl::Append(const std::span<const char> data)
    {
        const char* dataPos = data.data();
        auto dataSize = static_cast<int64_t>(data.size());
        while (dataSize > 0)
        {
            const auto spaceLeft = m_bufferSize - m_bufferOffset;
            if (spaceLeft < Configuration::PageBlob::PageSize)
            {
                Flush();
                continue;
            }

            auto bufPos = &m_buffer[static_cast<size_t>(m_bufferOffset)];
            const auto bytesToCopy = std::min(spaceLeft, dataSize);
            std::copy(dataPos, dataPos + bytesToCopy, bufPos);

            dataSize -= bytesToCopy;
            m_bufferOffset += bytesToCopy;
            dataPos += bytesToCopy;
            m_size += bytesToCopy;
            m_flushed = false;  // Mark as not flushed since we added new data
        }
    }

    void WriteableFileImpl::Flush()
    {
        if (m_bufferOffset == 0)
        {
            return;
        }

        // If already flushed and no new data, don't flush again
        if (m_flushed)
        {
            return;
        }

        const auto [remaining, bytesToWrite] = BlobHelpers::RoundToEndOfNearestPage(m_bufferOffset);
        if ((m_lastPageOffset + bytesToWrite) > m_capacity)
        {
            Expand();
        }

        m_blobClient->UploadPages(std::span(m_buffer.begin(), m_buffer.begin() + bytesToWrite), m_lastPageOffset);
        if (remaining != 0)
        {
            const auto residualOffsetBegin = m_bufferOffset - remaining;
            const auto residualOffsetEnd = residualOffsetBegin + remaining;

            // TODO: Can there be overlap here? If so, memmove is the way to go.
            std::copy(m_buffer.data() + residualOffsetBegin, m_buffer.data() + residualOffsetEnd, m_buffer.begin());

            // TODO: Set target offset appropriately for next flush.
        }

        BOOST_LOG_SEV(*m_logger, debug) << "Flushed " << bytesToWrite << " bytes to writeable file '" << m_name << "'.";
        m_bufferOffset = remaining;
        m_lastPageOffset = (m_size / Configuration::PageBlob::PageSize) * Configuration::PageBlob::PageSize;
        m_flushed = true;
    }

    void WriteableFileImpl::Sync()
    {
        if (m_fileCache)
        {
            m_fileCache->MarkFileAsStaleIfExists(m_name);
        }

        Flush();
        m_blobClient->SetSize(m_size);
        BOOST_LOG_SEV(*m_logger, debug) << "Synced writeable file '" << m_name << "' to " << m_size << " bytes";
    }

    void WriteableFileImpl::Truncate(int64_t size)
    {
        // Truncate only allows shrinking, not expanding
        if (size > m_size)
        {
            throw std::invalid_argument("Truncate can only shrink the file. Cannot expand from " +
                std::to_string(m_size) + " to " + std::to_string(size) + " bytes.");
        }

        // Ensure all data is written to blob before modifications are made
        Sync();

        const auto [partialPageSize, totalPageOffset] = BlobHelpers::RoundToBeginningOfNearestPage(size);
        m_bufferOffset = 0;
        m_lastPageOffset = totalPageOffset;
        m_flushed = true;  // Buffer is empty after truncate

        if (partialPageSize != 0)
        {
            // Read the partial page into memory for further appends
            const auto bytesDownloaded = m_blobClient->DownloadTo(m_buffer, totalPageOffset, partialPageSize);
            assert(bytesDownloaded == partialPageSize);
            m_bufferOffset = partialPageSize;
            m_flushed = false;  // We have data in buffer now
        }

        m_size = size;
        m_blobClient->SetSize(m_size);

        // Calculate new capacity rounded up to page size
        const auto [_, newCapacity] = BlobHelpers::RoundToEndOfNearestPage(size);
        m_capacity = newCapacity;
        m_blobClient->SetCapacity(newCapacity);
    }

    int64_t WriteableFileImpl::GetFileSize() const noexcept
    {
        return m_size;
    }

    int64_t WriteableFileImpl::GetUniqueId(char* id, const int64_t maxIdSize) const noexcept
    {
        const auto length = std::min(static_cast<int64_t>(m_name.size()), maxIdSize);
        std::copy_n(m_name.begin(), length, id);
        return length;
    }

    void WriteableFileImpl::Expand()
    {
        // TODO: Consider expanding by less for large files.
        const auto [_, rounded] = BlobHelpers::RoundToEndOfNearestPage(m_capacity * 2);
        const auto desiredSize = rounded;

        BOOST_LOG_SEV(*m_logger, debug) << "Expanding writeable file '" << m_name << "' to " << desiredSize << " bytes";

        m_blobClient->SetCapacity(desiredSize);
        m_capacity = desiredSize;
    }
}
