#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"

#include <boost/log/trivial.hpp>

#include <algorithm>
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    ReadWriteFileImpl::ReadWriteFileImpl(std::string_view name,
        std::shared_ptr<Core::BlobClient> blobClient,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::logger_mt> logger)
        : m_name(name),
        m_blobClient(std::move(blobClient)),
        m_fileCache(std::move(fileCache)),
        m_logger(std::move(logger)),
        m_size(static_cast<int64_t>(m_blobClient->GetSize())),
        m_syncSize(m_size),
        m_capacity(static_cast<int64_t>(m_blobClient->GetCapacity())),
        m_closed(false),
        m_buffer(Configuration::PageBlob::DefaultBufferSize)
    {
    }

    ReadWriteFileImpl::~ReadWriteFileImpl()
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
        }
    }

    ReadWriteFileImpl::ReadWriteFileImpl(ReadWriteFileImpl&& other) noexcept
        : m_name(std::move(other.m_name)),
        m_blobClient(std::move(other.m_blobClient)),
        m_fileCache(std::move(other.m_fileCache)),
        m_logger(std::move(other.m_logger)),
        m_size(other.m_size),
        m_syncSize(other.m_syncSize),
        m_capacity(other.m_capacity),
        m_closed(std::exchange(other.m_closed, true)),
        m_buffer(std::move(other.m_buffer)),
        m_bufferStats(std::move(other.m_bufferStats))
    {
    }

    ReadWriteFileImpl& ReadWriteFileImpl::operator=(ReadWriteFileImpl&& other) noexcept
    {
        m_name = std::move(other.m_name);
        m_blobClient = std::move(other.m_blobClient);
        m_fileCache = std::move(other.m_fileCache);
        m_logger = std::move(other.m_logger);
        m_size = other.m_size;
        m_syncSize = other.m_syncSize;
        m_capacity = other.m_capacity;
        m_closed = std::exchange(other.m_closed, true);
        m_buffer = std::move(other.m_buffer);
        m_bufferStats = std::move(other.m_bufferStats);
        return *this;
    }

    void ReadWriteFileImpl::Close()
    {
        if (!m_closed)
        {
            Sync();
            m_closed = true;
        }
    }

    void ReadWriteFileImpl::Sync()
    {
        if (m_fileCache)
        {
            m_fileCache->MarkFileAsStaleIfExists(m_name);
        }

        Flush();

        m_blobClient->SetSize(m_size);
        m_syncSize = m_size;
        BOOST_LOG_SEV(*m_logger, debug) << "Synced read/writeable file '" << m_name << "' to " << m_size << " bytes";
    }

    void ReadWriteFileImpl::Flush()
    {
        m_capacity = static_cast<int64_t>(m_blobClient->GetCapacity());
        auto roundSize = m_size + static_cast<int64_t>(m_buffer.size());
        if (m_size % Configuration::PageBlob::PageSize != 0)
        {
            roundSize += Configuration::PageBlob::PageSize;
        }

        // Pre-calculate the maximum size we'll need and expand if necessary
        int64_t maxSizeNeeded = m_size;
        for (const auto& chunk : m_bufferStats)
        {
            const auto chunkEnd = chunk.targetOffset + chunk.dataLength;
            if (chunkEnd > maxSizeNeeded)
            {
                maxSizeNeeded = chunkEnd;
            }
        }

        // Expand capacity if needed BEFORE processing chunks
        while ((maxSizeNeeded + Configuration::PageBlob::PageSize) > m_capacity)
        {
            Expand();
        }

        // Flush has to happen per chunk in the buffer and fetch any partial pages
        // to merge in existing data from the underlying page blob improvements could include
        // checking for all partial pages and looking more globally through the buffer
        // Also could reduce the allocation necessary through some initial padding
        for (const auto& chunk : m_bufferStats)
        {
            assert(chunk.targetOffset >= chunk.prePadding && "Target Offset is smaller than Pre-padding");

            const auto targetStart = chunk.targetOffset - chunk.prePadding;
            assert(targetStart % Configuration::PageBlob::PageSize == 0 && "TargetStart should be page aligned");

            if (chunk.prePadding > 0)
            {
                assert(chunk.prePadding < Configuration::PageBlob::PageSize && "Pre-padding should not exceed page size");

                // read in the padding bits from first page
                std::span<char> prePaddingBuffer(m_buffer.data() + chunk.bufferOffset, static_cast<size_t>(chunk.prePadding));
                m_blobClient->DownloadTo(prePaddingBuffer, targetStart, chunk.prePadding);
            }
            if (chunk.postPadding > 0)
            {
                auto targetEnd = chunk.targetOffset + chunk.dataLength;
                assert((targetEnd + chunk.postPadding) % Configuration::PageBlob::PageSize == 0 && "TargetEnd should be page aligned");
                assert((targetEnd + chunk.postPadding) <= m_capacity && "We shouldn't try to read data that isn't at least reserved");

                // NOTE: If the data we're writing is APPENDED at the end, this post padding may be trash data and can be skipped.
                if (!(targetEnd > m_size))
                {
                    assert(chunk.postPadding < Configuration::PageBlob::PageSize && "PostPadding shouldn't be greater than a page's size");

                    // read in the padding bits from last page
                    std::span<char> postPaddingBuffer(&m_buffer[chunk.bufferOffset + chunk.prePadding + chunk.dataLength], static_cast<size_t>(chunk.postPadding));
                    m_blobClient->DownloadTo(postPaddingBuffer, targetEnd, chunk.postPadding);
                }
            }

            if ((chunk.targetOffset + chunk.dataLength) > m_size)
            {
                m_size = chunk.targetOffset + chunk.dataLength;
            }

            std::span<char> uploadBuffer(&m_buffer[chunk.bufferOffset], static_cast<size_t>(chunk.ChunkSize()));
            m_blobClient->UploadPages(uploadBuffer, targetStart);

            BOOST_LOG_SEV(*m_logger, debug) << "Flushed " << chunk.ChunkSize() << " bytes to read/writeable file '" << m_name << "'";
        }

        m_bufferStats.clear();

#if _DEBUG
        // NOTE: This is to ensure predictable output in tests.
        std::fill(m_buffer.begin(), m_buffer.end(), '\0');
#endif // _DEBUG
    }

    void ReadWriteFileImpl::Write(int64_t offset, const char* data, int64_t size)
    {
        // This one is TRICKY. Incomplete pages must track the location within a page
        // along with length information and then fetch the relevant page before committing
        // likely to be lots of room for optimization maybe around allocation
        auto* dataPos = data;
        auto targetOffset = offset;
        auto bufferOffset = m_bufferStats.empty()
            ? 0LL
            : m_bufferStats.back().bufferOffset + m_bufferStats.back().ChunkSize();
        while (size > 0)
        {
            const auto space = static_cast<int64_t>(m_buffer.size()) - bufferOffset;
            auto numBytes = std::min(size, space);

            // calculate the relevant padding to leave space to merge
            // existing page data when flushing
            const auto startPaddingFirstPage = targetOffset % Configuration::PageBlob::PageSize;

            int64_t endPaddingLastPage;
            const auto dataEndPos = (targetOffset + numBytes) % Configuration::PageBlob::PageSize;
            if (dataEndPos == 0)
            {
                endPaddingLastPage = 0;
            }
            else
            {
                endPaddingLastPage = Configuration::PageBlob::PageSize - dataEndPos;
            }


            const auto totalBytesNeeded = startPaddingFirstPage + numBytes + endPaddingLastPage;
            assert(totalBytesNeeded % Configuration::PageBlob::PageSize == 0 && "totalBytesNeeded should be page aligned");

            if (totalBytesNeeded > space)
            {
                numBytes -= (Configuration::PageBlob::PageSize - endPaddingLastPage);
                endPaddingLastPage = 0;
            }

            assert((bufferOffset + startPaddingFirstPage + numBytes) <= static_cast<int64_t>(m_buffer.size()) && "calculated total offset must be less than buffer size");
            auto* bufPos = m_buffer.data() + bufferOffset + startPaddingFirstPage;
            std::copy(dataPos, dataPos + numBytes, bufPos);

            size -= numBytes;
            BufferChunkInfo info(bufferOffset, targetOffset, numBytes, startPaddingFirstPage, endPaddingLastPage);
            m_bufferStats.push_back(info);
            targetOffset += numBytes;
            dataPos += numBytes;
            bufferOffset += info.ChunkSize();

            if (bufferOffset >= (Configuration::PageBlob::DefaultBufferSize - Configuration::PageBlob::PageSize))
            {
                Flush();
                bufferOffset = 0;
            }
        }
    }

    int64_t ReadWriteFileImpl::Read(int64_t offset, int64_t bytesRequested, char* buffer) const
    {
        if (offset >= m_syncSize)
        {
            return 0;
        }

        if (m_fileCache)
        {
            const auto bytesRead = m_fileCache->ReadFile(m_name, static_cast<size_t>(offset), static_cast<size_t>(bytesRequested), buffer);
            if (bytesRead)
            {
                return static_cast<int64_t>(*bytesRead);
            }
        }

        const auto bytesCanRead = m_syncSize - offset;
        if (bytesCanRead < bytesRequested) bytesRequested = bytesCanRead;

        std::span<char> readBuffer(buffer, static_cast<size_t>(bytesRequested));
        return m_blobClient->DownloadTo(readBuffer, offset, bytesRequested);
    }

    void ReadWriteFileImpl::Expand()
    {
        const auto [_, rounded] = BlobHelpers::RoundToEndOfNearestPage((m_size + m_capacity) * 2);
        const auto desiredSize = rounded;
        m_blobClient->SetCapacity(desiredSize);
        m_capacity = desiredSize;

        BOOST_LOG_SEV(*m_logger, debug) << "Expanding read/writeable file '" << m_name << "' to " << desiredSize << " bytes";
    }
}
