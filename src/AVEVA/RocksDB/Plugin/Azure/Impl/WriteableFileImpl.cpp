#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"

#include <boost/log/trivial.hpp>

#include <algorithm>
using namespace ::Azure::Storage;
using namespace ::Azure::Storage::Blobs;
using namespace ::Azure::Core::IO;
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    WriteableFileImpl::WriteableFileImpl(const std::string_view name,
        const size_t bufferSize,
        std::shared_ptr<::Azure::Storage::Blobs::PageBlobClient> blobClient,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::logger_mt> logger)
        : m_name(name),
        m_bufferSize(bufferSize),
        m_blobClient(std::move(blobClient)),
        m_fileCache(std::move(fileCache)),
        m_logger(std::move(logger)),
        m_lastPageOffset(0),
        m_size(BlobHelpers::GetFileSize(*m_blobClient)),
        m_capacity(BlobHelpers::GetBlobCapacity(*m_blobClient)),
        m_bufferOffset(0),
        m_closed(false)
    {
        if (m_bufferSize < Configuration::PageBlob::DefaultBufferSize)
        {
            m_bufferSize = Configuration::PageBlob::DefaultBufferSize;
        }

        assert(m_bufferSize > 0);
        m_buffer.resize(static_cast<size_t>(m_bufferSize));
        if (m_size > 0) // Existing file with data
        {
            m_lastPageOffset = m_size / Configuration::PageBlob::PageSize;;
            const auto lastPageBytes = m_size % Configuration::PageBlob::PageSize;
            if (lastPageBytes > 0) // There is a partially filled page
            {
                DownloadBlobToOptions opt;
                opt.Range.Emplace(m_lastPageOffset, static_cast<size_t>(lastPageBytes));
                const auto res =
                    m_blobClient->DownloadTo(reinterpret_cast<uint8_t*>(m_buffer.data()), static_cast<size_t>(lastPageBytes), opt);
                assert((res.Value.ContentRange.Length.HasValue()
                    ? res.Value.ContentRange.Length.Value()
                    : 0) == static_cast<int64_t>(lastPageBytes));

                m_bufferOffset = lastPageBytes;
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
        m_closed(std::exchange(other.m_closed, false)),
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
        m_closed = std::exchange(other.m_closed, false);
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

    void WriteableFileImpl::Append(const char* data, size_t size)
    {
        auto dataPos = data;
        while (size > 0)
        {
            auto space = m_bufferSize - m_bufferOffset;
            auto bufPos = &m_buffer[m_bufferOffset];
            const auto numBytes = size > space ? space : size;

            std::copy(dataPos, dataPos + numBytes, bufPos);

            size -= numBytes;
            m_bufferOffset += numBytes;
            dataPos += numBytes;
            m_size += numBytes;
            space -= numBytes;
            if (space < Configuration::PageBlob::PageSize)
            {
                Flush();
            }
        }
    }

    void WriteableFileImpl::Flush()
    {
        if (m_bufferOffset == 0)
        {
            return;
        }

        const auto [remaining, numToWrite] = BlobHelpers::RoundToNearestPage(m_bufferOffset);
        if ((m_size + m_bufferSize) > m_capacity)
        {
            Expand();
        }

        MemoryBodyStream dataStream(reinterpret_cast<uint8_t*>(m_buffer.data()), numToWrite);
        auto resp = m_blobClient->UploadPages(static_cast<int64_t>(m_lastPageOffset), dataStream);
        auto code = resp.RawResponse->GetStatusCode();
        if (remaining != 0)
        {
            const auto residualOffsetBegin = m_bufferOffset - remaining;
            const auto residualOffsetEnd = residualOffsetBegin + remaining;

            // TODO: Can there be overlap here? If so, memmove is the way to go.
            std::copy(m_buffer.data() + residualOffsetBegin, m_buffer.data() + residualOffsetEnd, m_buffer.begin());

            // TODO: Set target offset appropriately for next flush.
        }

        BOOST_LOG_SEV(*m_logger, debug) << "Flushed " << numToWrite << " bytes to writeable file '" << m_name << "'. HttpStatus(" << static_cast<int>(code) << ")";

        m_bufferOffset = remaining;
        m_lastPageOffset = (m_size / Configuration::PageBlob::PageSize) * Configuration::PageBlob::PageSize;
    }

    void WriteableFileImpl::Sync()
    {
        if (m_fileCache)
        {
            m_fileCache->MarkFileAsStaleIfExists(m_name);
        }

        Flush();
        BlobHelpers::SetFileSize(*m_blobClient, m_size);
        BOOST_LOG_SEV(*m_logger, debug) << "Synced writeable file '" << m_name << "' to " << m_size << " bytes";
    }

    void WriteableFileImpl::Truncate(uint64_t size)
    {
        Sync();

        // TODO: This is a workaround. Find a more comprehensive solution.
        if (size == 0)
        {
            m_bufferOffset = 0;
        }

        m_size = size;
        const auto [_, totalPageOffset] = BlobHelpers::RoundToNearestPage(size);
        m_blobClient->Resize(static_cast<int64_t>(totalPageOffset));
        m_capacity = totalPageOffset;
        BlobHelpers::SetFileSize(*m_blobClient, m_size);
    }

    uint64_t WriteableFileImpl::GetFileSize() const noexcept
    {
        return m_size;
    }

    uint64_t WriteableFileImpl::GetUniqueId(char* id, const size_t maxIdSize) const noexcept
    {
        const auto length = std::min(maxIdSize, m_name.size());
        std::copy_n(m_name.begin(), length, id);
        return length;
    }

    void WriteableFileImpl::Expand()
    {
        // TODO: Consider expanding by less for large files.
        const auto [_, rounded] = BlobHelpers::RoundToNearestPage(m_capacity * 2);
        const auto desiredSize = rounded;

        BOOST_LOG_SEV(*m_logger, debug) << "Expanding writeable file '" << m_name << "' to " << desiredSize << " bytes";

        m_blobClient->Resize(static_cast<int64_t>(desiredSize));
        m_capacity = desiredSize;
    }
}
