#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include <cassert>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    ReadableFileImpl::ReadableFileImpl(std::string_view name,
        std::shared_ptr<Core::BlobClient> blobClient,
        std::shared_ptr<Core::FileCache> fileCache)
        : m_name(name),
        m_blobClient(std::move(blobClient)),
        m_fileCache(std::move(fileCache)),
        m_offset(0),
        m_size(m_blobClient ? m_blobClient->GetSize() : 0)
    {
    }

    uint64_t ReadableFileImpl::SequentialRead(const size_t bytesToRead, char* buffer)
    {
        if (m_fileCache)
        {
            const auto bytesRead = m_fileCache->ReadFile(m_name, m_offset, bytesToRead, buffer);
            if (bytesRead)
            {
                m_offset += *bytesRead;
                return *bytesRead;
            }
        }

        uint64_t bytesRead = 0;
        assert(m_size >= m_offset && "m_size needs to be bigger than m_offset or else we will overflow");
        size_t bytesRequested = m_size - m_offset;
        if (bytesRequested > bytesToRead) bytesRequested = bytesToRead;
        if (bytesRequested <= 0)
        {
            return 0;
        }

        const auto result = m_blobClient->DownloadTo(std::span<char>(buffer, bytesRequested), m_offset, bytesRequested);
        bytesRead = result > 0 ? static_cast<uint64_t>(result) : 0;

        m_offset += bytesRead;
        return bytesRead;
    }

    uint64_t ReadableFileImpl::RandomRead(const uint64_t offset, const size_t bytesToRead, char* buffer) const
    {
        if (m_fileCache)
        {
            const auto bytesRead = m_fileCache->ReadFile(m_name, offset, bytesToRead, buffer);
            if (bytesRead)
            {
                return *bytesRead;
            }
        }

        uint64_t bytesRead = 0;

        assert(m_size > offset && "m_size needs to be bigger than offset or else we will overflow");
        size_t bytesRequested = m_size - offset;
        if (bytesRequested > bytesToRead) bytesRequested = bytesToRead;
        if (bytesRequested <= 0)
        {
            return 0;
        }

        const auto result = m_blobClient->DownloadTo(std::span<char>(buffer, bytesRequested), offset, bytesRequested);
        bytesRead = result > 0 ? static_cast<uint64_t>(result) : 0;

        return bytesRead;
    }

    uint64_t ReadableFileImpl::GetOffset() const
    {
        return m_offset;
    }

    void ReadableFileImpl::Skip(const uint64_t n)
    {
        m_offset += n;
    }

    uint64_t ReadableFileImpl::GetSize() const
    {
        return m_size;
    }
}
