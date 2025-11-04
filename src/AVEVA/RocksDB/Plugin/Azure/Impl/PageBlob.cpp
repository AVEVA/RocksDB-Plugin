#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    PageBlob::PageBlob(::Azure::Storage::Blobs::PageBlobClient client)
        : m_client(std::move(client))
    {
    }

    uint64_t PageBlob::GetSize()
    {
        return BlobHelpers::GetFileSize(m_client);
    }

    void PageBlob::DownloadTo(const std::string& path, uint64_t offset, uint64_t length)
    {
        ::Azure::Storage::Blobs::DownloadBlobToOptions options;
        options.Range = ::Azure::Core::Http::HttpRange(static_cast<int64_t>(offset), static_cast<int64_t>(length));
        m_client.DownloadTo(path, options);
    }
}
