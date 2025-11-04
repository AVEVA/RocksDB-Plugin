#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <azure/storage/blobs.hpp>

#include <cassert>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    LockFileImpl::LockFileImpl(std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> file)
        : m_file(std::move(file))
    {
    }

    bool LockFileImpl::Lock()
    {
        // Do not attempt to lock again when you already have a lock aquired.
        if (m_lease != nullptr)
        {
            return false;
        }

        auto start = std::chrono::high_resolution_clock::now();
        auto end = std::chrono::high_resolution_clock::now();
        std::optional<std::exception> ex;
        while ((end - start) < Configuration::LeaseLength)
        {
            try
            {
                const auto leaseId = ::Azure::Storage::Blobs::BlobLeaseClient::CreateUniqueLeaseId();
                m_lease = std::make_unique<::Azure::Storage::Blobs::BlobLeaseClient>(*m_file, leaseId);
                const auto response = m_lease->Acquire(Configuration::LeaseLength);
                assert(response.Value.LeaseId == leaseId);

                ex.reset();
                break;
            }
            catch (const ::Azure::Storage::StorageException& e)
            {
                ex = e;
            }

            end = std::chrono::high_resolution_clock::now();
        }

        if (ex.has_value())
        {
            try
            {
                if (m_lease != nullptr)
                {
                    m_lease->Release();
                }
            }
            catch (...) {}

            return false;
        }

        return true;
    }

    void LockFileImpl::Renew() const
    {
        m_lease->Renew();
    }

    void LockFileImpl::Unlock()
    {
        m_lease->Release();
        m_lease.reset();
    }
}
