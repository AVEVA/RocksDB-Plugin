#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"

namespace AVEVA::RocksDB::Plugin::Azure
{
    ReadableFile::ReadableFile(Impl::ReadableFileImpl file)
        : m_file(std::move(file))
    {
    }

    rocksdb::IOStatus ReadableFile::Read(const size_t n,
        const rocksdb::IOOptions&,
        rocksdb::Slice* result,
        char* scratch,
        rocksdb::IODebugContext*)
    {
        try
        {
            const auto bytesRead = m_file.SequentialRead(n, scratch);
            *result = rocksdb::Slice(scratch, bytesRead);
            return rocksdb::IOStatus::OK();
        }
        catch (const ::Azure::Core::RequestFailedException& e)
        {
            return AzureErrorTranslator::IOStatusFromError(e.Message, e.StatusCode);
        }
        catch (const std::exception& e)
        {
            return rocksdb::IOStatus::IOError(e.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Failed to Read from file");
        }
    }

    rocksdb::IOStatus ReadableFile::Read(const uint64_t offset,
        const size_t n,
        const rocksdb::IOOptions&,
        rocksdb::Slice* result,
        char* scratch,
        rocksdb::IODebugContext*) const
    {
        try
        {
            const auto bytesRead = m_file.RandomRead(offset, n, scratch);
            *result = rocksdb::Slice(scratch, bytesRead);
            return rocksdb::IOStatus::OK();
        }
        catch (const ::Azure::Core::RequestFailedException& e)
        {
            return AzureErrorTranslator::IOStatusFromError(e.Message, e.StatusCode);
        }
        catch (const std::exception& e)
        {
            return rocksdb::IOStatus::IOError(e.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Failed to Read from file");
        }
    }

    rocksdb::IOStatus ReadableFile::Skip(const uint64_t n)
    {
        try
        {
            m_file.Skip(n);
            return rocksdb::IOStatus::OK();
        }
        catch (const ::Azure::Core::RequestFailedException& e)
        {
            return AzureErrorTranslator::IOStatusFromError(e.Message, e.StatusCode);
        }
        catch (const std::exception& e)
        {
            return rocksdb::IOStatus::IOError(e.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Failed to Read from file");
        }
    }
}
