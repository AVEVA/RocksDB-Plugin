#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"

#include <azure/core/exception.hpp>
#include <cassert>
#include <limits>

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
            assert(n <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
                "size_t value exceeds int64_t max value");
            const auto bytesRead = m_file.SequentialRead(static_cast<int64_t>(n), scratch);
            assert(bytesRead >= 0 && "SequentialRead should not return negative values");
            assert(static_cast<size_t>(bytesRead) <= std::numeric_limits<size_t>::max() &&
                "bytesRead exceeds size_t max value");
            *result = rocksdb::Slice(scratch, static_cast<size_t>(bytesRead));
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
            assert(offset <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
                "offset exceeds int64_t max value");
            assert(n <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
                "size_t value exceeds int64_t max value");
            const auto bytesRead = m_file.RandomRead(static_cast<int64_t>(offset), static_cast<int64_t>(n), scratch);
            assert(bytesRead >= 0 && "RandomRead should not return negative values");
            assert(static_cast<uint64_t>(bytesRead) <= std::numeric_limits<size_t>::max() &&
                "bytesRead exceeds size_t max value");
            *result = rocksdb::Slice(scratch, static_cast<size_t>(bytesRead));
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
            assert(n <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
                "skip value exceeds int64_t max value");
            m_file.Skip(static_cast<int64_t>(n));
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
