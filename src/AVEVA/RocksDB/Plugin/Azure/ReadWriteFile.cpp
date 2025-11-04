#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include <boost/log/trivial.hpp>
namespace AVEVA::RocksDB::Plugin::Azure
{
    using namespace boost::log::trivial;
    ReadWriteFile::ReadWriteFile(Impl::ReadWriteFileImpl file, std::shared_ptr<boost::log::sources::logger_mt> logger)
        : m_file(std::move(file)),
        m_logger(std::move(logger))
    {
    }

    rocksdb::IOStatus ReadWriteFile::Write(uint64_t offset, const rocksdb::Slice& data, const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {
            m_file.Write(offset, data.data(), data.size());
        }
        catch (const ::Azure::Core::RequestFailedException& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << "[" << ex.ErrorCode << "]" << " (Status Code: " << static_cast<int>(ex.StatusCode) << ") " << ex.Message;
            return AzureErrorTranslator::IOStatusFromError(ex.Message, ex.StatusCode);
        }
        catch (const std::exception& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << ex.what();
            return rocksdb::IOStatus::IOError(ex.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Unknown error when writing to file");
        }
        
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus ReadWriteFile::Read(uint64_t offset, size_t n, const rocksdb::IOOptions&, rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext*) const
    {
        try
        {
            const auto bytesRead = m_file.Read(offset, n, scratch);
            result->data_ = scratch;
            result->size_ = bytesRead;
        }
        catch (const ::Azure::Core::RequestFailedException& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << "[" << ex.ErrorCode << "]" << " (Status Code: " << static_cast<int>(ex.StatusCode) << ") " << ex.Message;
            return AzureErrorTranslator::IOStatusFromError(ex.Message, ex.StatusCode);
        }
        catch (const std::exception& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << ex.what();
            return rocksdb::IOStatus::IOError(ex.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Unknown error when reading file");
        }

        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus ReadWriteFile::Flush(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {
            m_file.Flush();
        }
        catch (const ::Azure::Core::RequestFailedException& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << "[" << ex.ErrorCode << "]" << " (Status Code: " << static_cast<int>(ex.StatusCode) << ") " << ex.Message;
            return AzureErrorTranslator::IOStatusFromError(ex.Message, ex.StatusCode);
        }
        catch (const std::exception& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << ex.what();
            return rocksdb::IOStatus::IOError(ex.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Unknown error when flushing file");
        }

        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus ReadWriteFile::Sync(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {
            m_file.Sync();
        }
        catch (const ::Azure::Core::RequestFailedException& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << "[" << ex.ErrorCode << "]" << " (Status Code: " << static_cast<int>(ex.StatusCode) << ") " << ex.Message;
            return AzureErrorTranslator::IOStatusFromError(ex.Message, ex.StatusCode);
        }
        catch (const std::exception& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << ex.what();
            return rocksdb::IOStatus::IOError(ex.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Unknown error when syncing file");
        }

        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus ReadWriteFile::Close(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {
            m_file.Close();
        }
        catch (const ::Azure::Core::RequestFailedException& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << "[" << ex.ErrorCode << "]" << " (Status Code: " << static_cast<int>(ex.StatusCode) << ") " << ex.Message;
            return AzureErrorTranslator::IOStatusFromError(ex.Message, ex.StatusCode);
        }
        catch (const std::exception& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << ex.what();
            return rocksdb::IOStatus::IOError(ex.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Unknown error when closing file");
        }

        return rocksdb::IOStatus::OK();
    }
}
