// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include <azure/core/exception.hpp>
#include <boost/log/trivial.hpp>
#include <cassert>
#include <limits>
namespace AVEVA::RocksDB::Plugin::Azure
{
    using namespace boost::log::trivial;
    ReadWriteFile::ReadWriteFile(Impl::ReadWriteFileImpl file, std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
        : m_file(std::move(file)),
        m_logger(std::move(logger))
    {
    }

    rocksdb::IOStatus ReadWriteFile::Write(uint64_t offset, const rocksdb::Slice& data, const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {
            assert(offset <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) && "Offset must fit in int64_t");
            assert(data.size() <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) && "Data size must fit in int64_t");

            m_file.Write(static_cast<int64_t>(offset), data.data(), static_cast<int64_t>(data.size()));
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
            assert(offset <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) && "Offset must fit in int64_t");
            assert(n <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) && "Bytes requested must fit in int64_t");

            const auto bytesRead = m_file.Read(static_cast<int64_t>(offset), static_cast<int64_t>(n), scratch);

            assert(bytesRead >= 0 && "Bytes read should not be negative");
            assert(bytesRead <= static_cast<int64_t>(n) && "Bytes read should not exceed requested amount");

            result->data_ = scratch;
            result->size_ = static_cast<size_t>(bytesRead);
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
