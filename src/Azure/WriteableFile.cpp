// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"

#include <boost/log/trivial.hpp>
#include <cassert>
#include <limits>
namespace AVEVA::RocksDB::Plugin::Azure
{
    using namespace boost::log::trivial;

    WriteableFile::WriteableFile(Impl::WriteableFileImpl file, std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
        : m_file(std::move(file)),
        m_logger(std::move(logger))
    {
    }

    rocksdb::IOStatus WriteableFile::Append(const rocksdb::Slice& data, const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {            
            m_file.Append(std::span(data.data(), data.size()));
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
            return rocksdb::IOStatus::IOError("Unknown error when appending to file");
        }

        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus WriteableFile::Close(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
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

    rocksdb::IOStatus WriteableFile::Flush(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
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

    rocksdb::IOStatus WriteableFile::Sync(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
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

    uint64_t WriteableFile::GetFileSize(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {
            const auto fileSize = m_file.GetFileSize();
            
            assert(fileSize >= 0 && "File size should not be negative");
            assert(fileSize <= std::numeric_limits<int64_t>::max() && "File size must fit in int64_t");
            
            return static_cast<uint64_t>(fileSize);
        }
        catch (const ::Azure::Core::RequestFailedException& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << "[" << ex.ErrorCode << "]" << " (Status Code: " << static_cast<int>(ex.StatusCode) << ") " << ex.Message;
            return 0;
        }
        catch (const std::exception& ex)
        {
            BOOST_LOG_SEV(*m_logger, error) << ex.what();
            return 0;
        }
        catch (...)
        {
            BOOST_LOG_SEV(*m_logger, error) << "Unknown error when syncing file";
            return 0;
        }
    }
}
