// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Directory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/LockFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"

#include <boost/log/trivial.hpp>
namespace AVEVA::RocksDB::Plugin::Azure
{
    using namespace boost::log::trivial;
    BlobFilesystem::BlobFilesystem(std::shared_ptr<rocksdb::FileSystem> rocksdbFs, std::unique_ptr<Impl::BlobFilesystemImpl> filesystem, std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
        : rocksdb::FileSystemWrapper(std::move(rocksdbFs)),
        m_filesystem(std::move(filesystem)),
        m_logger(std::move(logger))
    {
    }

    BlobFilesystem::~BlobFilesystem()
    {
        for (auto lock : m_lockFiles)
        {
            m_filesystem->UnlockFile(lock->GetImpl());
            delete lock;
        }
    }

    const char* BlobFilesystem::Name() const
    {
        return "AzureBlobFileSystem";
    }

    rocksdb::IOStatus BlobFilesystem::NewSequentialFile(const std::string& f,
        const rocksdb::FileOptions&,
        std::unique_ptr<rocksdb::FSSequentialFile>* r,
        rocksdb::IODebugContext*)
    {
        try
        {
            *r = std::unique_ptr<rocksdb::FSSequentialFile>(new ReadableFile(m_filesystem->CreateReadableFile(f)));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when creating NewSequentialFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::NewRandomAccessFile(const std::string& f,
        const rocksdb::FileOptions&,
        std::unique_ptr<rocksdb::FSRandomAccessFile>* r,
        rocksdb::IODebugContext*)
    {
        try
        {
            *r = std::unique_ptr<rocksdb::FSRandomAccessFile>(new ReadableFile(m_filesystem->CreateReadableFile(f)));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when creating NewRandomAccessFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::NewWritableFile(const std::string& f,
        const rocksdb::FileOptions&,
        std::unique_ptr<rocksdb::FSWritableFile>* r,
        rocksdb::IODebugContext*)
    {
        try
        {
            *r = std::unique_ptr<rocksdb::FSWritableFile>(new WriteableFile(m_filesystem->CreateWriteableFile(f), m_logger));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when creating NewWritableFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::ReopenWritableFile(const std::string& fname,
        const rocksdb::FileOptions&,
        std::unique_ptr<rocksdb::FSWritableFile>* result,
        rocksdb::IODebugContext*)
    {
        try
        {
            *result = std::unique_ptr<rocksdb::FSWritableFile>(new WriteableFile(m_filesystem->ReopenWriteableFile(fname), m_logger));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling ReopenWritableFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::ReuseWritableFile(const std::string& fname,
        const std::string&,
        const rocksdb::FileOptions&,
        std::unique_ptr<rocksdb::FSWritableFile>* r,
        rocksdb::IODebugContext*)
    {
        try
        {
            *r = std::unique_ptr<rocksdb::FSWritableFile>(new WriteableFile(m_filesystem->ReuseWritableFile(fname), m_logger));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling ReuseWritableFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::NewRandomRWFile(const std::string& fname,
        const rocksdb::FileOptions&,
        std::unique_ptr<rocksdb::FSRandomRWFile>* result,
        rocksdb::IODebugContext*)
    {
        try
        {
            *result = std::unique_ptr<rocksdb::FSRandomRWFile>(new ReadWriteFile(m_filesystem->CreateReadWriteFile(fname), m_logger));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling NewRandomRWFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::NewMemoryMappedFileBuffer(const std::string&,
        std::unique_ptr<rocksdb::MemoryMappedFileBuffer>*)
    {
        return rocksdb::IOStatus::NotSupported();
    }

    rocksdb::IOStatus BlobFilesystem::NewDirectory(const std::string& name,
        const rocksdb::IOOptions&,
        std::unique_ptr<rocksdb::FSDirectory>* result,
        rocksdb::IODebugContext*)
    {
        try
        {
            *result = std::unique_ptr<rocksdb::FSDirectory>(new Directory(m_filesystem->CreateDirectory(name)));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when creating NewDirectory");
        }
    }

    rocksdb::IOStatus BlobFilesystem::FileExists(const std::string& f,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        try
        {
            if (m_filesystem->FileExists(f))
            {
                return rocksdb::IOStatus::OK();
            }
            else
            {
                return rocksdb::IOStatus::NotFound();
            }
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
            return rocksdb::IOStatus::IOError("Unknown error when calling FileExists");
        }
    }

    rocksdb::IOStatus BlobFilesystem::GetChildren(const std::string& dir,
        const rocksdb::IOOptions&,
        std::vector<std::string>* r,
        rocksdb::IODebugContext*)
    {
        try
        {
            *r = m_filesystem->GetChildren(dir);
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling GetChildren");
        }
    }

    rocksdb::IOStatus BlobFilesystem::GetChildrenFileAttributes(const std::string& dir,
        const rocksdb::IOOptions&,
        std::vector<rocksdb::FileAttributes>* result,
        rocksdb::IODebugContext*)
    {
        try
        {
            const auto attributes = m_filesystem->GetChildrenFileAttributes(dir);
            for (const auto& attr : attributes)
            {
                result->push_back({
                    .name = attr.GetName(),
                    .size_bytes = attr.GetSize(),
                    });
            }

            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling GetChildrenFileAttributes");
        }
    }

    rocksdb::IOStatus BlobFilesystem::DeleteFile(const std::string& f,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        try
        {
            if (m_filesystem->DeleteFile(f))
            {
                return rocksdb::IOStatus::OK();
            }
            else
            {
                return rocksdb::IOStatus::NotFound();
            }
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
            return rocksdb::IOStatus::IOError("Unknown error when calling DeleteFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::Truncate(const std::string& fname,
        size_t size,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        try
        {
            assert(size < static_cast<size_t>(std::numeric_limits<int64_t>::max()));
            m_filesystem->Truncate(fname, static_cast<int64_t>(size));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling Truncate");
        }
    }

    rocksdb::IOStatus BlobFilesystem::CreateDir(const std::string&,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus BlobFilesystem::CreateDirIfMissing(const std::string&,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus BlobFilesystem::DeleteDir(const std::string& d,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        try
        {
            const auto remainingFiles = m_filesystem->DeleteDir(d);
            if (remainingFiles == 0)
            {
                return rocksdb::IOStatus::OK();
            }
            else
            {
                BOOST_LOG_SEV(*m_logger, error) << "Failed to delete all contents within directory. " << remainingFiles << " remaining.";
                return rocksdb::IOStatus::IOError("Failed to delete all contents within directory");
            }
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
            return rocksdb::IOStatus::IOError("Unknown error when calling DeleteDir");
        }
    }

    rocksdb::IOStatus BlobFilesystem::GetFileSize(const std::string& f,
        const rocksdb::IOOptions&,
        uint64_t* s,
        rocksdb::IODebugContext*)
    {
        try
        {
            const auto fileSize = m_filesystem->GetFileSize(f);
            assert(fileSize >= 0);
            *s = static_cast<uint64_t>(fileSize);
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling GetFileSize");
        }
    }

    rocksdb::IOStatus BlobFilesystem::GetFileModificationTime(const std::string& fname,
        const rocksdb::IOOptions&,
        uint64_t* file_mtime,
        rocksdb::IODebugContext*)
    {
        try
        {
            *file_mtime = m_filesystem->GetFileModificationTime(fname);
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling GetFileModificationTime");
        }
    }

    rocksdb::IOStatus BlobFilesystem::GetAbsolutePath(const std::string& db_path,
        const rocksdb::IOOptions&,
        std::string* output_path,
        rocksdb::IODebugContext*)
    {
        // note that this can be simply set as all paths are absolute in blob land
        *output_path = db_path;
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus BlobFilesystem::RenameFile(const std::string& s,
        const std::string& t,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        try
        {
            m_filesystem->RenameFile(s, t);
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling RenameFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::LinkFile(const std::string&,
        const std::string&,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        return rocksdb::IOStatus::NotSupported();
    }

    rocksdb::IOStatus BlobFilesystem::NumFileLinks(const std::string&,
        const rocksdb::IOOptions&,
        uint64_t*,
        rocksdb::IODebugContext*)
    {
        return rocksdb::IOStatus::NotSupported();
    }

    rocksdb::IOStatus BlobFilesystem::AreFilesSame(const std::string& first,
        const std::string& second,
        const rocksdb::IOOptions& options,
        bool* res,
        rocksdb::IODebugContext* dbg)
    {
        return target_->AreFilesSame(first, second, options, res, dbg);
    }

    rocksdb::IOStatus BlobFilesystem::LockFile(const std::string& f,
        const rocksdb::IOOptions&,
        rocksdb::FileLock** l,
        rocksdb::IODebugContext*)
    {
        try
        {
            *l = nullptr;
            auto lock = m_filesystem->LockFile(f);
            auto lockFileWrapper = new Plugin::Azure::LockFile(lock);
            m_lockFiles.push_back(lockFileWrapper);
            *l = lockFileWrapper;
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling LockFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::UnlockFile(rocksdb::FileLock* l,
        const rocksdb::IOOptions&,
        rocksdb::IODebugContext*)
    {
        try
        {
            auto lockFile = dynamic_cast<Plugin::Azure::LockFile*>(l);
            if (lockFile == nullptr)
            {
                BOOST_LOG_SEV(*m_logger, error) << "Unable to cast file lock to Azure::LockFile";
                return rocksdb::IOStatus::InvalidArgument();
            }

            m_filesystem->UnlockFile(lockFile->GetImpl());
            std::erase_if(m_lockFiles, [lockFile](const auto& l) { return l == lockFile; });
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when calling UnlockFile");
        }
    }

    rocksdb::IOStatus BlobFilesystem::GetTestDirectory(const rocksdb::IOOptions& options,
        std::string* path,
        rocksdb::IODebugContext* dbg)
    {
        return target_->GetTestDirectory(options, path, dbg);
    }

    rocksdb::IOStatus BlobFilesystem::NewLogger(const std::string& fname,
        const rocksdb::IOOptions&,
        std::shared_ptr<rocksdb::Logger>* result,
        rocksdb::IODebugContext*)
    {
        try
        {
            auto impl = m_filesystem->CreateLogger(fname, rocksdb::Logger::kDefaultLogLevel);
            *result = std::shared_ptr<rocksdb::Logger>(new Plugin::Azure::Logger(std::move(impl)));
            return rocksdb::IOStatus::OK();
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
            return rocksdb::IOStatus::IOError("Unknown error when creating NewLogger");
        }
    }

    rocksdb::IOStatus BlobFilesystem::GetFreeSpace(const std::string&,
        const rocksdb::IOOptions&,
        uint64_t* diskfree,
        rocksdb::IODebugContext*)
    {
        //TODO: Figure out if this is something we need to manage
        *diskfree = static_cast<uint64_t>(INT_MAX) * 256; // a terrabyte
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus BlobFilesystem::IsDirectory(const std::string& path,
        const rocksdb::IOOptions& options,
        bool* is_dir,
        rocksdb::IODebugContext* dbg)
    {
        // TODO: figure out if there is a way to tell based on path separator
        return target_->IsDirectory(path, options, is_dir, dbg);
    }

    rocksdb::IOStatus BlobFilesystem::Poll(std::vector<void*>&, size_t)
    {
        return rocksdb::IOStatus::NotSupported();
    }

    rocksdb::IOStatus BlobFilesystem::AbortIO(std::vector<void*>&)
    {
        return rocksdb::IOStatus::NotSupported();
    }

    void BlobFilesystem::DiscardCacheForDirectory(const std::string&)
    {
        return;
    }

    void BlobFilesystem::SupportedOps(int64_t& supported_ops)
    {
        supported_ops = rocksdb::FSSupportedOps::kAsyncIO;
    }
}
