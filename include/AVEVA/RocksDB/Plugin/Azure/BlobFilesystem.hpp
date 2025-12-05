// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/LockFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"

#include <boost/log/trivial.hpp>
#include <boost/intrusive/list.hpp>

#include <rocksdb/file_system.h>

#include <string>
#include <memory>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class BlobFilesystem final : public rocksdb::FileSystemWrapper
    {
        std::unique_ptr<Impl::BlobFilesystemImpl> m_filesystem;
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
        boost::intrusive::list<Azure::LockFile, boost::intrusive::constant_time_size<false>> m_lockFiles;

    public:
        BlobFilesystem(std::shared_ptr<rocksdb::FileSystem> rocksdbFs,
            std::unique_ptr<Impl::BlobFilesystemImpl> filesystem,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
        virtual ~BlobFilesystem() override;
        BlobFilesystem(const BlobFilesystem&) = delete;
        BlobFilesystem& operator=(const BlobFilesystem&) = delete;
        BlobFilesystem(BlobFilesystem&&) noexcept = delete;
        BlobFilesystem& operator=(BlobFilesystem&&) = delete;

        virtual const char* Name() const override;
        virtual rocksdb::IOStatus NewSequentialFile(const std::string& f,
            const rocksdb::FileOptions& file_opts,
            std::unique_ptr<rocksdb::FSSequentialFile>* r,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus NewRandomAccessFile(const std::string& f,
            const rocksdb::FileOptions& file_opts,
            std::unique_ptr<rocksdb::FSRandomAccessFile>* r,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus NewWritableFile(const std::string& f,
            const rocksdb::FileOptions& file_opts,
            std::unique_ptr<rocksdb::FSWritableFile>* r,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus ReopenWritableFile(const std::string& fname,
            const rocksdb::FileOptions& file_opts,
            std::unique_ptr<rocksdb::FSWritableFile>* result,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus ReuseWritableFile(const std::string& fname,
            const std::string& old_fname,
            const rocksdb::FileOptions& file_opts,
            std::unique_ptr<rocksdb::FSWritableFile>* r,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus NewRandomRWFile(const std::string& fname,
            const rocksdb::FileOptions& file_opts,
            std::unique_ptr<rocksdb::FSRandomRWFile>* result,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus NewMemoryMappedFileBuffer(const std::string& fname,
            std::unique_ptr<rocksdb::MemoryMappedFileBuffer>* result) override;
        virtual rocksdb::IOStatus NewDirectory(const std::string& name,
            const rocksdb::IOOptions& io_opts,
            std::unique_ptr<rocksdb::FSDirectory>* result,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus FileExists(const std::string& f,
            const rocksdb::IOOptions& io_opts,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus GetChildren(const std::string& dir,
            const rocksdb::IOOptions& io_opts,
            std::vector<std::string>* r,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus GetChildrenFileAttributes(const std::string& dir,
            const rocksdb::IOOptions& options,
            std::vector<rocksdb::FileAttributes>* result,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus DeleteFile(const std::string& f,
            const rocksdb::IOOptions & options,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Truncate(const std::string& fname,
            size_t size,
            const rocksdb::IOOptions& options,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus CreateDir(const std::string& d,
            const rocksdb::IOOptions& options,
            rocksdb::IODebugContext*) override;
        virtual rocksdb::IOStatus CreateDirIfMissing(const std::string& d,
            const rocksdb::IOOptions& options,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus DeleteDir(const std::string& d,
            const rocksdb::IOOptions& options,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus GetFileSize(const std::string& f,
            const rocksdb::IOOptions& options,
            uint64_t* s,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus GetFileModificationTime(const std::string& fname,
            const rocksdb::IOOptions& options,
            uint64_t* file_mtime,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus GetAbsolutePath(const std::string& db_path,
            const rocksdb::IOOptions& options,
            std::string* output_path,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus RenameFile(const std::string& s,
            const std::string& t,
            const rocksdb::IOOptions& options,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus LinkFile(const std::string& s,
            const std::string& t,
            const rocksdb::IOOptions& options,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus NumFileLinks(const std::string& fname,
            const rocksdb::IOOptions& options,
            uint64_t* count,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus AreFilesSame(const std::string& first,
            const std::string& second,
            const rocksdb::IOOptions& options, bool* res,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus LockFile(const std::string& f,
            const rocksdb::IOOptions& options,
            rocksdb::FileLock** l,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus UnlockFile(rocksdb::FileLock* l,
            const rocksdb::IOOptions& options,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus GetTestDirectory(const rocksdb::IOOptions& options,
            std::string* path,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus NewLogger(const std::string& fname,
            const rocksdb::IOOptions& options,
            std::shared_ptr<rocksdb::Logger>* result,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus GetFreeSpace(const std::string& path,
            const rocksdb::IOOptions& options,
            uint64_t* diskfree,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus IsDirectory(const std::string& path,
            const rocksdb::IOOptions& options,
            bool* is_dir,
            rocksdb::IODebugContext* dbg) override;
        virtual rocksdb::IOStatus Poll(std::vector<void*>& io_handles,
            size_t min_completions) override;
        virtual rocksdb::IOStatus AbortIO(std::vector<void*>& io_handles) override;
        void DiscardCacheForDirectory(const std::string& path) override;
        void SupportedOps(int64_t& supported_ops) override;
    };
}
