// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Directory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/LockFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/SequentialFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"

#include <boost/log/trivial.hpp>
#include <sstream>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Azure {
using namespace boost::log::trivial;
using Impl::AbortAsyncReads;
using Impl::PollAsyncReads;
namespace {
std::string FormatRequestFailedLogMessage(const RequestFailedException& ex, const std::string_view path = {}) {
    std::ostringstream formattedMessage;
    formattedMessage << "[" << ex.ErrorCode << "]" << " (Status Code: " << static_cast<int>(ex.StatusCode) << ") ";

    if (!path.empty()) {
        if (ex.StatusCode == HttpStatus::NotFound) {
            formattedMessage << "Blob/path '" << path << "' not found. ";
        } else {
            formattedMessage << "Blob/path '" << path << "'. ";
        }
    }

    formattedMessage << ex.Message;
    return formattedMessage.str();
}
} // namespace

BlobFilesystem::BlobFilesystem(
    std::shared_ptr<rocksdb::FileSystem> rocksdbFs, std::unique_ptr<Impl::BlobFilesystemImpl> filesystem,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : rocksdb::FileSystemWrapper(std::move(rocksdbFs)), m_filesystem(std::move(filesystem)),
      m_logger(std::move(logger)),
      m_blobNotFoundRateLimiter(std::make_unique<Impl::LogRateLimiter>(std::vector<std::string>{"BlobNotFound"},
                                                                       Impl::Configuration::LogRateLimiterCooldown)) {}

void BlobFilesystem::LogRequestFailed(const RequestFailedException& ex, std::string_view path) {
    if (ex.ErrorCode == "BlobNotFound") {
        const auto result = m_blobNotFoundRateLimiter->CheckAndRecord(ex.ErrorCode.c_str());
        if (result.decision == Impl::RateDecision::Suppress) {
            return;
        }
        if (result.decision == Impl::RateDecision::AllowWithSummary) {
            const auto seconds = m_blobNotFoundRateLimiter->Cooldown().count();
            BOOST_LOG_SEV(*m_logger, warning)
                << "[" << result.suppressedCount << " BlobNotFound messages suppressed in last " << seconds << "s]";
        }
        BOOST_LOG_SEV(*m_logger, warning) << FormatRequestFailedLogMessage(ex, path);
        return;
    }
    BOOST_LOG_SEV(*m_logger, error) << FormatRequestFailedLogMessage(ex, path);
}

BlobFilesystem::~BlobFilesystem() {
    // A destructor must not throw: a failed release is logged and the lease simply expires on the service.
    for (auto& lock : m_lockFiles) {
        try {
            m_filesystem->UnlockFile(lock->GetImpl());
        } catch (const std::exception& ex) {
            BOOST_LOG_SEV(*m_logger, error) << "Failed to release lock file during shutdown: " << ex.what();
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error) << "Failed to release lock file during shutdown: unknown error";
        }
    }
}

// Runs an operation, converting any exception into a logged rocksdb::IOStatus so that nothing escapes into RocksDB.
template <class F> rocksdb::IOStatus BlobFilesystem::Guard(std::string_view operation, std::string_view path, F&& f) {
    try {
        return f();
    } catch (const RequestFailedException& ex) {
        LogRequestFailed(ex, path);
        return AzureErrorTranslator::IOStatusFromError(ex);
    } catch (const std::exception& ex) {
        BOOST_LOG_SEV(*m_logger, error) << operation << " failed: " << ex.what();
        return rocksdb::IOStatus::IOError(ex.what());
    } catch (...) {
        BOOST_LOG_SEV(*m_logger, error) << operation << " failed with an unknown error";
        return rocksdb::IOStatus::IOError(std::string("Unknown error when calling ") + std::string(operation));
    }
}

const char* BlobFilesystem::Name() const { return "AzureBlobFileSystem"; }

rocksdb::IOStatus BlobFilesystem::NewSequentialFile(const std::string& f, const rocksdb::FileOptions&,
                                                    std::unique_ptr<rocksdb::FSSequentialFile>* r,
                                                    rocksdb::IODebugContext*) {
    return Guard("NewSequentialFile", f, [&]() -> rocksdb::IOStatus {
        *r = std::unique_ptr<rocksdb::FSSequentialFile>(new SequentialFile(m_filesystem->CreateSequentialFile(f)));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::NewRandomAccessFile(const std::string& f, const rocksdb::FileOptions&,
                                                      std::unique_ptr<rocksdb::FSRandomAccessFile>* r,
                                                      rocksdb::IODebugContext*) {
    return Guard("NewRandomAccessFile", f, [&]() -> rocksdb::IOStatus {
        *r = std::unique_ptr<rocksdb::FSRandomAccessFile>(new ReadableFile(m_filesystem->CreateReadableFile(f)));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::NewWritableFile(const std::string& f, const rocksdb::FileOptions&,
                                                  std::unique_ptr<rocksdb::FSWritableFile>* r,
                                                  rocksdb::IODebugContext*) {
    return Guard("NewWritableFile", f, [&]() -> rocksdb::IOStatus {
        *r =
            std::unique_ptr<rocksdb::FSWritableFile>(new WriteableFile(m_filesystem->CreateWriteableFile(f), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::ReopenWritableFile(const std::string& fname, const rocksdb::FileOptions&,
                                                     std::unique_ptr<rocksdb::FSWritableFile>* result,
                                                     rocksdb::IODebugContext*) {
    return Guard("ReopenWritableFile", fname, [&]() -> rocksdb::IOStatus {
        *result = std::unique_ptr<rocksdb::FSWritableFile>(
            new WriteableFile(m_filesystem->ReopenWriteableFile(fname), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::ReuseWritableFile(const std::string& fname, const std::string&,
                                                    const rocksdb::FileOptions&,
                                                    std::unique_ptr<rocksdb::FSWritableFile>* r,
                                                    rocksdb::IODebugContext*) {
    return Guard("ReuseWritableFile", fname, [&]() -> rocksdb::IOStatus {
        *r = std::unique_ptr<rocksdb::FSWritableFile>(
            new WriteableFile(m_filesystem->ReuseWritableFile(fname), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::NewRandomRWFile(const std::string& fname, const rocksdb::FileOptions&,
                                                  std::unique_ptr<rocksdb::FSRandomRWFile>* result,
                                                  rocksdb::IODebugContext*) {
    return Guard("NewRandomRWFile", fname, [&]() -> rocksdb::IOStatus {
        *result = std::unique_ptr<rocksdb::FSRandomRWFile>(
            new ReadWriteFile(m_filesystem->CreateReadWriteFile(fname), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::NewMemoryMappedFileBuffer(const std::string&,
                                                            std::unique_ptr<rocksdb::MemoryMappedFileBuffer>*) {
    return rocksdb::IOStatus::NotSupported();
}

rocksdb::IOStatus BlobFilesystem::NewDirectory(const std::string& name, const rocksdb::IOOptions&,
                                               std::unique_ptr<rocksdb::FSDirectory>* result,
                                               rocksdb::IODebugContext*) {
    return Guard("NewDirectory", name, [&]() -> rocksdb::IOStatus {
        *result = std::unique_ptr<rocksdb::FSDirectory>(new Directory(m_filesystem->CreateDirectory(name)));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::FileExists(const std::string& f, const rocksdb::IOOptions&,
                                             rocksdb::IODebugContext*) {
    return Guard("FileExists", f, [&]() -> rocksdb::IOStatus {
        if (m_filesystem->FileExists(f)) {
            return rocksdb::IOStatus::OK();
        } else {
            return rocksdb::IOStatus::NotFound();
        }
    });
}

rocksdb::IOStatus BlobFilesystem::GetChildren(const std::string& dir, const rocksdb::IOOptions&,
                                              std::vector<std::string>* r, rocksdb::IODebugContext*) {
    return Guard("GetChildren", dir, [&]() -> rocksdb::IOStatus {
        *r = m_filesystem->GetChildren(dir);
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::GetChildrenFileAttributes(const std::string& dir, const rocksdb::IOOptions&,
                                                            std::vector<rocksdb::FileAttributes>* result,
                                                            rocksdb::IODebugContext*) {
    return Guard("GetChildrenFileAttributes", dir, [&]() -> rocksdb::IOStatus {
        const auto attributes = m_filesystem->GetChildrenFileAttributes(dir);
        for (const auto& attr : attributes) {
            result->push_back({
                .name = attr.GetName(),
                .size_bytes = attr.GetSize(),
            });
        }

        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::DeleteFile(const std::string& f, const rocksdb::IOOptions&,
                                             rocksdb::IODebugContext*) {
    return Guard("DeleteFile", f, [&]() -> rocksdb::IOStatus {
        if (m_filesystem->DeleteFile(f)) {
            return rocksdb::IOStatus::OK();
        } else {
            return rocksdb::IOStatus::NotFound();
        }
    });
}

rocksdb::IOStatus BlobFilesystem::Truncate(const std::string& fname, size_t size, const rocksdb::IOOptions&,
                                           rocksdb::IODebugContext*) {
    return Guard("Truncate", fname, [&]() -> rocksdb::IOStatus {
        assert(size < static_cast<size_t>(std::numeric_limits<int64_t>::max()));
        m_filesystem->Truncate(fname, static_cast<int64_t>(size));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::CreateDir(const std::string&, const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus BlobFilesystem::CreateDirIfMissing(const std::string&, const rocksdb::IOOptions&,
                                                     rocksdb::IODebugContext*) {
    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus BlobFilesystem::DeleteDir(const std::string& d, const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return Guard("DeleteDir", d, [&]() -> rocksdb::IOStatus {
        const auto remainingFiles = m_filesystem->DeleteDir(d);
        if (remainingFiles == 0) {
            return rocksdb::IOStatus::OK();
        } else {
            BOOST_LOG_SEV(*m_logger, error)
                << "Failed to delete all contents within directory. " << remainingFiles << " remaining.";
            return rocksdb::IOStatus::IOError("Failed to delete all contents within directory");
        }
    });
}

rocksdb::IOStatus BlobFilesystem::GetFileSize(const std::string& f, const rocksdb::IOOptions&, uint64_t* s,
                                              rocksdb::IODebugContext*) {
    return Guard("GetFileSize", f, [&]() -> rocksdb::IOStatus {
        const auto fileSize = m_filesystem->GetFileSize(f);
        assert(fileSize >= 0);
        *s = static_cast<uint64_t>(fileSize);
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::GetFileModificationTime(const std::string& fname, const rocksdb::IOOptions&,
                                                          uint64_t* file_mtime, rocksdb::IODebugContext*) {
    return Guard("GetFileModificationTime", fname, [&]() -> rocksdb::IOStatus {
        *file_mtime = m_filesystem->GetFileModificationTime(fname);
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::GetAbsolutePath(const std::string& db_path, const rocksdb::IOOptions&,
                                                  std::string* output_path, rocksdb::IODebugContext*) {
    // note that this can be simply set as all paths are absolute in blob land
    *output_path = db_path;
    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus BlobFilesystem::RenameFile(const std::string& s, const std::string& t, const rocksdb::IOOptions&,
                                             rocksdb::IODebugContext*) {
    return Guard("RenameFile", s + " -> " + t, [&]() -> rocksdb::IOStatus {
        m_filesystem->RenameFile(s, t);
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::LinkFile(const std::string&, const std::string&, const rocksdb::IOOptions&,
                                           rocksdb::IODebugContext*) {
    return rocksdb::IOStatus::NotSupported();
}

rocksdb::IOStatus BlobFilesystem::NumFileLinks(const std::string&, const rocksdb::IOOptions&, uint64_t*,
                                               rocksdb::IODebugContext*) {
    return rocksdb::IOStatus::NotSupported();
}

rocksdb::IOStatus BlobFilesystem::AreFilesSame(const std::string& first, const std::string& second,
                                               const rocksdb::IOOptions& options, bool* res,
                                               rocksdb::IODebugContext* dbg) {
    return target_->AreFilesSame(first, second, options, res, dbg);
}

rocksdb::IOStatus BlobFilesystem::LockFile(const std::string& f, const rocksdb::IOOptions&, rocksdb::FileLock** l,
                                           rocksdb::IODebugContext*) {
    return Guard("LockFile", f, [&]() -> rocksdb::IOStatus {
        *l = nullptr;
        auto lock = m_filesystem->LockFile(f);
        auto lockFileWrapper = std::make_unique<Plugin::Azure::LockFile>(lock);
        *l = lockFileWrapper.get();
        std::scoped_lock _(m_lockFilesMutex);
        m_lockFiles.push_back(std::move(lockFileWrapper));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::UnlockFile(rocksdb::FileLock* l, const rocksdb::IOOptions&,
                                             rocksdb::IODebugContext*) {
    return Guard("UnlockFile", {}, [&]() -> rocksdb::IOStatus {
        auto lockFile = dynamic_cast<Plugin::Azure::LockFile*>(l);
        if (lockFile == nullptr) {
            BOOST_LOG_SEV(*m_logger, error) << "Unable to cast file lock to Azure::LockFile";
            return rocksdb::IOStatus::InvalidArgument();
        }

        m_filesystem->UnlockFile(lockFile->GetImpl());
        std::scoped_lock _(m_lockFilesMutex);
        std::erase_if(m_lockFiles, [lockFile](const auto& entry) { return entry.get() == lockFile; });
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::GetTestDirectory(const rocksdb::IOOptions& options, std::string* path,
                                                   rocksdb::IODebugContext* dbg) {
    return target_->GetTestDirectory(options, path, dbg);
}

rocksdb::IOStatus BlobFilesystem::NewLogger(const std::string& fname, const rocksdb::IOOptions&,
                                            std::shared_ptr<rocksdb::Logger>* result, rocksdb::IODebugContext*) {
    return Guard("NewLogger", fname, [&]() -> rocksdb::IOStatus {
        auto impl = m_filesystem->CreateLogger(fname, rocksdb::Logger::kDefaultLogLevel);
        *result = std::shared_ptr<rocksdb::Logger>(new Plugin::Azure::Logger(std::move(impl)));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::GetFreeSpace(const std::string&, const rocksdb::IOOptions&, uint64_t* diskfree,
                                               rocksdb::IODebugContext*) {
    // TODO: Figure out if this is something we need to manage
    *diskfree = static_cast<uint64_t>(INT_MAX) * 256; // a terrabyte
    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus BlobFilesystem::IsDirectory(const std::string& path, const rocksdb::IOOptions& options, bool* is_dir,
                                              rocksdb::IODebugContext* dbg) {
    // TODO: figure out if there is a way to tell based on path separator
    return target_->IsDirectory(path, options, is_dir, dbg);
}

// Reads issued via ReadAsync complete on the host io_context; their callbacks are delivered here on the caller's
// thread. Every handle is waited on, which satisfies any min_completions.
rocksdb::IOStatus BlobFilesystem::Poll(std::vector<void*>& io_handles, size_t) { return PollAsyncReads(io_handles); }

rocksdb::IOStatus BlobFilesystem::AbortIO(std::vector<void*>& io_handles) { return AbortAsyncReads(io_handles); }

void BlobFilesystem::DiscardCacheForDirectory(const std::string&) { return; }

// Only async reads are advertised: FS-allocated buffers and verify-and-reconstruct are not implemented.
void BlobFilesystem::SupportedOps(int64_t& supported_ops) {
    supported_ops = int64_t{1} << rocksdb::FSSupportedOps::kAsyncIO;
}
} // namespace AVEVA::RocksDB::Plugin::Azure