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
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/SequentialFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"

#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>
#include <boost/scope/scope_fail.hpp>
#include <cassert>
#include <climits>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Azure {
using namespace boost::log::trivial;

struct BlobFilesystem::State {
    std::vector<std::unique_ptr<Azure::LockFile>> LockFiles;
    std::mutex LockFilesMutex; // RocksDB may call LockFile/UnlockFile from different threads.
    // Directories RocksDB created explicitly. Blob storage has no empty directories, so these are the only way to
    // report a directory that does not hold a blob yet.
    std::set<std::string> KnownDirectories;
    std::mutex KnownDirectoriesMutex;
    Impl::LogRateLimiter BlobNotFoundRateLimiter{std::vector<std::string>{"BlobNotFound"},
                                                 Impl::Configuration::LogRateLimiterCooldown};
};

using Impl::AbortAsyncReads;
using Impl::PollAsyncReads;
namespace {
// Blob paths use forward slashes and have no notion of ".." or repeated separators, so two spellings of one path
// compare equal once the separators are normalized.
std::string NormalizePath(std::string_view path) {
    std::string normalized;
    normalized.reserve(path.size());
    for (const char c : path) {
        const char next = c == '\\' ? '/' : c;
        if (next == '/' && !normalized.empty() && normalized.back() == '/') {
            continue;
        }
        normalized.push_back(next);
    }
    if (normalized.size() > 1 && normalized.back() == '/') {
        normalized.pop_back();
    }
    return normalized;
}

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
    // The request id is what Azure support asks for, so it belongs in the filesystem-level log too.
    if (!ex.RequestId.empty()) {
        formattedMessage << " (request id: " << ex.RequestId << ")";
    }
    return formattedMessage.str();
}
} // namespace

BlobFilesystem::BlobFilesystem(
    std::shared_ptr<rocksdb::FileSystem> rocksdbFs, std::unique_ptr<Impl::BlobFilesystemImpl> filesystem,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::shared_ptr<void> keepAlive)
    : rocksdb::FileSystemWrapper(std::move(rocksdbFs)), m_keepAlive(std::move(keepAlive)),
      m_filesystem(std::move(filesystem)), m_logger(std::move(logger)), m_state(std::make_unique<State>()) {}

void BlobFilesystem::LogRequestFailed(const RequestFailedException& ex, std::string_view path) {
    if (ex.ErrorCode == "BlobNotFound") {
        const auto result = m_state->BlobNotFoundRateLimiter.CheckAndRecord(ex.ErrorCode.c_str());
        if (result.decision == Impl::RateDecision::Suppress) {
            return;
        }
        if (result.decision == Impl::RateDecision::AllowWithSummary) {
            const auto seconds = m_state->BlobNotFoundRateLimiter.Cooldown().count();
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
    for (auto& lock : m_state->LockFiles) {
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
    } catch (const std::invalid_argument& ex) {
        BOOST_LOG_SEV(*m_logger, error) << operation << " rejected: " << ex.what();
        return rocksdb::IOStatus::InvalidArgument(ex.what());
    } catch (...) {
        // Everything else (fencing, generic and unknown exceptions) shares the translator's mapping so that
        // filesystem-level operations report IOFenced just like file-level ones.
        return AzureErrorTranslator::FromCurrentException(*m_logger, operation);
    }
}

const char* BlobFilesystem::Name() const { return "AzureBlobFileSystem"; }

rocksdb::IOStatus BlobFilesystem::NewSequentialFile(const std::string& f, const rocksdb::FileOptions&,
                                                    std::unique_ptr<rocksdb::FSSequentialFile>* r,
                                                    rocksdb::IODebugContext*) {
    return Guard("NewSequentialFile", f, [&]() -> rocksdb::IOStatus {
        auto file = m_filesystem->CreateSequentialFile(f);
        // RenameFile commits over an existing destination by resetting its logical size to 0 and writing it last, so
        // an interrupted replace of CURRENT reads as empty. Say so explicitly rather than letting RocksDB report a
        // generic parse failure.
        if (std::string_view(f).ends_with("/CURRENT") && file.GetSize() == 0) {
            BOOST_LOG_SEV(*m_logger, error)
                << "'" << f << "' is empty; a rename over it was probably interrupted. Restore it from the manifest "
                << "name before reopening the database";
            return rocksdb::IOStatus::Corruption("CURRENT is empty: " + f);
        }
        *r = std::unique_ptr<rocksdb::FSSequentialFile>(new SequentialFile(std::move(file), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::NewRandomAccessFile(const std::string& f, const rocksdb::FileOptions&,
                                                      std::unique_ptr<rocksdb::FSRandomAccessFile>* r,
                                                      rocksdb::IODebugContext*) {
    return Guard("NewRandomAccessFile", f, [&]() -> rocksdb::IOStatus {
        *r = std::unique_ptr<rocksdb::FSRandomAccessFile>(
            new ReadableFile(m_filesystem->CreateReadableFile(f), m_logger));
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
        *result = std::unique_ptr<rocksdb::FSDirectory>(new Directory(m_filesystem->CreateDirectory(name), m_logger));
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
        result->clear();
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
        if (size > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            return rocksdb::IOStatus::InvalidArgument("Truncate size exceeds the maximum blob size");
        }
        m_filesystem->Truncate(fname, static_cast<int64_t>(size));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::CreateDir(const std::string& d, const rocksdb::IOOptions& options,
                                            rocksdb::IODebugContext* dbg) {
    return CreateDirIfMissing(d, options, dbg);
}

rocksdb::IOStatus BlobFilesystem::CreateDirIfMissing(const std::string& d, const rocksdb::IOOptions&,
                                                     rocksdb::IODebugContext*) {
    return Guard("CreateDirIfMissing", d, [&]() -> rocksdb::IOStatus {
        std::scoped_lock lock(m_state->KnownDirectoriesMutex);
        m_state->KnownDirectories.insert(NormalizePath(d));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::DeleteDir(const std::string& d, const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return Guard("DeleteDir", d, [&]() -> rocksdb::IOStatus {
        const auto remainingFiles = m_filesystem->DeleteDir(d);
        if (remainingFiles == 0) {
            std::scoped_lock lock(m_state->KnownDirectoriesMutex);
            m_state->KnownDirectories.erase(NormalizePath(d));
            return rocksdb::IOStatus::OK();
        } else {
            BOOST_LOG_SEV(*m_logger, error) << "Failed to delete all contents within directory; some remain.";
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
                                               const rocksdb::IOOptions&, bool* res, rocksdb::IODebugContext*) {
    return Guard("AreFilesSame", first + " vs " + second, [&]() -> rocksdb::IOStatus {
        *res = NormalizePath(first) == NormalizePath(second);
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::LockFile(const std::string& f, const rocksdb::IOOptions&, rocksdb::FileLock** l,
                                           rocksdb::IODebugContext*) {
    return Guard("LockFile", f, [&]() -> rocksdb::IOStatus {
        *l = nullptr;
        auto lock = m_filesystem->LockFile(f);
        // The lease is already held; releasing it on failure keeps it from lingering until it expires.
        boost::scope::scope_fail releaseLease{[&] {
            try {
                m_filesystem->UnlockFile(*lock);
            } catch (const std::exception& ex) {
                BOOST_LOG_SEV(*m_logger, error)
                    << "Could not release the lease on '" << f << "' after wrapping it failed: " << ex.what();
            } catch (...) {
                BOOST_LOG_SEV(*m_logger, error)
                    << "Could not release the lease on '" << f << "' after wrapping it failed";
            }
        }};
        auto lockFileWrapper = std::make_unique<Plugin::Azure::LockFile>(lock);
        std::scoped_lock _(m_state->LockFilesMutex);
        m_state->LockFiles.push_back(std::move(lockFileWrapper));
        *l = m_state->LockFiles.back().get();
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

        // The wrapper is dropped even if the release fails: the lock is unlinked from renewal either way, so keeping
        // it would only make RocksDB or the destructor retry a release that cannot succeed.
        const boost::scope::scope_exit forget{[&] {
            std::scoped_lock _(m_state->LockFilesMutex);
            std::erase_if(m_state->LockFiles, [lockFile](const auto& entry) { return entry.get() == lockFile; });
        }};
        m_filesystem->UnlockFile(lockFile->GetImpl());
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
        *result = std::shared_ptr<rocksdb::Logger>(new Plugin::Azure::Logger(std::move(impl), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

rocksdb::IOStatus BlobFilesystem::GetFreeSpace(const std::string&, const rocksdb::IOOptions&, uint64_t* diskfree,
                                               rocksdb::IODebugContext*) {
    // The container is effectively unbounded, so report a large fixed amount rather than querying the service.
    *diskfree = static_cast<uint64_t>(INT_MAX) * 256; // about a terabyte
    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus BlobFilesystem::IsDirectory(const std::string& path, const rocksdb::IOOptions&, bool* is_dir,
                                              rocksdb::IODebugContext*) {
    return Guard("IsDirectory", path, [&]() -> rocksdb::IOStatus {
        {
            std::scoped_lock lock(m_state->KnownDirectoriesMutex);
            if (m_state->KnownDirectories.contains(NormalizePath(path))) {
                *is_dir = true;
                return rocksdb::IOStatus::OK();
            }
        }
        // A prefix is a directory exactly when something lives beneath it.
        if (m_filesystem->HasChildren(path)) {
            *is_dir = true;
            return rocksdb::IOStatus::OK();
        }
        if (m_filesystem->FileExists(path)) {
            *is_dir = false;
            return rocksdb::IOStatus::OK();
        }
        return rocksdb::IOStatus::NotFound();
    });
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