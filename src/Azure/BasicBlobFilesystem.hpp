// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Directory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/HandleFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/LockFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"
#include "BasicReadWriteFile.hpp"
#include "BasicReadableFile.hpp"
#include "BasicSequentialFile.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>
#include <boost/scope/scope_fail.hpp>
#include <rocksdb/file_system.h>

#include <cassert>
#include <climits>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure {
// Lock, directory and log-rate-limiter bookkeeping. Declared outside the filesystem class because that class has a
// member named LockFile, which would otherwise change the meaning of the type name used here.
struct BlobFilesystemState {
    std::vector<std::unique_ptr<Azure::LockFile>> LockFiles;
    std::mutex LockFilesMutex; // RocksDB may call LockFile/UnlockFile from different threads.
    // Directories RocksDB created explicitly. Blob storage has no empty directories, so these are the only way to
    // report a directory that does not hold a blob yet.
    std::set<std::string> KnownDirectories;
    std::mutex KnownDirectoriesMutex;
    Impl::LogRateLimiter BlobNotFoundRateLimiter{std::vector<std::string>{"BlobNotFound"},
                                                 Impl::Configuration::LogRateLimiterCooldown};
};

// Adapts a blob filesystem implementation to RocksDB. A template so the implementation is injected rather than named;
// the production instantiation is registered by Plugin::Register.
template <class TImpl> class BasicBlobFilesystem final : public rocksdb::FileSystemWrapper {
    using SequentialImpl = decltype(std::declval<TImpl&>().CreateSequentialFile(std::declval<const std::string&>()));
    using ReadableImpl =
        typename decltype(std::declval<TImpl&>().CreateReadableFile(std::declval<const std::string&>()))::element_type;
    using ReadWriteImpl = decltype(std::declval<TImpl&>().CreateReadWriteFile(std::declval<const std::string&>()));

    TImpl m_filesystem;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    BlobFilesystemState m_state;

    // Logs a RequestFailedException, suppressing repeated BlobNotFound (404) messages
    // during the rate-limit cooldown window.
    void LogRequestFailed(const RequestFailedException& ex, std::string_view path = {});

    // Runs f, translating and logging any exception as a rocksdb::IOStatus.
    template <class F> rocksdb::IOStatus Guard(std::string_view operation, std::string_view path, F&& f);
    // As above for two-path operations; the "first <separator> second" label is only built if f throws.
    template <class F>
    rocksdb::IOStatus Guard(std::string_view operation, std::string_view path, std::string_view separator,
                            std::string_view path2, F&& f);

  public:
    // Constructs the implementation in place from implArgs: it owns threads, caches and clients, so it can be
    // neither copied nor moved.
    template <class... TImplArgs>
    explicit BasicBlobFilesystem(
        std::shared_ptr<rocksdb::FileSystem> rocksdbFs,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        TImplArgs&&... implArgs)
        : rocksdb::FileSystemWrapper(std::move(rocksdbFs)), m_filesystem(std::forward<TImplArgs>(implArgs)...),
          m_logger(std::move(logger)) {}
    [[nodiscard]] TImpl& Implementation() { return m_filesystem; }
    ~BasicBlobFilesystem() override;
    BasicBlobFilesystem(const BasicBlobFilesystem&) = delete;
    BasicBlobFilesystem& operator=(const BasicBlobFilesystem&) = delete;
    BasicBlobFilesystem(BasicBlobFilesystem&&) noexcept = delete;
    BasicBlobFilesystem& operator=(BasicBlobFilesystem&&) = delete;

    const char* Name() const override;
    rocksdb::IOStatus NewSequentialFile(const std::string& f, const rocksdb::FileOptions& file_opts,
                                        std::unique_ptr<rocksdb::FSSequentialFile>* r,
                                        rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus NewRandomAccessFile(const std::string& f, const rocksdb::FileOptions& file_opts,
                                          std::unique_ptr<rocksdb::FSRandomAccessFile>* r,
                                          rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus NewWritableFile(const std::string& f, const rocksdb::FileOptions& file_opts,
                                      std::unique_ptr<rocksdb::FSWritableFile>* r,
                                      rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus ReopenWritableFile(const std::string& fname, const rocksdb::FileOptions& file_opts,
                                         std::unique_ptr<rocksdb::FSWritableFile>* result,
                                         rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus ReuseWritableFile(const std::string& fname, const std::string& old_fname,
                                        const rocksdb::FileOptions& file_opts,
                                        std::unique_ptr<rocksdb::FSWritableFile>* r,
                                        rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus NewRandomRWFile(const std::string& fname, const rocksdb::FileOptions& file_opts,
                                      std::unique_ptr<rocksdb::FSRandomRWFile>* result,
                                      rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus NewMemoryMappedFileBuffer(const std::string& fname,
                                                std::unique_ptr<rocksdb::MemoryMappedFileBuffer>* result) override;
    rocksdb::IOStatus NewDirectory(const std::string& name, const rocksdb::IOOptions& io_opts,
                                   std::unique_ptr<rocksdb::FSDirectory>* result,
                                   rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus FileExists(const std::string& f, const rocksdb::IOOptions& io_opts,
                                 rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus GetChildren(const std::string& dir, const rocksdb::IOOptions& io_opts,
                                  std::vector<std::string>* r, rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus GetChildrenFileAttributes(const std::string& dir, const rocksdb::IOOptions& options,
                                                std::vector<rocksdb::FileAttributes>* result,
                                                rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus DeleteFile(const std::string& f, const rocksdb::IOOptions& options,
                                 rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus Truncate(const std::string& fname, size_t size, const rocksdb::IOOptions& options,
                               rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus CreateDir(const std::string& d, const rocksdb::IOOptions& options,
                                rocksdb::IODebugContext*) override;
    rocksdb::IOStatus CreateDirIfMissing(const std::string& d, const rocksdb::IOOptions& options,
                                         rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus DeleteDir(const std::string& d, const rocksdb::IOOptions& options,
                                rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus GetFileSize(const std::string& f, const rocksdb::IOOptions& options, uint64_t* s,
                                  rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus GetFileModificationTime(const std::string& fname, const rocksdb::IOOptions& options,
                                              uint64_t* file_mtime, rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus GetAbsolutePath(const std::string& db_path, const rocksdb::IOOptions& options,
                                      std::string* output_path, rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus RenameFile(const std::string& s, const std::string& t, const rocksdb::IOOptions& options,
                                 rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus LinkFile(const std::string& s, const std::string& t, const rocksdb::IOOptions& options,
                               rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus NumFileLinks(const std::string& fname, const rocksdb::IOOptions& options, uint64_t* count,
                                   rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus AreFilesSame(const std::string& first, const std::string& second,
                                   const rocksdb::IOOptions& options, bool* res, rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus LockFile(const std::string& f, const rocksdb::IOOptions& options, rocksdb::FileLock** l,
                               rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus UnlockFile(rocksdb::FileLock* l, const rocksdb::IOOptions& options,
                                 rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus GetTestDirectory(const rocksdb::IOOptions& options, std::string* path,
                                       rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus NewLogger(const std::string& fname, const rocksdb::IOOptions& options,
                                std::shared_ptr<rocksdb::Logger>* result, rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus GetFreeSpace(const std::string& path, const rocksdb::IOOptions& options, uint64_t* diskfree,
                                   rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus IsDirectory(const std::string& path, const rocksdb::IOOptions& options, bool* is_dir,
                                  rocksdb::IODebugContext* dbg) override;
    rocksdb::IOStatus Poll(std::vector<void*>& io_handles, size_t min_completions) override;
    rocksdb::IOStatus AbortIO(std::vector<void*>& io_handles) override;
    void DiscardCacheForDirectory(const std::string& path) override;
    void SupportedOps(int64_t& supported_ops) override;
};

namespace BlobFilesystemDetail {
using namespace boost::log::trivial;

// Blob paths use forward slashes and have no notion of ".." or repeated separators, so two spellings of one path
// compare equal once the separators are normalized.
inline std::string NormalizePath(std::string_view path) {
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

inline std::string FormatRequestFailedLogMessage(const RequestFailedException& ex, const std::string_view path = {}) {
    std::ostringstream formattedMessage;
    formattedMessage << "[" << ex.ErrorCode().Value() << "]" << " (Status Code: " << ex.StatusCode().Value() << ") ";

    if (!path.empty()) {
        if (ex.StatusCode() == HttpStatus::NotFound) {
            formattedMessage << "Blob/path '" << path << "' not found. ";
        } else {
            formattedMessage << "Blob/path '" << path << "'. ";
        }
    }

    formattedMessage << ex.Message().Value();
    // The request id is what Azure support asks for, so it belongs in the filesystem-level log too.
    if (!ex.RequestId().Value().empty()) {
        formattedMessage << " (request id: " << ex.RequestId().Value() << ")";
    }
    return formattedMessage.str();
}
} // namespace BlobFilesystemDetail

template <class TImpl>
void BasicBlobFilesystem<TImpl>::LogRequestFailed(const RequestFailedException& ex, std::string_view path) {
    using namespace boost::log::trivial;
    if (ex.ErrorCode().Value() == "BlobNotFound") {
        const auto result = m_state.BlobNotFoundRateLimiter.CheckAndRecord(ex.ErrorCode().Value().c_str());
        if (result.decision == Impl::RateDecision::Suppress) {
            return;
        }
        if (result.decision == Impl::RateDecision::AllowWithSummary) {
            const auto seconds = m_state.BlobNotFoundRateLimiter.Cooldown().count();
            BOOST_LOG_SEV(*m_logger, warning)
                << "[" << result.suppressedCount << " BlobNotFound messages suppressed in last " << seconds << "s]";
        }
        BOOST_LOG_SEV(*m_logger, warning) << BlobFilesystemDetail::FormatRequestFailedLogMessage(ex, path);
        return;
    }
    BOOST_LOG_SEV(*m_logger, error) << BlobFilesystemDetail::FormatRequestFailedLogMessage(ex, path);
}

template <class TImpl> BasicBlobFilesystem<TImpl>::~BasicBlobFilesystem() {
    using namespace boost::log::trivial;
    // A destructor must not throw: a failed release is logged and the lease simply expires on the service.
    for (auto& lock : m_state.LockFiles) {
        try {
            m_filesystem.UnlockFile(*lock->SharedImpl());
        } catch (const std::exception& ex) {
            BOOST_LOG_SEV(*m_logger, error) << "Failed to release lock file during shutdown: " << ex.what();
        } catch (...) {
            BOOST_LOG_SEV(*m_logger, error) << "Failed to release lock file during shutdown: unknown error";
        }
    }
}

// Runs an operation, converting any exception into a logged rocksdb::IOStatus so that nothing escapes into RocksDB.
template <class TImpl>
template <class F>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::Guard(std::string_view operation, std::string_view path, F&& f) {
    try {
        return f();
    } catch (const RequestFailedException& ex) {
        LogRequestFailed(ex, path);
        return AzureErrorTranslator::IOStatusFromError(ex);
    } catch (...) {
        // Everything else (invalid arguments, cancellation, fencing, generic and unknown exceptions) shares the
        // translator's mapping so that filesystem-level and file-level operations report the same statuses.
        return AzureErrorTranslator::FromException(*m_logger, std::current_exception(), operation);
    }
}

template <class TImpl>
template <class F>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::Guard(std::string_view operation, std::string_view path,
                                                    std::string_view separator, std::string_view path2, F&& f) {
    try {
        return f();
    } catch (...) {
        std::string label;
        label.reserve(path.size() + separator.size() + path2.size());
        label.append(path).append(separator).append(path2);
        return Guard(operation, label, []() -> rocksdb::IOStatus { throw; });
    }
}

template <class TImpl> const char* BasicBlobFilesystem<TImpl>::Name() const { return "AzureBlobFileSystem"; }

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::NewSequentialFile(const std::string& f, const rocksdb::FileOptions&,
                                                                std::unique_ptr<rocksdb::FSSequentialFile>* r,
                                                                rocksdb::IODebugContext*) {
    return Guard("NewSequentialFile", f, [&]() -> rocksdb::IOStatus {
        using namespace boost::log::trivial;
        auto file = m_filesystem.CreateSequentialFile(f);
        // RenameFile commits over an existing destination by resetting its logical size to 0 and writing it last, so
        // an interrupted replace of CURRENT reads as empty. Say so explicitly rather than letting RocksDB report a
        // generic parse failure.
        if (std::string_view(f).ends_with("/CURRENT") && file.GetSize() == 0) {
            BOOST_LOG_SEV(*m_logger, error)
                << "'" << f << "' is empty; a rename over it was probably interrupted. Restore it from the manifest "
                << "name before reopening the database";
            return rocksdb::IOStatus::Corruption("CURRENT is empty: " + f);
        }
        *r = std::make_unique<BasicSequentialFile<SequentialImpl>>(std::move(file), m_logger);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::NewRandomAccessFile(const std::string& f, const rocksdb::FileOptions&,
                                                                  std::unique_ptr<rocksdb::FSRandomAccessFile>* r,
                                                                  rocksdb::IODebugContext*) {
    return Guard("NewRandomAccessFile", f, [&]() -> rocksdb::IOStatus {
        *r = std::make_unique<BasicReadableFile<ReadableImpl>>(m_filesystem.CreateReadableFile(f), m_logger);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::NewWritableFile(const std::string& f, const rocksdb::FileOptions&,
                                                              std::unique_ptr<rocksdb::FSWritableFile>* r,
                                                              rocksdb::IODebugContext*) {
    return Guard("NewWritableFile", f, [&]() -> rocksdb::IOStatus {
        *r =
            std::unique_ptr<rocksdb::FSWritableFile>(new WriteableFile(m_filesystem.CreateWriteableFile(f), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::ReopenWritableFile(const std::string& fname, const rocksdb::FileOptions&,
                                                                 std::unique_ptr<rocksdb::FSWritableFile>* result,
                                                                 rocksdb::IODebugContext*) {
    return Guard("ReopenWritableFile", fname, [&]() -> rocksdb::IOStatus {
        *result = std::unique_ptr<rocksdb::FSWritableFile>(
            new WriteableFile(m_filesystem.ReopenWriteableFile(fname), m_logger));
        return rocksdb::IOStatus::OK();
    });
}

// Blobs cannot be recycled in place. Creating a fresh fname would orphan old_fname in the container, so report
// NotSupported and let RocksDB fall back to NewWritableFile and delete the old log itself.
template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::ReuseWritableFile(const std::string&, const std::string&,
                                                                const rocksdb::FileOptions&,
                                                                std::unique_ptr<rocksdb::FSWritableFile>*,
                                                                rocksdb::IODebugContext*) {
    return rocksdb::IOStatus::NotSupported("ReuseWritableFile");
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::NewRandomRWFile(const std::string& fname, const rocksdb::FileOptions&,
                                                              std::unique_ptr<rocksdb::FSRandomRWFile>* result,
                                                              rocksdb::IODebugContext*) {
    return Guard("NewRandomRWFile", fname, [&]() -> rocksdb::IOStatus {
        *result =
            std::make_unique<BasicReadWriteFile<ReadWriteImpl>>(m_filesystem.CreateReadWriteFile(fname), m_logger);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus
BasicBlobFilesystem<TImpl>::NewMemoryMappedFileBuffer(const std::string&,
                                                      std::unique_ptr<rocksdb::MemoryMappedFileBuffer>*) {
    return rocksdb::IOStatus::NotSupported();
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::NewDirectory(const std::string& name, const rocksdb::IOOptions&,
                                                           std::unique_ptr<rocksdb::FSDirectory>* result,
                                                           rocksdb::IODebugContext*) {
    return Guard("NewDirectory", name, [&]() -> rocksdb::IOStatus {
        *result = Impl::HandleFactory::Create<Directory>(m_filesystem.CreateDirectory(name), m_logger);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::FileExists(const std::string& f, const rocksdb::IOOptions&,
                                                         rocksdb::IODebugContext*) {
    return Guard("FileExists", f, [&]() -> rocksdb::IOStatus {
        return m_filesystem.FileExists(f) == AzureClient::Existence::Present ? rocksdb::IOStatus::OK()
                                                                              : rocksdb::IOStatus::NotFound();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::GetChildren(const std::string& dir, const rocksdb::IOOptions&,
                                                          std::vector<std::string>* r, rocksdb::IODebugContext*) {
    return Guard("GetChildren", dir, [&]() -> rocksdb::IOStatus {
        *r = m_filesystem.GetChildren(dir);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::GetChildrenFileAttributes(const std::string& dir,
                                                                        const rocksdb::IOOptions&,
                                                                        std::vector<rocksdb::FileAttributes>* result,
                                                                        rocksdb::IODebugContext*) {
    return Guard("GetChildrenFileAttributes", dir, [&]() -> rocksdb::IOStatus {
        const auto attributes = m_filesystem.GetChildrenFileAttributes(dir);
        result->clear();
        result->reserve(attributes.size());
        for (const auto& attr : attributes) {
            result->push_back({
                .name = attr.GetName(),
                .size_bytes = attr.GetSize(),
            });
        }

        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::DeleteFile(const std::string& f, const rocksdb::IOOptions&,
                                                         rocksdb::IODebugContext*) {
    return Guard("DeleteFile", f, [&]() -> rocksdb::IOStatus {
        return m_filesystem.DeleteFile(f) == AzureClient::PriorState::Existed ? rocksdb::IOStatus::OK()
                                                                               : rocksdb::IOStatus::NotFound();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::Truncate(const std::string& fname, size_t size,
                                                       const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return Guard("Truncate", fname, [&]() -> rocksdb::IOStatus {
        if (size > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            return rocksdb::IOStatus::InvalidArgument("Truncate size exceeds the maximum blob size");
        }
        m_filesystem.Truncate(fname, static_cast<int64_t>(size));
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::CreateDir(const std::string& d, const rocksdb::IOOptions& options,
                                                        rocksdb::IODebugContext* dbg) {
    return CreateDirIfMissing(d, options, dbg);
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::CreateDirIfMissing(const std::string& d, const rocksdb::IOOptions&,
                                                                 rocksdb::IODebugContext*) {
    return Guard("CreateDirIfMissing", d, [&]() -> rocksdb::IOStatus {
        std::scoped_lock lock(m_state.KnownDirectoriesMutex);
        m_state.KnownDirectories.insert(BlobFilesystemDetail::NormalizePath(d));
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::DeleteDir(const std::string& d, const rocksdb::IOOptions&,
                                                        rocksdb::IODebugContext*) {
    return Guard("DeleteDir", d, [&]() -> rocksdb::IOStatus {
        using namespace boost::log::trivial;
        if (m_filesystem.DeleteDir(d) == Impl::DirectoryContents::Empty) {
            std::scoped_lock lock(m_state.KnownDirectoriesMutex);
            m_state.KnownDirectories.erase(BlobFilesystemDetail::NormalizePath(d));
            return rocksdb::IOStatus::OK();
        } else {
            BOOST_LOG_SEV(*m_logger, error) << "Failed to delete all contents within directory; some remain.";
            return rocksdb::IOStatus::IOError("Failed to delete all contents within directory");
        }
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::GetFileSize(const std::string& f, const rocksdb::IOOptions&, uint64_t* s,
                                                          rocksdb::IODebugContext*) {
    return Guard("GetFileSize", f, [&]() -> rocksdb::IOStatus {
        const auto fileSize = m_filesystem.GetFileSize(f);
        assert(fileSize >= 0);
        *s = static_cast<uint64_t>(fileSize);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::GetFileModificationTime(const std::string& fname,
                                                                      const rocksdb::IOOptions&, uint64_t* file_mtime,
                                                                      rocksdb::IODebugContext*) {
    return Guard("GetFileModificationTime", fname, [&]() -> rocksdb::IOStatus {
        *file_mtime = m_filesystem.GetFileModificationTime(fname);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::GetAbsolutePath(const std::string& db_path, const rocksdb::IOOptions&,
                                                              std::string* output_path, rocksdb::IODebugContext*) {
    // note that this can be simply set as all paths are absolute in blob land
    *output_path = db_path;
    return rocksdb::IOStatus::OK();
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::RenameFile(const std::string& s, const std::string& t,
                                                         const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return Guard("RenameFile", s, " -> ", t, [&]() -> rocksdb::IOStatus {
        m_filesystem.RenameFile(s, t);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::LinkFile(const std::string&, const std::string&,
                                                       const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    return rocksdb::IOStatus::NotSupported();
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::NumFileLinks(const std::string&, const rocksdb::IOOptions&, uint64_t*,
                                                           rocksdb::IODebugContext*) {
    return rocksdb::IOStatus::NotSupported();
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::AreFilesSame(const std::string& first, const std::string& second,
                                                           const rocksdb::IOOptions&, bool* res,
                                                           rocksdb::IODebugContext*) {
    return Guard("AreFilesSame", first, " vs ", second, [&]() -> rocksdb::IOStatus {
        *res = BlobFilesystemDetail::NormalizePath(first) == BlobFilesystemDetail::NormalizePath(second);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::LockFile(const std::string& f, const rocksdb::IOOptions&,
                                                       rocksdb::FileLock** l, rocksdb::IODebugContext*) {
    return Guard("LockFile", f, [&]() -> rocksdb::IOStatus {
        using namespace boost::log::trivial;
        *l = nullptr;
        auto lock = m_filesystem.LockFile(f);
        // The lease is already held; releasing it on failure keeps it from lingering until it expires.
        boost::scope::scope_fail releaseLease{[&] {
            try {
                m_filesystem.UnlockFile(*lock);
            } catch (const std::exception& ex) {
                BOOST_LOG_SEV(*m_logger, error)
                    << "Could not release the lease on '" << f << "' after wrapping it failed: " << ex.what();
            } catch (...) {
                BOOST_LOG_SEV(*m_logger, error)
                    << "Could not release the lease on '" << f << "' after wrapping it failed";
            }
        }};
        auto lockFileWrapper = Impl::HandleFactory::Create<Azure::LockFile>(lock);
        std::scoped_lock _(m_state.LockFilesMutex);
        m_state.LockFiles.push_back(std::move(lockFileWrapper));
        *l = m_state.LockFiles.back().get();
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::UnlockFile(rocksdb::FileLock* l, const rocksdb::IOOptions&,
                                                         rocksdb::IODebugContext*) {
    return Guard("UnlockFile", {}, [&]() -> rocksdb::IOStatus {
        using namespace boost::log::trivial;
        auto lockFile = dynamic_cast<Azure::LockFile*>(l);
        if (lockFile == nullptr) {
            BOOST_LOG_SEV(*m_logger, error) << "Unable to cast file lock to Azure::LockFile";
            return rocksdb::IOStatus::InvalidArgument();
        }

        // The wrapper is dropped even if the release fails: the lock is unlinked from renewal either way, so keeping
        // it would only make RocksDB or the destructor retry a release that cannot succeed.
        const boost::scope::scope_exit forget{[&] {
            std::scoped_lock _(m_state.LockFilesMutex);
            std::erase_if(m_state.LockFiles, [lockFile](const auto& entry) { return entry.get() == lockFile; });
        }};
        m_filesystem.UnlockFile(*lockFile->SharedImpl());
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::GetTestDirectory(const rocksdb::IOOptions& options, std::string* path,
                                                               rocksdb::IODebugContext* dbg) {
    return target_->GetTestDirectory(options, path, dbg);
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::NewLogger(const std::string& fname, const rocksdb::IOOptions&,
                                                        std::shared_ptr<rocksdb::Logger>* result,
                                                        rocksdb::IODebugContext*) {
    return Guard("NewLogger", fname, [&]() -> rocksdb::IOStatus {
        auto impl = m_filesystem.CreateLogger(fname);
        *result = Impl::HandleFactory::Create<Azure::Logger>(std::move(impl), m_logger);
        return rocksdb::IOStatus::OK();
    });
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::GetFreeSpace(const std::string&, const rocksdb::IOOptions&,
                                                           uint64_t* diskfree, rocksdb::IODebugContext*) {
    // The container is effectively unbounded, so report a large fixed amount rather than querying the service.
    *diskfree = static_cast<uint64_t>(INT_MAX) * 256; // about a terabyte
    return rocksdb::IOStatus::OK();
}

template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::IsDirectory(const std::string& path, const rocksdb::IOOptions&,
                                                          bool* is_dir, rocksdb::IODebugContext*) {
    return Guard("IsDirectory", path, [&]() -> rocksdb::IOStatus {
        {
            std::scoped_lock lock(m_state.KnownDirectoriesMutex);
            if (m_state.KnownDirectories.contains(BlobFilesystemDetail::NormalizePath(path))) {
                *is_dir = true;
                return rocksdb::IOStatus::OK();
            }
        }
        // A prefix is a directory exactly when something lives beneath it.
        if (m_filesystem.HasChildren(path) == Impl::DirectoryContents::NonEmpty) {
            *is_dir = true;
            return rocksdb::IOStatus::OK();
        }
        if (m_filesystem.FileExists(path) == AzureClient::Existence::Present) {
            *is_dir = false;
            return rocksdb::IOStatus::OK();
        }
        return rocksdb::IOStatus::NotFound();
    });
}

// Reads issued via ReadAsync complete on the host io_context; their callbacks are delivered here on the caller's
// thread. Every handle is waited on, which satisfies any min_completions.
template <class TImpl>
rocksdb::IOStatus BasicBlobFilesystem<TImpl>::Poll(std::vector<void*>& io_handles, size_t) {
    return Impl::PollAsyncReads(io_handles);
}

template <class TImpl> rocksdb::IOStatus BasicBlobFilesystem<TImpl>::AbortIO(std::vector<void*>& io_handles) {
    return Impl::AbortAsyncReads(io_handles);
}

template <class TImpl> void BasicBlobFilesystem<TImpl>::DiscardCacheForDirectory(const std::string&) {}

// Only async reads are advertised: FS-allocated buffers and verify-and-reconstruct are not implemented.
template <class TImpl> void BasicBlobFilesystem<TImpl>::SupportedOps(int64_t& supported_ops) {
    supported_ops = int64_t{1} << rocksdb::FSSupportedOps::kAsyncIO;
}
} // namespace AVEVA::RocksDB::Plugin::Azure
