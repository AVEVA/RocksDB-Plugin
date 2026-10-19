// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

// Part of BlobFilesystemImpl.ipp: included at its end, never on its own.
#pragma once

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename THttp>
std::shared_ptr<typename BasicBlobFilesystemImpl<THttp>::LockFileType>
BasicBlobFilesystemImpl<THttp>::LockFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(*m_runtime, blob, Configuration::PageBlob::DefaultSize);
    auto lockFile = std::make_shared<LockFileType>(
        m_runtime, std::make_unique<PageBlobClientOn<THttp>>(std::move(blob)), Configuration::LeaseLength, m_logger,
        std::string(realPath), m_lockAcquireTimeout);
    // Lock() can retry for the full lease length. The mutex must not be held across it, or the renewal thread
    // would stall and the leases this Env already holds would expire.
    if (lockFile->Lock()) {
        // The lease is held from here on, so a failure while registering it must not leave it orphaned until it
        // expires: nobody else holds a reference that could release it.
        boost::scope::scope_fail releaseLease{[&] {
            try {
                {
                    std::scoped_lock _(m_lockFilesMutex);
                    lockFile->unlink();
                    std::erase_if(m_renewableLocks, [&lockFile](const std::weak_ptr<LockFileType>& weak) {
                        const auto shared = weak.lock();
                        return !shared || shared == lockFile;
                    });
                }
                lockFile->Unlock();
            } catch (const std::exception& ex) {
                BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
                    << "Could not release the lease on '" << realPath << "' after registering it failed: " << ex.what();
            } catch (...) {
                BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
                    << "Could not release the lease on '" << realPath << "' after registering it failed";
            }
        }};
        {
            std::scoped_lock _(m_lockFilesMutex);
            m_renewableLocks.push_back(lockFile);
            m_locks.push_back(*lockFile);
            assert(lockFile->is_linked());
        }
        // Purged only once the lease is registered for renewal, since the listing can outlast the lease.
        PurgeOrphanedRenameTemps(prefix, realPath);
        return lockFile;
    } else {
        const auto message =
            std::format("The targeted storage location is locked: container '{}', path '{}'", prefix, realPath);
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning) << message;
        throw std::runtime_error(message);
    }
}

// Deliberately skips EnsureLiveness: once writes are fenced the lease may already be lost, and releasing whatever is
// still held is exactly what shutdown must still be able to do. Releasing is never a write to the data.
template <typename THttp> void BasicBlobFilesystemImpl<THttp>::UnlockFile(LockFileType& lock) {
    {
        std::scoped_lock _(m_lockFilesMutex);
        lock.unlink();
        std::erase_if(m_renewableLocks, [&lock](const std::weak_ptr<LockFileType>& weak) {
            const auto shared = weak.lock();
            return !shared || shared.get() == &lock;
        });
    }
    // Released outside the list mutex. Unlock marks the lock as releasing first, so a renewal already in flight that
    // the service rejects because the release won is treated as benign rather than fatal.
    lock.Unlock();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
