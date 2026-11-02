// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <rocksdb/db.h>

#include <cstdint>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class LockFileImpl;
struct HandleFactory;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
namespace AVEVA::RocksDB::Plugin::Azure {
class BlobFilesystem;

class LockFile : public rocksdb::FileLock {
    friend struct Impl::HandleFactory; // the only creator: the constructor takes an implementation type
    friend class BlobFilesystem;       // the only user of SharedImpl
    std::shared_ptr<Impl::LockFileImpl> m_lock;

    explicit LockFile(std::shared_ptr<Impl::LockFileImpl> lock);
    [[nodiscard]] std::shared_ptr<Impl::LockFileImpl> SharedImpl() const;

  public:
    enum class LockResult : std::uint8_t {
        Acquired,
        HeldElsewhere,
    };

    ~LockFile() override;
    // Returns HeldElsewhere if the lease is already held; throws RequestFailedException on any other failure.
    [[nodiscard]] LockResult Lock();
    void Renew() const;
    void Unlock();
};
} // namespace AVEVA::RocksDB::Plugin::Azure
