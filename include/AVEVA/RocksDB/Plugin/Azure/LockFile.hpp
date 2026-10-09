// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <rocksdb/db.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class LockFileImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure {
class LockFile : public rocksdb::FileLock {
    std::shared_ptr<Impl::LockFileImpl> m_lock;

  public:
    explicit LockFile(std::shared_ptr<Impl::LockFileImpl> lock);
    ~LockFile() override;
    // Returns false if the lease is already held; throws RequestFailedException on any other failure.
    bool Lock();
    void Renew() const;
    void Unlock();

    Impl::LockFileImpl& GetImpl() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
