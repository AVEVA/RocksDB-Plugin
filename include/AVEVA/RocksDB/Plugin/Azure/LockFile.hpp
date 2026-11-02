// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <rocksdb/db.h>

#include <cstdint>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class LockFileImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure {
class LockFile : public rocksdb::FileLock {
    std::shared_ptr<Impl::LockFileImpl> m_lock;

  public:
    enum class LockResult : std::uint8_t {
        Acquired,
        HeldElsewhere,
    };

    explicit LockFile(std::shared_ptr<Impl::LockFileImpl> lock);
    ~LockFile() override;
    // Returns HeldElsewhere if the lease is already held; throws RequestFailedException on any other failure.
    [[nodiscard]] LockResult Lock();
    void Renew() const;
    void Unlock();

    [[nodiscard]] std::shared_ptr<Impl::LockFileImpl> SharedImpl() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
