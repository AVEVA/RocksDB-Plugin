// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"

#include <rocksdb/db.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class LockFile : public rocksdb::FileLock
    {
        std::shared_ptr<Impl::LockFileImpl> m_lock;
    public:
        explicit LockFile(std::shared_ptr<Impl::LockFileImpl> lock);
        bool Lock();
        void Renew() const;
        void Unlock();

        Impl::LockFileImpl& GetImpl() const;
    };
}
