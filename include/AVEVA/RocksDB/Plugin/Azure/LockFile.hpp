// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"

#include <boost/intrusive/list.hpp>
#include <rocksdb/db.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class LockFile : public rocksdb::FileLock, public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
    {
        std::shared_ptr<Impl::LockFileImpl> m_lock;
    public:
        explicit LockFile(std::shared_ptr<Impl::LockFileImpl> lock);
        bool Lock();
        void Renew() const;
        void Unlock();

        void unlink();
        bool is_linked();
    };
}
