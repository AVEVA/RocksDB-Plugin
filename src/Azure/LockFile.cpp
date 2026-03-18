// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/LockFile.hpp"
namespace AVEVA::RocksDB::Plugin::Azure
{
    LockFile::LockFile(std::shared_ptr<Impl::LockFileImpl> lock)
        : m_lock(std::move(lock))
    {
    }

    bool LockFile::Lock()
    {
        return m_lock->Lock();
    }

    void LockFile::Renew() const
    {
        m_lock->Renew();
    }

    void LockFile::Unlock()
    {
        m_lock->Unlock();
    }

    Impl::LockFileImpl& LockFile::GetImpl() const
    {
        return *m_lock;
    }
}
