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

    void LockFile::unlink()
    {
        boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::unlink();
    }

    bool LockFile::is_linked()
    {
        return boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::is_linked();
    }
}
