// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Directory.hpp"
namespace AVEVA::RocksDB::Plugin::Azure
{
    Directory::Directory(Impl::DirectoryImpl directory)
        : m_directory(std::move(directory))
    {
    }

    rocksdb::IOStatus Directory::Fsync(const rocksdb::IOOptions&, rocksdb::IODebugContext*)
    {
        try
        {
            m_directory.Fsync();
            return rocksdb::IOStatus::OK();
        }
        catch (const ::Azure::Core::RequestFailedException& e)
        {
            return AzureErrorTranslator::IOStatusFromError(e.Message, e.StatusCode);
        }
        catch (const std::exception& e)
        {
            return rocksdb::IOStatus::IOError(e.what());
        }
        catch (...)
        {
            return rocksdb::IOStatus::IOError("Unknown Fsync error occurred");
        }
    }

    size_t Directory::GetUniqueId(char* id, size_t max_size) const
    {
        return m_directory.GetUniqueId(id, max_size);
    }
}
