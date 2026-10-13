// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Directory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"

#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure {
Directory::Directory(Impl::DirectoryImpl&& directory)
    : m_directory(std::make_unique<Impl::DirectoryImpl>(std::move(directory))) {}

Directory::~Directory() = default;

rocksdb::IOStatus Directory::Fsync(const rocksdb::IOOptions&, rocksdb::IODebugContext*) {
    try {
        m_directory->Fsync();
        return rocksdb::IOStatus::OK();
    } catch (...) {
        return AzureErrorTranslator::FromException(std::current_exception());
    }
}

size_t Directory::GetUniqueId(char* id, size_t max_size) const { return m_directory->GetUniqueId(id, max_size); }
} // namespace AVEVA::RocksDB::Plugin::Azure
