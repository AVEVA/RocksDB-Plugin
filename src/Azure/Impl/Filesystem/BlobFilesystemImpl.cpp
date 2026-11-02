// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "Impl/Filesystem/BlobFilesystemImpl.ipp"

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template class BasicBlobFilesystemImpl<::AVEVA::HttpClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
