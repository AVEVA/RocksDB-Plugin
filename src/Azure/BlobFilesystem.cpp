// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "BasicBlobFilesystem.hpp"

namespace AVEVA::RocksDB::Plugin::Azure {
template class BasicBlobFilesystem<Impl::BlobFilesystemImpl>;
} // namespace AVEVA::RocksDB::Plugin::Azure
