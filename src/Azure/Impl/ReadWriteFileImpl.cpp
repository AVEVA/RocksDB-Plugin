// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "ReadWriteFileImpl.ipp"

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template class BasicReadWriteFileImpl<PageBlobClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
