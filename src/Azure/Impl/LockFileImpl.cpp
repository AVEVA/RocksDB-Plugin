// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "LockFileImpl.ipp"

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template class BasicLockFileImpl<PageBlobClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
