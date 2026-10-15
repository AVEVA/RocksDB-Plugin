// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "SequentialFileImpl.ipp"

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template class BasicSequentialFileImpl<PageBlobClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
