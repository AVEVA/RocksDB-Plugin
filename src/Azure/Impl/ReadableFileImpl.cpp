// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "ReadableFileImpl.ipp"

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template class BasicReadableFileImpl<PageBlobClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
