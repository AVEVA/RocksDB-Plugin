// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "BasicReadWriteFile.hpp"

namespace AVEVA::RocksDB::Plugin::Azure {
template class BasicReadWriteFile<Impl::ReadWriteFileImpl>;
} // namespace AVEVA::RocksDB::Plugin::Azure
