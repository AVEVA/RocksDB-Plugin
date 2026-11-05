// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "BasicReadableFile.hpp"

namespace AVEVA::RocksDB::Plugin::Azure {
template class BasicReadableFile<Impl::ReadableFileImpl>;

} // namespace AVEVA::RocksDB::Plugin::Azure
