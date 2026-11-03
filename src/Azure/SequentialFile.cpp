// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "BasicSequentialFile.hpp"

namespace AVEVA::RocksDB::Plugin::Azure {
template class BasicSequentialFile<Impl::SequentialFileImpl>;
} // namespace AVEVA::RocksDB::Plugin::Azure
