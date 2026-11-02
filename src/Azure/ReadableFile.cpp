// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "BasicReadableFile.ipp"
#include "Impl/ReadableFileImpl.ipp" // ReadAsync/Prefetch are templates on the completion, so they instantiate here

namespace AVEVA::RocksDB::Plugin::Azure {
template class BasicReadableFile<Impl::ReadableFileImpl>;

} // namespace AVEVA::RocksDB::Plugin::Azure
