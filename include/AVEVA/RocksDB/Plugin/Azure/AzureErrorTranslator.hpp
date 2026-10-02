// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <rocksdb/io_status.h>
namespace AVEVA::RocksDB::Plugin::Azure {
struct AzureErrorTranslator {
    static rocksdb::IOStatus IOStatusFromError(const std::string& context, unsigned int statusCode);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
