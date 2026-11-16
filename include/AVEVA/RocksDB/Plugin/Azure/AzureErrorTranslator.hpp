// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <rocksdb/io_status.h>
#include <azure/core/http/http_status_code.hpp>
namespace AVEVA::RocksDB::Plugin::Azure
{
    struct AzureErrorTranslator
    {
        // Status for a missing file/blob. RocksDB's secondary tolerates a WAL purged by the primary only when
        // IsPathNotFound() is true; existing IsNotFound() checks keep working.
        static rocksdb::IOStatus PathNotFound(const std::string& context = {});

        static rocksdb::IOStatus IOStatusFromError(const std::string& context, const ::Azure::Core::Http::HttpStatusCode& statusCode);
    };
}
