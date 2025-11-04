#pragma once
#include <rocksdb/io_status.h>
#include <azure/core/http/http_status_code.hpp>
namespace AVEVA::RocksDB::Plugin::Azure
{
    struct AzureErrorTranslator
    {
        static rocksdb::IOStatus IOStatusFromError(const std::string& context, const ::Azure::Core::Http::HttpStatusCode& statusCode);
    };
}
