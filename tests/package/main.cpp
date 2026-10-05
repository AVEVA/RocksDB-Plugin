// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

#include <iostream>

namespace {
using Logger = std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>;
using Info = AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;
using RegisterFn = rocksdb::Status (*)(rocksdb::ConfigOptions&, rocksdb::Env**, std::shared_ptr<rocksdb::Env>*,
                                       boost::asio::io_context&, Info, std::optional<Info>, Logger, int64_t, int64_t,
                                       std::optional<std::string_view>, size_t);
} // namespace

// Taking the address of Plugin::Register forces the link against the plugin, impl, core and client packages.
int main() {
    volatile RegisterFn registerFn = &AVEVA::RocksDB::Plugin::Azure::Plugin::Register;
    std::cout << (registerFn != nullptr ? "ok" : "null") << '\n';
    return registerFn != nullptr ? 0 : 1;
}
