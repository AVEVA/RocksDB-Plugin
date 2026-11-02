// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Consumer code written against the public API of the plugin. It includes only installed public headers and must keep
// compiling. It is built in-tree by SourceCompatTests and against the installed package by the install-consume CI job
// (tests/consumer/CMakeLists.txt). See "Source and binary compatibility" in README.md.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>

#include <memory>
#include <optional>
#include <string>
#include <type_traits>

namespace AVEVA::RocksDB::Plugin::ConsumerFixtures {
namespace Azure = AVEVA::RocksDB::Plugin::Azure;

// The registration call with each credential kind, through the single Plugin::Register entry point.
inline rocksdb::Status RegisterServicePrincipalLikeMain(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                                        std::shared_ptr<rocksdb::Env>* guard,
                                                        std::shared_ptr<boost::asio::io_context> ioContext,
                                                        const std::string& accountUrl) {
    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    Azure::Models::ServicePrincipalStorageInfo primary{"mydb", accountUrl, "client", "secret", "tenant"};
    return Azure::Plugin::Register(
        configOptions, env, guard,
        Azure::Plugin::RegisterOptions{
            .IoContext = std::move(ioContext),
            .Accounts = Azure::Plugin::AccountPair<Azure::Models::ServicePrincipalStorageInfo>{std::move(primary),
                                                                                               std::nullopt},
            .Log = std::move(logger)});
}

inline rocksdb::Status RegisterChainedCredentialLikeMain(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                                         std::shared_ptr<rocksdb::Env>* guard,
                                                         std::shared_ptr<boost::asio::io_context> ioContext,
                                                         const std::string& accountUrl) {
    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    Azure::Models::ChainedCredentialInfo primary{"mydb", accountUrl, "client", "secret", "tenant"};
    return Azure::Plugin::Register(
        configOptions, env, guard,
        Azure::Plugin::RegisterOptions{
            .IoContext = std::move(ioContext),
            .Accounts =
                Azure::Plugin::AccountPair<Azure::Models::ChainedCredentialInfo>{std::move(primary), std::nullopt},
            .Log = std::move(logger)});
}

// Normal RocksDB file usage: everything goes through the rocksdb::FileSystem interface the registered filesystem
// implements; consumers never name the Azure file classes. Compiled and linked, never executed without an account.
inline rocksdb::IOStatus UseFilesLikeMain(rocksdb::FileSystem& fs) {
    const rocksdb::IOOptions options;
    const rocksdb::FileOptions fileOptions;
    char scratch[64];
    rocksdb::Slice result;

    std::unique_ptr<rocksdb::FSWritableFile> writable;
    if (auto s = fs.NewWritableFile("db/000001.sst", fileOptions, &writable, nullptr); !s.ok()) {
        return s;
    }
    if (auto s = writable->Append(rocksdb::Slice("data"), options, nullptr); !s.ok()) {
        return s;
    }
    if (auto s = writable->Sync(options, nullptr); !s.ok()) {
        return s;
    }
    if (auto s = writable->Close(options, nullptr); !s.ok()) {
        return s;
    }

    std::unique_ptr<rocksdb::FSRandomAccessFile> random;
    if (auto s = fs.NewRandomAccessFile("db/000001.sst", fileOptions, &random, nullptr); !s.ok()) {
        return s;
    }
    if (auto s = random->Read(0, sizeof(scratch), options, &result, scratch, nullptr); !s.ok()) {
        return s;
    }

    std::unique_ptr<rocksdb::FSSequentialFile> sequential;
    if (auto s = fs.NewSequentialFile("db/CURRENT", fileOptions, &sequential, nullptr); !s.ok()) {
        return s;
    }
    if (auto s = sequential->Read(sizeof(scratch), options, &result, scratch, nullptr); !s.ok()) {
        return s;
    }

    std::unique_ptr<rocksdb::FSRandomRWFile> readWrite;
    if (auto s = fs.NewRandomRWFile("db/LOG", fileOptions, &readWrite, nullptr); !s.ok()) {
        return s;
    }
    if (auto s = readWrite->Write(0, rocksdb::Slice("x"), options, nullptr); !s.ok()) {
        return s;
    }
    return readWrite->Close(options, nullptr);
}

// The Azure file types remain usable as RocksDB file interfaces, which is all main's public headers promised.
static_assert(std::is_base_of_v<rocksdb::FSRandomAccessFile, Azure::ReadableFile>);
static_assert(std::is_base_of_v<rocksdb::FSRandomRWFile, Azure::ReadWriteFile>);
static_assert(std::is_base_of_v<rocksdb::FSWritableFile, Azure::WriteableFile>);
} // namespace AVEVA::RocksDB::Plugin::ConsumerFixtures
