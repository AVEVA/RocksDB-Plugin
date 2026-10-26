// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Consumer code written against the public API of `main` (before the io_context was injected). It includes only
// installed public headers and must keep compiling, with at most deprecation warnings, to keep source compatibility.
// It is built in-tree by SourceCompatTests and against the installed package by the install-consume CI job
// (tests/consumer/CMakeLists.txt). See "Source and binary compatibility" in README.md.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadWriteFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/WriteableFile.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>

#include <memory>
#include <optional>
#include <string>
#include <type_traits>

#if defined(_MSC_VER)
#define AVEVA_CONSUMER_ALLOW_DEPRECATED_BEGIN __pragma(warning(push)) __pragma(warning(disable : 4996))
#define AVEVA_CONSUMER_ALLOW_DEPRECATED_END __pragma(warning(pop))
#else
#define AVEVA_CONSUMER_ALLOW_DEPRECATED_BEGIN                                                                          \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#define AVEVA_CONSUMER_ALLOW_DEPRECATED_END _Pragma("GCC diagnostic pop")
#endif

namespace AVEVA::RocksDB::Plugin::ConsumerFixtures {
namespace Azure = AVEVA::RocksDB::Plugin::Azure;

// The registration calls as main documented them: no io_context, positional trailing arguments.
inline rocksdb::Status RegisterServicePrincipalLikeMain(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                                        std::shared_ptr<rocksdb::Env>* guard,
                                                        const std::string& accountUrl) {
    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    Azure::Models::ServicePrincipalStorageInfo primary{"mydb", accountUrl, "client", "secret", "tenant"};
    std::optional<Azure::Models::ServicePrincipalStorageInfo> backup;
    AVEVA_CONSUMER_ALLOW_DEPRECATED_BEGIN
    return Azure::Plugin::Register(configOptions, env, guard, primary, backup, logger,
                                   Azure::Impl::Configuration::PageBlob::DefaultBufferSize,
                                   Azure::Impl::Configuration::PageBlob::DefaultSize, std::nullopt, 0);
    AVEVA_CONSUMER_ALLOW_DEPRECATED_END
}

inline rocksdb::Status RegisterChainedCredentialLikeMain(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                                         std::shared_ptr<rocksdb::Env>* guard,
                                                         const std::string& accountUrl) {
    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    Azure::Models::ChainedCredentialInfo primary{"mydb", accountUrl, "client", "secret", "tenant"};
    AVEVA_CONSUMER_ALLOW_DEPRECATED_BEGIN
    return Azure::Plugin::Register(configOptions, env, guard, primary, std::nullopt, logger);
    AVEVA_CONSUMER_ALLOW_DEPRECATED_END
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
