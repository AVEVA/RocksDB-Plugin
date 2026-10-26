// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Built against the installed package by the install-consume CI job. The runtime guard keeps the calls from
// executing (there is no storage account) but the linker must still resolve every symbol from the installed library.
#include "MainApiConsumer.hpp"

#include <rocksdb/convenience.h>

// The implementation headers that moved out of the public tree are intentionally not installed.
#if __has_include("AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp") ||                                           \
                  __has_include("AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp") ||                           \
                                __has_include("AVEVA/RocksDB/Plugin/Core/BlobClient.hpp")
#error "Implementation headers must not be part of the installed package."
#endif

int main(int argc, char**) {
    using namespace AVEVA::RocksDB::Plugin::ConsumerFixtures;
    if (argc > 100) {
        rocksdb::ConfigOptions configOptions;
        rocksdb::Env* env = nullptr;
        std::shared_ptr<rocksdb::Env> guard;
        const auto first =
            RegisterServicePrincipalLikeMain(configOptions, &env, &guard, "https://account.blob.core.windows.net");
        const auto second =
            RegisterChainedCredentialLikeMain(configOptions, &env, &guard, "https://account.blob.core.windows.net");
        if (!first.ok() || !second.ok() || !UseFilesLikeMain(*env->GetFileSystem()).ok()) {
            return 1;
        }
    }
    return 0;
}
