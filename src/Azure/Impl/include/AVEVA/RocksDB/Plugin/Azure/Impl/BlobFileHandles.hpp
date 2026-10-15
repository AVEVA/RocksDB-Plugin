// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// The objects every blob-backed file shares: its name, the runtime and client it talks through, the optional file
/// cache and the logger. Grouping them gives the file classes one defaulted move instead of five hand-written
/// member moves, and one place that fixes the destruction order.
/// </summary>
template <typename TBlob> struct BlobFileHandles {
    using LoggerPtr = std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>;

    BlobFileHandles(std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
                    std::shared_ptr<Core::FileCache> fileCache, LoggerPtr logger)
        : Name(name), Runtime(std::move(runtime)), Blob(std::move(blob)), FileCache(std::move(fileCache)),
          Logger(std::move(logger)) {}

    std::string Name;
    // Declared before the client so that it is destroyed after it: the client references the runtime's HTTP client.
    std::shared_ptr<ClientRuntimeBase> Runtime;
    std::shared_ptr<TBlob> Blob;
    std::shared_ptr<Core::FileCache> FileCache;
    LoggerPtr Logger;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
