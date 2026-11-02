// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/file_system.h>

#include <functional>
#include <memory>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class ReadableFileImpl;
}

namespace AVEVA::RocksDB::Plugin::Azure {
class BlobFilesystem;

/// <summary>
/// A random access file over a page blob. The implementation (and with it Boost.Asio and the Azure client) stays
/// behind a pimpl, so this header pulls in nothing beyond RocksDB and Boost.Log.
/// </summary>
class ReadableFile : public rocksdb::FSRandomAccessFile {
  protected:
    // Only the library's implementation class derives from this; instances come from
    // BlobFilesystem::NewRandomAccessFile.
    ReadableFile() = default;

  public:
    ~ReadableFile() override = default;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
