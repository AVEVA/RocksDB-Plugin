// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
namespace AVEVA::RocksDB::Plugin::Core {
namespace {
std::string_view LastPathComponent(const std::string_view pathname) {
    static const constexpr std::string_view pathsep{"/"};
    if (const size_t offset = pathname.find_last_of(pathsep); offset != std::string_view::npos) {
        return pathname.substr(offset + 1);
    }
    return pathname;
}
} // namespace

RocksDBHelpers::FileClass RocksDBHelpers::GetFileType(const std::string_view pathname) {
    // Is this a sst file, i.e. ends in ".sst" or ".ldb"
    if (pathname.ends_with(FileType::sst) || pathname.ends_with(FileType::ldb)) {
        return FileClass::SST;
    }

    // A log file has ".log" suffix
    if (pathname.ends_with(FileType::log)) {
        return FileClass::WAL;
    }

    if (LastPathComponent(pathname).starts_with("MANIFEST")) {
        return FileClass::Manifest;
    }

    if (LastPathComponent(pathname).starts_with("IDENTITY")) {
        return FileClass::Identity;
    }

    return FileClass::Directory;
}
} // namespace AVEVA::RocksDB::Plugin::Core
