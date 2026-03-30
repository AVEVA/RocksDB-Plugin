// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <memory>

namespace AVEVA::RocksDB::Plugin::Core
{
    class MappedFileView;

    /// <summary>
    /// Result of attempting to map a cache entry for reading.
    /// Status indicates whether the file was missing, corrupt, or successfully mapped.
    /// When <c>status</c> is <c>Status::Ok</c> the <c>view</c> holds a non-null
    /// mapped-file view pointing at the file's contents.
    /// </summary>
    struct MapEntryResult
    {
        enum class Status { Miss, Corrupt, Ok };
        Status status;
        std::unique_ptr<MappedFileView> view; // non-null only when status == Ok
    };
}
