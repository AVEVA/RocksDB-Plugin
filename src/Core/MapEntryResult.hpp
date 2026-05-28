// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "LruFileIndex.hpp"

#include <memory>
#include <optional>

namespace AVEVA::RocksDB::Plugin::Core
{
    class MappedFileView;

    /// <summary>
    /// Result of attempting to map a cache entry for reading.
    /// Status indicates whether the file was missing, corrupt, or successfully mapped.
    /// When <c>status</c> is <c>Status::Ok</c> the <c>view</c> holds a non-null
    /// mapped-file view pointing at the file's contents, and <c>pin</c> keeps the
    /// LRU entry pinned for the lifetime of this result to prevent eviction while
    /// the caller is reading through the view.
    /// </summary>
    struct MapEntryResult
    {
        enum class Status { Miss, Corrupt, Ok };
        Status status;
        std::unique_ptr<MappedFileView> view; // non-null only when status == Ok
        std::optional<LruFileIndex::ScopedPin> pin; // keeps entry pinned while view is live
    };
}
