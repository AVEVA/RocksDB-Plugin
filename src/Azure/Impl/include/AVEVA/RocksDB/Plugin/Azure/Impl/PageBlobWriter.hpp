// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// What the writeable and read/write files have in common: the blob handles, the capacity tracked locally (so no
/// round trip is needed to learn it), growth of that capacity, publishing the logical size, and invalidating the
/// cached copy. Both files own one so these rules cannot drift apart, and moving a file moves the writer.
/// </summary>
template <typename TBlob> class PageBlobWriter {
  public:
    PageBlobWriter(std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
                   std::shared_ptr<Core::FileCache> fileCache, BlobFileHandles<TBlob>::LoggerPtr logger,
                   const int64_t capacity)
        : H(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger)), m_capacity(capacity) {}

    /// <summary>The handles, public because the files use them directly for reads and logging.</summary>
    BlobFileHandles<TBlob> H;

    [[nodiscard]] int64_t Capacity() const noexcept { return m_capacity; }

    /// <summary>
    /// Grows the blob so that `requiredCapacity` fits. Doubling alone under-sizes the blob when the capacity is zero
    /// (after Truncate(0)) or smaller than the pending write, so the required size is always honoured; the doubling
    /// keeps repeated writes past the end from each paying for a resize request.
    /// </summary>
    void EnsureCapacity(const int64_t requiredCapacity) {
        if (requiredCapacity <= m_capacity) {
            return;
        }
        const auto wanted = std::max({requiredCapacity, m_capacity * 2, Configuration::PageBlob::DefaultSize});
        const auto [_, desiredSize] = BlobHelpers::RoundToEndOfNearestPage(wanted);

        BOOST_LOG_SEV(*H.Logger, boost::log::trivial::debug)
            << "Expanding file '" << H.Name << "' to " << desiredSize << " bytes";
        Resize(desiredSize);
    }

    /// <summary>Records a capacity already known from the service (no request is made).</summary>
    void AdoptCapacity(const int64_t capacity) noexcept { m_capacity = capacity; }

    /// <summary>Sets the blob's capacity exactly (Truncate shrinks it), and tracks it.</summary>
    void Resize(const int64_t capacity) {
        BlobOperations::SetCapacity(*H.Runtime, *H.Blob, capacity);
        m_capacity = capacity;
    }

    /// <summary>
    /// Records the logical size in the blob's metadata. The physical length is page-rounded, so this is a separate
    /// write; it changes the ETag, which is why callers skip it when nothing new is to be published.
    /// </summary>
    void PublishSize(const int64_t size) { BlobOperations::SetSize(*H.Runtime, *H.Blob, size); }

    /// <summary>Marks the cached copy stale before the blob changes, so a reader never serves the old bytes.</summary>
    void MarkStale() const {
        if (H.FileCache) {
            H.FileCache->MarkFileAsStaleIfExists(H.Name);
        }
    }

  private:
    int64_t m_capacity;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
