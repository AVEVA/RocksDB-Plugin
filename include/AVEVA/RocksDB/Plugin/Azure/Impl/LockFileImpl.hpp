// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <azure/storage/blobs/page_blob_client.hpp>
#include <azure/storage/blobs/blob_lease_client.hpp>
#include <memory>
#include <chrono>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class LockFileImpl
    {
        std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> m_file;
        std::unique_ptr<::Azure::Storage::Blobs::BlobLeaseClient> m_lease = nullptr;
        mutable std::chrono::steady_clock::time_point m_lastRenewalTime;
        std::chrono::seconds m_leaseLength;

    public:
        LockFileImpl(std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> file, std::chrono::seconds leaseLength);
        bool Lock();
        void Renew() const;
        void Unlock();

        [[nodiscard]] std::chrono::seconds TimeSinceLastRenewal() const;
        [[nodiscard]] bool HasExceededLeaseLength() const;
    };
}
