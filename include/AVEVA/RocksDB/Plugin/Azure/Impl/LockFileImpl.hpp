// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/intrusive/list.hpp>
#include <azure/storage/blobs/page_blob_client.hpp>
#include <azure/storage/blobs/blob_lease_client.hpp>
#include <boost/log/trivial.hpp>
#include <string>
#include <memory>
#include <chrono>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class LockFileImpl : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
    {
        std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> m_file;
        std::unique_ptr<::Azure::Storage::Blobs::BlobLeaseClient> m_lease = nullptr;
        mutable std::chrono::steady_clock::time_point m_lastRenewalTime;
        std::chrono::seconds m_leaseLength;
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
        std::string m_fileName;

    public:
        LockFileImpl(
            std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> file,
            std::chrono::seconds leaseLength,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            std::string fileName);
        bool Lock();
        void Renew() const;
        void Unlock();

        [[nodiscard]] std::chrono::seconds TimeSinceLastRenewal() const;
        [[nodiscard]] bool HasExceededLeaseLength() const;

        void unlink();
        bool is_linked();
    };
}
