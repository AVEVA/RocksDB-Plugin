// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <rocksdb/secondary_cache.h>

#include <cstddef>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>
    /// Small immediately-ready SecondaryCacheResultHandle implementation used by
    /// FileBasedCompressedSecondaryCache to return reconstructed objects.
    /// </summary>
    class ResultHandle final : public rocksdb::SecondaryCacheResultHandle
    {
    public:
        /// <summary>
        /// Constructs a ready result handle that will return ownership of the
        /// reconstructed cached object via <c>Value()</c> and report the object's
        /// charge via <c>Size()</c>.
        /// </summary>
        /// <param name="value">Pointer to the reconstructed cache object. Ownership
        /// is transferred to the caller when <c>Value()</c> is invoked.</param>
        /// <param name="charge">Charge (size in bytes) associated with the object.</param>
        ResultHandle(rocksdb::Cache::ObjectPtr value, size_t charge);

        /// <summary>Virtual dtor; defaulted.</summary>
        ~ResultHandle() override;

        ResultHandle(const ResultHandle&) = delete;
        ResultHandle& operator=(const ResultHandle&) = delete;
        ResultHandle(ResultHandle&&) = delete;
        ResultHandle& operator=(ResultHandle&&) = delete;

        /// <summary>
        /// Returns true when the result is ready. This implementation is always
        /// immediately ready and therefore returns true.
        /// </summary>
        /// <returns>True.</returns>
        bool IsReady() noexcept override;

        /// <summary>No-op wait; the result is always ready.</summary>
        void Wait() noexcept override;

        /// <summary>
        /// Transfers ownership of the stored object pointer to the caller and
        /// returns it. Subsequent calls return nullptr.
        /// </summary>
        /// <returns>Pointer to the cached object or nullptr if already taken.</returns>
        rocksdb::Cache::ObjectPtr Value() noexcept override;

        /// <summary>Returns the charge (size in bytes) of the stored object.</summary>
        /// <returns>The charge passed to the constructor.</returns>
        size_t Size() noexcept override;

    private:
        /// <summary>
        /// Pointer to the reconstructed cache object. Ownership is transferred to
        /// the caller by a call to <c>Value()</c> which sets this field to nullptr.
        /// </summary>
        rocksdb::Cache::ObjectPtr m_value;

        /// <summary>
        /// Charge (size in bytes) associated with the reconstructed object. Reported
        /// via <c>Size()</c>.
        /// </summary>
        size_t m_charge;
    };
}
