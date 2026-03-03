// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstddef>
#include <cstdint>
namespace AVEVA::RocksDB::Plugin::Core
{
    class File
    {
    public:
        virtual ~File() = default;

        virtual int64_t Read(char* buffer, int64_t offset, int64_t length) = 0;
    };

    /// <summary>
    /// A read-only view of a memory-mapped file.  The mapping remains valid until
    /// this object is destroyed.  The production implementation wraps
    /// <c>boost::interprocess::mapped_region</c>; test implementations can back
    /// the view with an in-memory buffer.
    /// </summary>
    class MappedFileView
    {
    public:
        virtual ~MappedFileView() = default;

        /// <summary>Returns a pointer to the start of the mapped data.</summary>
        virtual const char* Data() const noexcept = 0;
        /// <summary>Returns the number of mapped bytes.</summary>
        virtual size_t Size() const noexcept = 0;
    };
}
