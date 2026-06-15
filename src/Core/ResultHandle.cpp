// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "ResultHandle.hpp"

namespace AVEVA::RocksDB::Plugin::Core
{
    ResultHandle::ResultHandle(rocksdb::Cache::ObjectPtr value, size_t charge)
        : m_value(value), m_charge(charge)
    {
    }

    ResultHandle::~ResultHandle() = default;

    bool ResultHandle::IsReady() noexcept
    {
        return true;
    }

    void ResultHandle::Wait() noexcept {}

    rocksdb::Cache::ObjectPtr ResultHandle::Value() noexcept
    {
        return m_value;
    }

    size_t ResultHandle::Size() noexcept
    {
        return m_charge;
    }
}
