// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <memory>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// The only creator of the public handle types (BlobFilesystem, Directory, Logger, LockFile) whose constructors take
/// implementation types consumers cannot name. Their constructors are private and befriend this, so creating one is
/// only possible from the plugin library and its tests.
/// </summary>
struct HandleFactory {
    template <class THandle, class... TArgs> [[nodiscard]] static std::unique_ptr<THandle> Create(TArgs&&... args) {
        return std::unique_ptr<THandle>(new THandle(std::forward<TArgs>(args)...));
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
