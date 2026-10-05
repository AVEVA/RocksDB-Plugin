// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "BlobClientMock.hpp"
namespace AVEVA::RocksDB::Plugin::Core::Mocks {
BlobClientMock::BlobClientMock() {
    // Unless a test expects GetMetadata itself, it is served from the GetSize and GetEtag expectations.
    ON_CALL(*this, GetMetadata()).WillByDefault([this] { return BlobMetadata{GetSize(), GetEtag()}; });
}

BlobClientMock::~BlobClientMock() {}
} // namespace AVEVA::RocksDB::Plugin::Core::Mocks
