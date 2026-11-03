// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// The member bodies of the file implementations are in the Impl headers. The production instantiations live in the
// library; this is where the tests instantiate them over the fake blob client.
#include "FakeBlobEnvironment.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"

#include "BasicReadableFile.hpp"

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
template class BasicReadableFileImpl<FakeBlobServer::BlobClientType>;
template class BasicWriteableFileImpl<FakeBlobServer::BlobClientType>;
template class BasicReadWriteFileImpl<FakeBlobServer::BlobClientType>;
template class BasicSequentialFileImpl<FakeBlobServer::BlobClientType>;
template class BasicLockFileImpl<FakeBlobServer::BlobClientType>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests

namespace AVEVA::RocksDB::Plugin::Azure {
template class BasicReadableFile<Impl::Tests::FakeReadableFileImpl>;
template class BasicReadableFile<Impl::Tests::SlowProbeReadableFileImpl>;
} // namespace AVEVA::RocksDB::Plugin::Azure
