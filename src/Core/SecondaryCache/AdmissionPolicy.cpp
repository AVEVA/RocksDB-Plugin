// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AdmissionPolicy.hpp"

#include <algorithm>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
namespace {
[[nodiscard]] uint64_t Mix(const uint64_t value) noexcept {
    uint64_t x = value + 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30u)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27u)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31u);
}
} // namespace

AdmissionPolicy::AdmissionPolicy(const AdmissionPolicyKind kind, const uint32_t shardCount)
    : m_kind(kind), m_shards(std::max<uint32_t>(1, shardCount)) {}

bool AdmissionPolicy::ShouldAdmit(const uint64_t keyHash, const bool forceInsert) noexcept {
    if (forceInsert || m_kind == AdmissionPolicyKind::kAdmitAll) {
        Forget(keyHash);
        return true;
    }

    auto& shard = ShardFor(keyHash);
    std::lock_guard lock(shard.mu);
    const auto [it, inserted] = shard.ghosts.insert(keyHash);
    if (inserted) {
        return false;
    }
    shard.ghosts.erase(it);
    return true;
}

void AdmissionPolicy::Forget(const uint64_t keyHash) noexcept {
    auto& shard = ShardFor(keyHash);
    std::lock_guard lock(shard.mu);
    shard.ghosts.erase(keyHash);
}

void AdmissionPolicy::Clear() noexcept {
    for (auto& shard : m_shards) {
        std::lock_guard lock(shard.mu);
        shard.ghosts.clear();
    }
}

AdmissionPolicy::Shard& AdmissionPolicy::ShardFor(const uint64_t keyHash) noexcept {
    return m_shards[Mix(keyHash) % m_shards.size()];
}
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
