// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
enum class AdmissionPolicyKind : uint8_t {
    kAdmitAll,
    kSecondChance,
};

class AdmissionPolicy {
  public:
    explicit AdmissionPolicy(AdmissionPolicyKind kind, uint32_t shardCount = 64);

    [[nodiscard]] bool ShouldAdmit(uint64_t keyHash, bool forceInsert) noexcept;
    void Forget(uint64_t keyHash) noexcept;
    void Clear() noexcept;

  private:
    struct Shard {
        std::mutex mu;
        std::unordered_set<uint64_t> ghosts;
    };

    [[nodiscard]] Shard& ShardFor(uint64_t keyHash) noexcept;

    AdmissionPolicyKind m_kind;
    std::vector<Shard> m_shards;
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
