// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Outcomes.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace AVEVA::AzureClient::Models {
struct CreateBlobContainerResult {
    std::string ETag;
    std::chrono::system_clock::time_point LastModified;
};

// Outcome of one blob in a batch delete.
struct DeleteBlobItemResult {
    std::string Name;
    // Engaged when this blob could not be deleted. A blob that was already absent is not a failure.
    std::optional<BlobStorageError> Error;
    // Whether the service answered BlobNotFound, i.e. there was nothing to delete.
    PriorState Prior = PriorState::Existed;

    [[nodiscard]] Completion Succeeded() const noexcept {
        return Error.has_value() ? Completion::Incomplete : Completion::Complete;
    }
};

// One entry per requested blob name, in request order.
struct DeleteBlobsResult {
    std::vector<DeleteBlobItemResult> Items;

    [[nodiscard]] Completion AllSucceeded() const noexcept {
        for (const DeleteBlobItemResult& item : Items) {
            if (item.Succeeded() == Completion::Incomplete) {
                return Completion::Incomplete;
            }
        }
        return Completion::Complete;
    }
};
} // namespace AVEVA::AzureClient::Models
