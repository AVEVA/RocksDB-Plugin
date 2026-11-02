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
/// <summary>
/// The result of creating a blob container.
/// </summary>
struct CreateBlobContainerResult {
    /// <summary>
    /// The container's ETag.
    /// </summary>
    std::string ETag;

    /// <summary>
    /// When the container was last modified.
    /// </summary>
    std::chrono::system_clock::time_point LastModified;
};

/// <summary>
/// Outcome of one blob in a batch delete.
/// </summary>
struct DeleteBlobItemResult {
    /// <summary>
    /// The name of the blob this outcome is for.
    /// </summary>
    std::string Name;

    /// <summary>
    /// Engaged when this blob could not be deleted. A blob that was already absent is not a failure.
    /// </summary>
    std::optional<BlobStorageError> Error;

    /// <summary>
    /// Whether the service answered BlobNotFound, i.e. there was nothing to delete.
    /// </summary>
    PriorState Prior = PriorState::Existed;

    /// <summary>
    /// Whether this blob's delete succeeded.
    /// </summary>
    /// <returns><c>Completion::Complete</c> when no error is recorded; otherwise
    /// <c>Completion::Incomplete</c>.</returns>
    [[nodiscard]] Completion Succeeded() const noexcept {
        return Error.has_value() ? Completion::Incomplete : Completion::Complete;
    }
};

/// <summary>
/// One entry per requested blob name, in request order.
/// </summary>
struct DeleteBlobsResult {
    /// <summary>
    /// The per-blob outcomes.
    /// </summary>
    std::vector<DeleteBlobItemResult> Items;

    /// <summary>
    /// Whether every blob of the batch was deleted (or already absent).
    /// </summary>
    /// <returns><c>Completion::Complete</c> when no item failed; otherwise <c>Completion::Incomplete</c>.</returns>
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
