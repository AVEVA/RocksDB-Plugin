// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Detail/BlobDownload.hpp>

#include <cerrno>
#include <expected>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient::Private::DownloadDetail {
[[nodiscard]] BlobStorageError MakeFailure(std::errc error, std::string message) {
    const std::error_code code = std::make_error_code(error);
    return MakeClientError(code, std::move(message));
}

[[nodiscard]] BlobStorageError MakeFailure(std::error_code error, std::string message) {
    return MakeClientError(error, std::move(message));
}

[[nodiscard]] std::error_code LastOpenError() noexcept {
    return errno != 0 ? std::error_code{errno, std::generic_category()} : std::make_error_code(std::errc::io_error);
}

[[nodiscard]] BlobStorageError MakeInvalidResponse(std::string message) {
    return MakeClientError(make_error_code(BlobStorageErrorCode::InvalidResponse), std::move(message));
}

[[nodiscard]] std::optional<BlobStorageError> ToFailure(std::error_code error, const HttpResponse& response) {
    RequestFailure const failure = DetermineBlobStorageFailure(error, response);
    if (!failure.Error) {
        return std::nullopt;
    }
    BlobStorageError details = failure.Details.value_or(MakeClientError(failure.Error));
    details.Code = failure.Error;
    return details;
}

[[nodiscard]] std::string TakeBody(HttpResponse& response) {
    std::string body = std::move(response).GetBody();
    return body;
}

[[nodiscard]] Response<Models::DownloadBlobToResult> ToDownloadToResponse(DownloadSummary summary) {
    Models::DownloadBlobToResult result;
    result.Properties = std::move(summary.Properties);
    result.BytesWritten = summary.BytesWritten;
    result.ContentRange = summary.ContentRange;
    return Response<Models::DownloadBlobToResult>{std::move(result), std::move(summary.FirstResponse), std::nullopt};
}

[[nodiscard]] std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>
FinalizeFileDownload(std::ofstream& file, const std::filesystem::path& tempPath, const std::filesystem::path& target,
                     SummaryResult result) {
    using FileResult = std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>;
    file.close();
    std::error_code ignored;
    if (!result.has_value()) {
        std::filesystem::remove(tempPath, ignored);
        return FileResult{std::unexpect, std::move(result).error()};
    }
    if (file.fail()) {
        std::filesystem::remove(tempPath, ignored);
        return FileResult{std::unexpect, MakeFailure(std::errc::io_error, "I/O error writing destination file.")};
    }
    std::error_code renameError;
    if (std::filesystem::is_directory(target, renameError)) {
        std::filesystem::remove(tempPath, ignored);
        return FileResult{std::unexpect, MakeFailure(std::make_error_code(std::errc::is_a_directory),
                                                     "The destination path is a directory.")};
    }
    renameError.clear();
    std::filesystem::rename(tempPath, target, renameError);
    if (renameError) {
        std::filesystem::remove(tempPath, ignored);
        return FileResult{std::unexpect,
                          MakeFailure(renameError, "Failed to rename temporary download file to destination.")};
    }
    return ToDownloadToResponse(std::move(*result));
}
} // namespace AVEVA::AzureClient::Private::DownloadDetail
