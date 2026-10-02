#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <cstddef>
#include <iostream>
#include <system_error>

int main()
{
    // Exercises both header-only constants and the compiled error category from the installed library.
    const std::error_code error = AVEVA::AzureClient::BlobStorageErrorCode::BlobNotFound;
    const bool ok = AVEVA::AzureClient::DefaultUploadBlockSize == std::size_t{4} * 1024U * 1024U &&
                    error == std::errc::no_such_file_or_directory;
    std::cout << (ok ? "install consumer ok\n" : "install consumer FAILED\n");
    return ok ? 0 : 1;
}
