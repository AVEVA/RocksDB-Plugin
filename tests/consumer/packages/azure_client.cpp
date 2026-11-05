#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

int main(int argc, char**) {
    if (argc > 100) {
        return AVEVA::AzureClient::make_error_code(AVEVA::AzureClient::BlobStorageErrorCode::BlobNotFound).value();
    }
    return 0;
}
