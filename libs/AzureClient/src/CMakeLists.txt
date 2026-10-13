# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

find_package(aveva-http-client CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(boost_algorithm CONFIG REQUIRED)
find_package(boost_asio CONFIG REQUIRED)
find_package(boost_json CONFIG REQUIRED)
find_package(boost_url CONFIG REQUIRED)
find_package(boost_uuid CONFIG REQUIRED)
find_package(LibXml2 CONFIG REQUIRED)

add_library(aveva-azure-client
    BlobClient.cpp
    BlobClientOptions.cpp
    BlobContainerClient.cpp
    BlobBatch.cpp
    BlobRequestHelpers.cpp
    SharedKeySigner.cpp
    BlobXmlParser.cpp
    BlobUrl.cpp
    BlobResponseParsers.cpp
    BlobServiceClient.cpp
    Credentials.cpp
    BlobStorageErrorCategory.cpp
    BlobModels.cpp
    BlockBlobClient.cpp
    PageBlobClient.cpp
    RequestPipeline.cpp
)

set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../include")

add_library(aveva::azure-client ALIAS aveva-azure-client)
if(BUILD_SHARED_LIBS)
    message(FATAL_ERROR "aveva-azure-client supports static linking only.")
endif()
if(AVEVA_AZURE_CLIENT_TESTS)
    target_compile_definitions(aveva-azure-client PRIVATE AVEVA_AZURE_CLIENT_TESTING)
endif()
aveva_azure_client_enable_clang_tidy(aveva-azure-client)
aveva_azure_client_instrument(aveva-azure-client)
target_sources(aveva-azure-client
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS
        "${base-include-dir}"
    FILES
        "${base-include-dir}/AVEVA/AzureClient/AzureClient.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlobClient.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlobClientOptions.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlobContainerClient.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlobOperationOptions.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlobServiceClient.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlobStorageError.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlobStorageErrorCode.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlockBlobClient.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/BlobBatch.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/BlobDownload.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/RequestPipeline.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/DownloadSinks.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/DownloadWriterPool.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/ProtocolConstants.hpp"
        "${base-include-dir}/AVEVA/AzureClient/HttpClientLike.hpp"
        "${base-include-dir}/AVEVA/AzureClient/TokenCredential.hpp"
        "${base-include-dir}/AVEVA/AzureClient/ClientForwardDeclarations.hpp"
        "${base-include-dir}/AVEVA/AzureClient/PageBlobClient.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Response.hpp"
        "${base-include-dir}/AVEVA/AzureClient/WithRequestOptions.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Credentials.hpp"
        "${base-include-dir}/AVEVA/AzureClient/CredentialVariant.hpp"
        "${base-include-dir}/AVEVA/AzureClient/ChainedTokenCredential.hpp"
        "${base-include-dir}/AVEVA/AzureClient/BlockOn.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Models/BlobContainerModels.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Models/BlobModels.hpp"
)
target_include_directories(aveva-azure-client
    PUBLIC
        "$<BUILD_INTERFACE:${base-include-dir}>"
        "$<INSTALL_INTERFACE:include>"
)

target_compile_features(aveva-azure-client PUBLIC cxx_std_23)
if(MSVC)
    target_compile_options(aveva-azure-client PRIVATE /W4 /permissive-)
else()
    target_compile_options(aveva-azure-client PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
endif()
# The Detail headers hold the client template definitions (consumers instantiate them for their own credential
# types), so the libraries they include are public. OpenSSL and LibXml2 stay private (used only in .cpp files).
target_link_libraries(aveva-azure-client
    PUBLIC
        aveva::http-client
        Boost::asio
        Boost::json
        Boost::url
    PRIVATE
        OpenSSL::Crypto
        Boost::algorithm
        Boost::uuid
        LibXml2::LibXml2
)
aveva_install_library(aveva-azure-client)
