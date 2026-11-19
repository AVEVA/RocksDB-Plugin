# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

find_package(aveva-http-client CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(boost_asio CONFIG REQUIRED)
find_package(boost_json CONFIG REQUIRED)
find_package(boost_url CONFIG REQUIRED)
find_package(boost_uuid CONFIG REQUIRED)
find_package(Microsoft.GSL CONFIG REQUIRED)
find_package(LibXml2 CONFIG REQUIRED)

add_library(aveva-azure-client STATIC
    BlobClientOptions.cpp
    BlobBatch.cpp
    BlobDownload.cpp
    BlobRequestHelpers.cpp
    SharedKeySigner.cpp
    BlobXmlParser.cpp
    BlobUrl.cpp
    BlobResponseParsers.cpp
    Credentials.cpp
    BlobStorageErrorCategory.cpp
    TokenEndpointError.cpp
    RequestPipeline.cpp
    ClientInstantiations.cpp
)

set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../include")

add_library(aveva::azure-client ALIAS aveva-azure-client)
# Lets the static library link into shared plugins on Linux.
set_target_properties(aveva-azure-client PROPERTIES POSITION_INDEPENDENT_CODE ON)
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
        "${base-include-dir}/AVEVA/AzureClient/Detail/AggregateDeadline.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/BlobBatch.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/BlobDownload.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/ClientCore.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/RequestPipeline.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/DownloadSinks.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Detail/ProtocolConstants.hpp"
        "${base-include-dir}/AVEVA/AzureClient/AzureRequestOptions.hpp"
        "${base-include-dir}/AVEVA/AzureClient/HttpClientLike.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Outcomes.hpp"
        "${base-include-dir}/AVEVA/AzureClient/TokenCredential.hpp"
        "${base-include-dir}/AVEVA/AzureClient/TokenEndpointError.hpp"
        "${base-include-dir}/AVEVA/AzureClient/ClientForwardDeclarations.hpp"
        "${base-include-dir}/AVEVA/AzureClient/PageBlobClient.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Response.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Types.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Credentials.hpp"
        "${base-include-dir}/AVEVA/AzureClient/CredentialVariant.hpp"
        "${base-include-dir}/AVEVA/AzureClient/ChainedTokenCredential.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Models/BlobContainerModels.hpp"
        "${base-include-dir}/AVEVA/AzureClient/Models/BlobModels.hpp"
)
# Every header under include/ is installed, so a public header can never include one the package lacks: the
# FILE_SET above must list exactly the headers on disk.
file(GLOB_RECURSE aveva_azure_client_headers_on_disk CONFIGURE_DEPENDS "${base-include-dir}/*.hpp")
get_target_property(aveva_azure_client_file_set aveva-azure-client HEADER_SET)
foreach(header_list aveva_azure_client_headers_on_disk aveva_azure_client_file_set)
    set(normalized "")
    foreach(header IN LISTS ${header_list})
        cmake_path(NORMAL_PATH header)
        list(APPEND normalized "${header}")
    endforeach()
    list(SORT normalized)
    set(${header_list} "${normalized}")
endforeach()
if(NOT aveva_azure_client_headers_on_disk STREQUAL aveva_azure_client_file_set)
    set(missing ${aveva_azure_client_headers_on_disk})
    list(REMOVE_ITEM missing ${aveva_azure_client_file_set})
    set(stale ${aveva_azure_client_file_set})
    list(REMOVE_ITEM stale ${aveva_azure_client_headers_on_disk})
    message(FATAL_ERROR
        "aveva-azure-client FILE_SET HEADERS does not match include/.\n"
        "Missing from FILE_SET: ${missing}\nListed but not on disk: ${stale}")
endif()

target_include_directories(aveva-azure-client
    PUBLIC
        "$<BUILD_INTERFACE:${base-include-dir}>"
        "$<INSTALL_INTERFACE:include>"
)

target_compile_features(aveva-azure-client PUBLIC cxx_std_23)
# Warnings are errors for this target even when it is configured without the repository presets.
set_target_properties(aveva-azure-client PROPERTIES COMPILE_WARNING_AS_ERROR ON)
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
        Microsoft.GSL::GSL
    PRIVATE
        Boost::json
        Boost::url
        OpenSSL::Crypto
        Boost::uuid
        LibXml2::LibXml2
)
aveva_install_library(aveva-azure-client)
