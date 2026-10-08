# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

# Builds the in-tree aveva-http-client and aveva-azure-client libraries (libs/) as part of this project
# and makes them consumable both in the build tree and from an installed package.
include_guard(GLOBAL)

set(AVEVA_HTTP_CLIENT_TESTS ${AVEVA_ROCKSDB_TESTS})
set(AVEVA_HTTP_CLIENT_BENCHMARKS OFF)
# Installed by aveva_install_library() using the library's own config template.
set(AVEVA_HTTP_CLIENT_INSTALL_CONFIG_FILE_PACKAGE ON)
add_subdirectory("${PROJECT_SOURCE_DIR}/libs/HttpClient" "${PROJECT_BINARY_DIR}/libs/HttpClient")

# aveva-azure-client resolves aveva-http-client through find_package(); point it at a shim that reuses the
# target built above instead of looking for an installed package.
set(_aveva_http_client_shim_dir "${PROJECT_BINARY_DIR}/cmake/aveva-http-client")
file(WRITE "${_aveva_http_client_shim_dir}/aveva-http-client-config.cmake"
    "if(NOT TARGET aveva::http-client)\n"
    "    message(FATAL_ERROR \"aveva::http-client must be built in-tree before aveva-azure-client.\")\n"
    "endif()\n")
set(aveva-http-client_DIR "${_aveva_http_client_shim_dir}" CACHE INTERNAL "In-tree aveva-http-client package shim")

set(AVEVA_AZURE_CLIENT_TESTS ${AVEVA_ROCKSDB_TESTS})
set(AVEVA_AZURE_CLIENT_EXAMPLES OFF)
set(AVEVA_AZURE_CLIENT_BENCHMARKS OFF)
set(AVEVA_AZURE_CLIENT_INSTALL_TEST OFF)
set(AVEVA_AZURE_CLIENT_INSTALL_CONFIG_FILE_PACKAGE ON)
add_subdirectory("${PROJECT_SOURCE_DIR}/libs/AzureClient" "${PROJECT_BINARY_DIR}/libs/AzureClient")

# Header-only test support (FakeHttpClient.hpp, TestFixtures.hpp) of the vendored AzureClient tests, shared with
# the plugin tests. All dependence on the library's test layout is confined to this target.
add_library(aveva-azure-client-test-support INTERFACE)
target_include_directories(aveva-azure-client-test-support INTERFACE "${PROJECT_SOURCE_DIR}/libs/AzureClient/tests")
target_link_libraries(aveva-azure-client-test-support INTERFACE aveva::azure-client)

# Asio types are shared across the plugin and both libraries, so they must agree on the Windows API level.
foreach(_aveva_lib IN ITEMS aveva-http-client aveva-azure-client)
    target_compile_definitions(${_aveva_lib} PUBLIC $<$<PLATFORM_ID:Windows>:_WIN32_WINNT=0x0A00>)
endforeach()
