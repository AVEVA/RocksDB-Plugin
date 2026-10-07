# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2025 AVEVA

cmake_minimum_required(VERSION 3.22)

option(AVEVA_ROCKSDB_BUILD_DB_BENCH
    "Build the aveva_db_bench wrapper around RocksDB's db_bench tool. \
Requires the vcpkg overlay port in infrastructure/vcpkg-overlays/rocksdb to \
be active; it installs RocksDB's db_bench_tool as a static library (via \
0004-install-db-bench-tool-lib.patch) that the wrapper links against." OFF)

if (${AVEVA_ROCKSDB_BUILD_DB_BENCH})
    list(APPEND VCPKG_MANIFEST_FEATURES "db-bench")
endif()

project(aveva-rocksdb-plugin LANGUAGES CXX VERSION 0.1.0)
option(AVEVA_ROCKSDB_TESTS "Build tests for the plugins and the vendored AzureClient/HttpClient libraries" ON)
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/infrastructure/cmake")

# In-tree Azure Blob Storage client libraries (libs/). Their own tests are built together with the plugin
# tests (AVEVA_ROCKSDB_TESTS); examples and benchmarks never are.
include(infrastructure/cmake/AvevaClientLibraries.cmake)

add_subdirectory(src)
if (${AVEVA_ROCKSDB_TESTS})
    include(CTest)
    enable_testing()
    add_subdirectory(tests)

    # Installs to a scratch prefix and builds tests/package through find_package(). Needs a fully built tree.
    option(AVEVA_ROCKSDB_PACKAGE_TEST "Add a CTest that installs the plugin and builds tests/package" OFF)
    if (AVEVA_ROCKSDB_PACKAGE_TEST)
        add_test(
            NAME PackageConsumer
            COMMAND ${CMAKE_COMMAND}
                "-DBUILD_DIR=${CMAKE_BINARY_DIR}"
                "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests/package"
                "-DCONFIG=$<CONFIG>"
                "-DGENERATOR=${CMAKE_GENERATOR}"
                "-DDEPENDENCY_PREFIX=${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}"
                "-DWORK_DIR=${CMAKE_BINARY_DIR}/package-consumer"
                "-DTOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/package/RunPackageTest.cmake")
        set_tests_properties(PackageConsumer PROPERTIES TIMEOUT 900 LABELS "install")
    endif()
endif()
if (${AVEVA_ROCKSDB_BUILD_DB_BENCH})
    add_subdirectory(tools)
endif()
