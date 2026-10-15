cmake_minimum_required(VERSION 3.22)

option(AVEVA_ROCKSDB_BUILD_DB_BENCH
    "Build the aveva_db_bench wrapper around RocksDB's db_bench tool. \
Requires the vcpkg overlay port in infrastructure/vcpkg-overlays/rocksdb to \
be active; it installs RocksDB's db_bench_tool as a static library (via \
0004-install-db-bench-tool-lib.patch) that the wrapper links against." OFF)

if (${AVEVA_ROCKSDB_BUILD_DB_BENCH})
    list(APPEND VCPKG_MANIFEST_FEATURES "db-bench")
    # The patched RocksDB port is only needed for db_bench; ordinary builds use the baseline port. This must be set
    # before project(), where the vcpkg toolchain reads it.
    list(APPEND VCPKG_OVERLAY_PORTS "${CMAKE_CURRENT_SOURCE_DIR}/infrastructure/vcpkg-overlays/rocksdb")
endif()

project(aveva-rocksdb-plugin LANGUAGES CXX VERSION 0.2.0)
option(AVEVA_ROCKSDB_TESTS "Build tests for the plugins and the vendored AzureClient/HttpClient libraries" ON)
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/infrastructure/cmake")

# Sanitizers instrument the plugin and both vendored libraries (not vcpkg dependencies). Values: "address",
# "undefined", "address;undefined" or "thread" (thread cannot be combined with address).
set(AVEVA_ROCKSDB_SANITIZER "" CACHE STRING "Sanitizers to build the plugin and in-tree libraries with")
if(AVEVA_ROCKSDB_SANITIZER)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        message(FATAL_ERROR "AVEVA_ROCKSDB_SANITIZER is only supported with Clang or GCC.")
    endif()
    if("thread" IN_LIST AVEVA_ROCKSDB_SANITIZER AND "address" IN_LIST AVEVA_ROCKSDB_SANITIZER)
        message(FATAL_ERROR "AVEVA_ROCKSDB_SANITIZER cannot combine thread with address.")
    endif()
    list(JOIN AVEVA_ROCKSDB_SANITIZER "," _aveva_sanitizer_list)
    add_compile_options(-fsanitize=${_aveva_sanitizer_list} -fno-omit-frame-pointer)
    add_link_options(-fsanitize=${_aveva_sanitizer_list})
    # Keep the AzureClient test-suite sanitizer handling in step with the global flags.
    if("address" IN_LIST AVEVA_ROCKSDB_SANITIZER)
        set(AVEVA_AZURE_CLIENT_ENABLE_ASAN ON CACHE BOOL "" FORCE)
    else()
        set(AVEVA_AZURE_CLIENT_ENABLE_ASAN OFF CACHE BOOL "" FORCE)
    endif()
    set(AVEVA_AZURE_CLIENT_ENABLE_UBSAN OFF CACHE BOOL "" FORCE)
    set(AVEVA_AZURE_CLIENT_ENABLE_TSAN OFF CACHE BOOL "" FORCE)
    if("undefined" IN_LIST AVEVA_ROCKSDB_SANITIZER)
        set(AVEVA_AZURE_CLIENT_ENABLE_UBSAN ON CACHE BOOL "" FORCE)
    endif()
    if("thread" IN_LIST AVEVA_ROCKSDB_SANITIZER)
        set(AVEVA_AZURE_CLIENT_ENABLE_TSAN ON CACHE BOOL "" FORCE)
    endif()
endif()

# In-tree Azure Blob Storage client libraries (libs/). Their own tests are built together with the plugin
# tests (AVEVA_ROCKSDB_TESTS); examples and benchmarks never are.
include(infrastructure/cmake/AvevaClientLibraries.cmake)

add_subdirectory(src)
if (${AVEVA_ROCKSDB_TESTS})
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
endif()
if (${AVEVA_ROCKSDB_BUILD_DB_BENCH})
    add_subdirectory(tools)
endif()
