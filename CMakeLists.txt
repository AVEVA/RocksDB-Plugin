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
endif()
if (${AVEVA_ROCKSDB_BUILD_DB_BENCH})
    add_subdirectory(tools)
endif()
