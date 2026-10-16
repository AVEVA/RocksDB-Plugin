cmake_minimum_required(VERSION 3.22)

option(AVEVA_ROCKSDB_BUILD_DB_BENCH
    "Build the aveva_db_bench wrapper around RocksDB's db_bench tool. \
Requires the vcpkg overlay port in infrastructure/vcpkg-overlays/rocksdb to \
be active; it installs RocksDB's db_bench_tool as a static library (via \
0004-install-db-bench-tool-lib.patch) that the wrapper links against." OFF)

if (${AVEVA_ROCKSDB_BUILD_DB_BENCH})
    list(APPEND VCPKG_MANIFEST_FEATURES "db-bench")
endif()

project(aveva-rocksdb-plugin LANGUAGES CXX VERSION 0.0.1)
option(AVEVA_ROCKSDB_TESTS "Build tests for plugins" ON)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
    set(aveva_cachelib_default ON)
else()
    set(aveva_cachelib_default OFF)
endif()
option(AVEVA_ROCKSDB_WITH_CACHELIB "Build the CacheLib (Navy) secondary cache backend (Linux x64 only)" ${aveva_cachelib_default})
if(AVEVA_ROCKSDB_WITH_CACHELIB AND NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "AVEVA_ROCKSDB_WITH_CACHELIB is only supported on Linux x64")
endif()
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/infrastructure/cmake")
add_subdirectory(src)
if (${AVEVA_ROCKSDB_TESTS})
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
endif()
if (${AVEVA_ROCKSDB_BUILD_DB_BENCH})
    add_subdirectory(tools)
endif()
