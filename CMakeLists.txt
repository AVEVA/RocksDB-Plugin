cmake_minimum_required(VERSION 3.22)
project(aveva-rocksdb-plugin LANGUAGES CXX VERSION 0.0.1)
option(AVEVA_ROCKSDB_TESTS "Build tests for plugins" ON)
option(AVEVA_ROCKSDB_BUILD_DB_BENCH
    "Build the aveva_db_bench wrapper around RocksDB's db_bench tool. \
Requires the vcpkg overlay port in infrastructure/vcpkg-overlays/rocksdb to \
be active; it installs RocksDB's db_bench_tool as a static library (via \
0004-install-db-bench-tool-lib.patch) that the wrapper links against." OFF)
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
