cmake_minimum_required(VERSION 3.22)
project(aveva-rocksdb-plugin LANGUAGES CXX VERSION 0.0.1)
option(AVEVA_ROCKSDB_TESTS "Build tests for plugins" ON)
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/infrastructure/cmake")

# In-tree Azure Blob Storage client libraries (libs/). Only the libraries themselves are built;
# their own tests, examples and benchmarks are not part of this project.
include(infrastructure/cmake/AvevaClientLibraries.cmake)

add_subdirectory(src)
if (${AVEVA_ROCKSDB_TESTS})
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
endif()
