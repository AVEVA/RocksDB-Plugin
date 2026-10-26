# Standalone consumer of the installed package, written against the public API of `main`. Run by the install-consume job
# in .github/workflows/build-and-test.yml; it is not part of the main build.
cmake_minimum_required(VERSION 3.25)
project(main_api_consumer CXX)
find_package(aveva-rocksdb-plugin-azure CONFIG REQUIRED)
add_executable(main_api_consumer main_api_consumer.cpp)
target_link_libraries(main_api_consumer PRIVATE aveva::rocksdb-plugin-azure)
target_compile_features(main_api_consumer PRIVATE cxx_std_23)
