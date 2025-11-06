add_executable(aveva-rocksdb-plugin-core-tests
    FileCacheTests.cpp
    FilesystemMock.cpp
    FilesystemMock.hpp
)

target_link_libraries(aveva-rocksdb-plugin-core-tests PRIVATE GTest::gtest_main aveva-rocksdb-plugin-core)
