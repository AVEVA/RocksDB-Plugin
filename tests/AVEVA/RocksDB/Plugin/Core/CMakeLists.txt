add_subdirectory(Mocks)
add_executable(aveva-rocksdb-plugin-core-tests
    CoreTests.cpp
    FileCacheTests.cpp
)

target_link_libraries(aveva-rocksdb-plugin-core-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-core aveva-rocksdb-plugin-core-mocks)
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-core-tests)
