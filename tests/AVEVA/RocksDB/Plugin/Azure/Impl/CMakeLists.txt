add_executable(aveva-rocksdb-plugin-azure-impl-tests
    ImplTests.cpp
    WriteableFileTests.cpp
)

target_link_libraries(aveva-rocksdb-plugin-azure-impl-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-azure-impl)
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests)
