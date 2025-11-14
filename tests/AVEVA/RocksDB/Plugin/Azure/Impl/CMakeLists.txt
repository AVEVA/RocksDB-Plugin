add_executable(aveva-rocksdb-plugin-azure-impl-tests
    ImplTests.cpp
    WriteableFileTests.cpp
    ReadableFileTests.cpp
    ReadWriteFileTests.cpp
    BufferChunkInfoTests.cpp
    IntegrationTestHelpers.cpp
    ReadableFileIntegrationTests.cpp
    WriteableFileIntegrationTests.cpp
    ReadWriteFileIntegrationTests.cpp
    BlobFilesystemIntegrationTests.cpp
)

target_link_libraries(aveva-rocksdb-plugin-azure-impl-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-azure-impl aveva-rocksdb-plugin-core-mocks)
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 60)
