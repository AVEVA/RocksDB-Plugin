add_executable(aveva-rocksdb-plugin-azure-impl-tests
    ImplTests.cpp
    AsyncReadTests.cpp
    WriteableFileTests.cpp
    ReadableFileTests.cpp
    ReadWriteFileTests.cpp
    BufferChunkInfoTests.cpp
    LogRateLimiterTests.cpp
    ClientRuntimeTests.cpp
    AzureErrorTranslatorTests.cpp
    LockFileImplTests.cpp
    PageBlobTests.cpp
    BlobFilesystemTests.cpp
    BlobHelpersTests.cpp
    TokenCredentialsTests.cpp
    IntegrationTestHelpers.cpp
    ReadableFileIntegrationTests.cpp
    WriteableFileIntegrationTests.cpp
    ReadWriteFileIntegrationTests.cpp
    BlobFilesystemIntegrationTests.cpp
    PluginSecondaryCacheIntegrationTests.cpp
)

target_link_libraries(aveva-rocksdb-plugin-azure-impl-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-azure aveva-rocksdb-plugin-core-mocks)
# Reuse the fake HTTP transport and fixtures of the vendored AzureClient tests.
target_include_directories(aveva-rocksdb-plugin-azure-impl-tests PRIVATE "${PROJECT_SOURCE_DIR}/libs/AzureClient/tests")
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 60)
