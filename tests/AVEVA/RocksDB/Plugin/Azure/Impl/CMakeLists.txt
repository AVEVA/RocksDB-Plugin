add_executable(aveva-rocksdb-plugin-azure-impl-tests
    ImplTests.cpp
    AsyncReadTests.cpp
    WriteableFileTests.cpp
    ReadableFileTests.cpp
    SequentialFileTests.cpp
    ReadWriteFileTests.cpp
    BufferChunkInfoTests.cpp
    LogRateLimiterTests.cpp
    ClientRuntimeTests.cpp
    BlockOnTests.cpp
    LeaseSafetyTests.cpp
    AzureErrorTranslatorTests.cpp
    LockFileImplTests.cpp
    PageBlobTests.cpp
    BlobFilesystemTests.cpp
    PluginTests.cpp
    BlobHelpersTests.cpp
    TokenCredentialsTests.cpp
    IntegrationTestHelpers.cpp
    ReadableFileIntegrationTests.cpp
    WriteableFileIntegrationTests.cpp
    ReadWriteFileIntegrationTests.cpp
    BlobFilesystemIntegrationTests.cpp
    PluginSecondaryCacheIntegrationTests.cpp
)

target_link_libraries(aveva-rocksdb-plugin-azure-impl-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-azure aveva-rocksdb-plugin-azure-impl aveva-rocksdb-plugin-core-mocks aveva-azure-client-test-support)
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 60)
