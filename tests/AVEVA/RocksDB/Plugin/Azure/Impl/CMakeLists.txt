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
    LeaseSafetyTests.cpp
    AzureErrorTranslatorTests.cpp
    LockFileImplTests.cpp
    BlobOperationsTests.cpp
    AzureContainerClientTests.cpp
    BlobFilesystemTests.cpp
    BlobFilesystemFakeTests.cpp
    PluginTests.cpp
    BlobHelpersTests.cpp
    TokenCredentialsTests.cpp
)

target_link_libraries(aveva-rocksdb-plugin-azure-impl-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-azure aveva-rocksdb-plugin-azure-impl aveva-rocksdb-plugin-core-mocks aveva::azure-client-testing)
if(MSVC)
    target_compile_options(aveva-rocksdb-plugin-azure-impl-tests PRIVATE /bigobj)
endif()
include(GoogleTest)
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 15)

# Live-Azure tests skip themselves unless AVEVA_RUN_AZURE_INTEGRATION_TESTS=1, so they live in their own executable.
add_executable(aveva-rocksdb-plugin-azure-impl-integration-tests
    IntegrationTestHelpers.cpp
    ReadableFileIntegrationTests.cpp
    WriteableFileIntegrationTests.cpp
    ReadWriteFileIntegrationTests.cpp
    BlobFilesystemIntegrationTests.cpp
    PluginSecondaryCacheIntegrationTests.cpp
)
target_link_libraries(aveva-rocksdb-plugin-azure-impl-integration-tests PRIVATE GTest::gtest_main GTest::gmock aveva-rocksdb-plugin-azure aveva-rocksdb-plugin-azure-impl aveva-rocksdb-plugin-core-mocks aveva::azure-client-testing)
if(MSVC)
    target_compile_options(aveva-rocksdb-plugin-azure-impl-integration-tests PRIVATE /bigobj)
endif()
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-integration-tests DISCOVERY_TIMEOUT 60 PROPERTIES LABELS integration)
