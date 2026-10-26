add_executable(aveva-rocksdb-plugin-azure-impl-tests
    ImplTests.cpp
    AsyncReadTests.cpp
    WriteableFileTests.cpp
    ReadableFileTests.cpp
    SequentialFileTests.cpp
    ReadWriteFileTests.cpp
    FakeBlobServerTests.cpp
    RangeCheckTests.cpp
    FakeBlobServer.cpp
    BufferChunkInfoTests.cpp
    LogRateLimiterTests.cpp
    LoggerTests.cpp
    ClientRuntimeTests.cpp
    ClientFactoryTests.cpp
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
    PublicHeadersTests.cpp
    SourceCompatTests.cpp
    FakeInstantiations.cpp
)

target_link_libraries(aveva-rocksdb-plugin-azure-impl-tests PRIVATE GTest::gtest GTest::gmock aveva-rocksdb-plugin-azure aveva-rocksdb-plugin-azure-impl aveva-rocksdb-plugin-core-mocks aveva::azure-client-testing)
if(MSVC)
    target_compile_options(aveva-rocksdb-plugin-azure-impl-tests PRIVATE /bigobj)
endif()
# The template bodies (*.ipp) are private to the library, so the tests that instantiate them over fakes reach them
# through the source tree.
target_include_directories(aveva-rocksdb-plugin-azure-impl-tests PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../src/Azure"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../src/Azure/Impl")
include(GoogleTest)
# Every test keeps the `unit` label and also gets one subsystem label (file-io, filesystem, lock,
# runtime) so a subsystem can be run alone with `ctest -L <label>`. Suites map to subsystems by
# name; `*Suite.*` also matches parameterised "Prefix/Suite" names. `runtime` is the catch-all, so a
# new suite is never left undiscovered.
set(aveva_impl_file_io_suites
    AsyncReadRequestTests AsyncReadTests HeldRequestsTests BufferChunkInfoTests RangeCheckTests
    ReadableFileTests ReadWriteFileImplTests PageBlobWriterTests SequentialFileTests WriteableFileTests)
set(aveva_impl_filesystem_suites
    AzureContainerClientTests BlobFilesystemFakeRuntimeTests BlobFilesystemFakeTests CachedFailureTests
    PageBoundaryTests RenameFailureTests BlobFilesystemTests BlobHelpersCredentialLifetimeTests
    BlobHelpersTests CredentialChainTests FileSizeMetadataTests PageRoundingTests BlobOperationsFakeBlobTests
    BlobOperationsTests DownloadToFileTests DownloadBufferPoolTests PluginTests)
set(aveva_impl_lock_suites
    AsyncReadTrackerDrainTests LeaseDeadlineTests WriteDeadlineTests WriteFenceTests
    LeaseRenewalLoopBackoffTests LeaseRenewalLoopTests LeaseRenewalLoopThrowTests LockFileImplTests)
function(aveva_impl_suite_filter out)
    set(parts "")
    foreach(suite IN LISTS ARGN)
        list(APPEND parts "*${suite}.*")
    endforeach()
    list(JOIN parts ":" joined)
    set(${out} "${joined}" PARENT_SCOPE)
endfunction()
aveva_impl_suite_filter(aveva_impl_file_io_filter ${aveva_impl_file_io_suites})
aveva_impl_suite_filter(aveva_impl_filesystem_filter ${aveva_impl_filesystem_suites})
aveva_impl_suite_filter(aveva_impl_lock_filter ${aveva_impl_lock_suites})
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 60
    TEST_FILTER "${aveva_impl_file_io_filter}"
    PROPERTIES TIMEOUT 300 LABELS "unit;file-io")
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 60
    TEST_FILTER "${aveva_impl_filesystem_filter}"
    PROPERTIES TIMEOUT 300 LABELS "unit;filesystem")
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 60
    TEST_FILTER "${aveva_impl_lock_filter}"
    PROPERTIES TIMEOUT 300 LABELS "unit;lock")
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-tests DISCOVERY_TIMEOUT 60
    TEST_FILTER "-${aveva_impl_file_io_filter}:${aveva_impl_filesystem_filter}:${aveva_impl_lock_filter}"
    PROPERTIES TIMEOUT 300 LABELS "unit;runtime")

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
target_include_directories(aveva-rocksdb-plugin-azure-impl-integration-tests PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../src/Azure")
gtest_discover_tests(aveva-rocksdb-plugin-azure-impl-integration-tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 600 LABELS integration RESOURCE_LOCK azure_integration_account)
