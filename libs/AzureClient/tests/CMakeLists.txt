find_package(aveva-http-client CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS asio beast)
find_package(GTest CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

include(GoogleTest)

add_test(NAME VersionConsistency
    COMMAND ${CMAKE_COMMAND} -DSOURCE_DIR=${PROJECT_SOURCE_DIR} -DPROJECT_VERSION=${PROJECT_VERSION}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/CheckVersions.cmake)
set_tests_properties(VersionConsistency PROPERTIES LABELS "unit;version")

add_executable(aveva-azure-client-tests
    AppendBlobClientTests.cpp
    AsyncBehaviorTests.cpp
    AzureIntegrationTestHelpers.cpp
    AzureIntegrationTestHelpersTests.cpp
    BlobContainerClientTests.cpp
    BlobFeatureSmokeTests.cpp
    BlobRequestHelpersTests.cpp
    RetryTests.cpp
    ParseContentRangeTests.cpp
    RequestOptionsTests.cpp
    OptionValidationTests.cpp
    RedactionTests.cpp
    UploadConcurrencyTests.cpp
    DownloadToFileTests.cpp
    EndpointValidationTests.cpp
    SharedKeyUploadTests.cpp
    ConcurrentUploadTests.cpp
    UploadRobustnessTests.cpp
    HeaderInjectionTests.cpp
    UploadBufferTests.cpp
    UploadMemoryTests.cpp
    ParallelDownloadTests.cpp
    TestEventListener.cpp
    BlobServiceClientTests.cpp
    CredentialsTests.cpp
    BlobStorageErrorCategoryTests.cpp
    BlockBlobClientTests.cpp
    ITokenCredentialTests.cpp
    ModelTests.cpp
    LeaseTests.cpp
    MetadataValidationTests.cpp
    fuzz/FuzzHarnessTests.cpp
    fuzz/ParseXmlFuzz.cpp
    ParserRobustnessTests.cpp
    RequestShapeTests.cpp
    SasTests.cpp
    SuppressedErrorResponseTests.cpp
    TagsAndVersionsTests.cpp
    PageBlobClientTests.cpp
    ResponseTests.cpp
)
target_include_directories(aveva-azure-client-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_link_libraries(aveva-azure-client-tests
    PRIVATE aveva::azure-client aveva::http-client Boost::asio Boost::beast GTest::gtest OpenSSL::SSL OpenSSL::Crypto
)
aveva_azure_client_enable_clang_tidy(aveva-azure-client-tests)
aveva_azure_client_instrument(aveva-azure-client-tests)
if(WIN32)
    target_link_libraries(aveva-azure-client-tests PRIVATE Crypt32)
endif()
if(MSVC)
    target_compile_options(aveva-azure-client-tests PRIVATE /bigobj)
endif()
set(aveva_azure_client_unit_test_labels unit)
if(AVEVA_AZURE_CLIENT_ENABLE_ASAN)
    list(APPEND aveva_azure_client_unit_test_labels sanitizer asan)
elseif(AVEVA_AZURE_CLIENT_ENABLE_UBSAN)
    list(APPEND aveva_azure_client_unit_test_labels sanitizer ubsan)
elseif(AVEVA_AZURE_CLIENT_ENABLE_TSAN)
    list(APPEND aveva_azure_client_unit_test_labels sanitizer tsan)
endif()
gtest_discover_tests(aveva-azure-client-tests PROPERTIES TIMEOUT 30 LABELS "${aveva_azure_client_unit_test_labels}")

# Consumer view of the library (T01): no include path into src/ or tests/, only the installed
# public headers, so any public header that depends on a private one fails to compile here.
add_executable(aveva-azure-client-public-api-tests PublicHeaderSelfContainedTests.cpp)
target_link_libraries(aveva-azure-client-public-api-tests PRIVATE aveva::azure-client GTest::gtest_main)
aveva_azure_client_enable_clang_tidy(aveva-azure-client-public-api-tests)
aveva_azure_client_instrument(aveva-azure-client-public-api-tests)
if(WIN32)
    target_link_libraries(aveva-azure-client-public-api-tests PRIVATE Crypt32)
endif()
gtest_discover_tests(aveva-azure-client-public-api-tests PROPERTIES TIMEOUT 30 LABELS "${aveva_azure_client_unit_test_labels}")

# Engines driven by a multi-threaded executor (T30). Label `concurrency`; run under LinuxDebugTSan.
add_executable(aveva-azure-client-concurrency-tests MultiThreadedExecutorTests.cpp)
target_link_libraries(aveva-azure-client-concurrency-tests PRIVATE aveva::azure-client GTest::gtest_main)
aveva_azure_client_enable_clang_tidy(aveva-azure-client-concurrency-tests)
aveva_azure_client_instrument(aveva-azure-client-concurrency-tests)
if(WIN32)
    target_link_libraries(aveva-azure-client-concurrency-tests PRIVATE Crypt32)
endif()
gtest_discover_tests(
    aveva-azure-client-concurrency-tests
    PROPERTIES TIMEOUT 60 LABELS "${aveva_azure_client_unit_test_labels};concurrency")

# Installs to a scratch prefix and builds/runs a separate consumer through find_package (T059).
# Needs a fully built library, so run it with `ctest -L install` after a full build.
if(AVEVA_AZURE_CLIENT_INSTALL_TEST)
    add_test(
        NAME InstallConsumer
        COMMAND ${CMAKE_COMMAND}
            "-DBUILD_DIR=${CMAKE_BINARY_DIR}"
            "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/cmake/consumer"
            "-DCONFIG=$<CONFIG>"
            "-DGENERATOR=${CMAKE_GENERATOR}"
            "-DDEPENDENCY_PREFIX=${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}"
            "-DWORK_DIR=${CMAKE_BINARY_DIR}/install-consumer"
            "-DTOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/RunInstallConsumerTest.cmake")
    set_tests_properties(InstallConsumer PROPERTIES TIMEOUT 900 LABELS "install")
endif()

# Live Azure integration tests (opt-in: -DAVEVA_AZURE_CLIENT_RUN_INTEGRATION_TESTS=ON).
# Required environment variables (tests GTEST_SKIP with this list when any is missing):
#   AZURE_STORAGE_ACCOUNT_NAME, AZURE_TENANT_ID, AZURE_SERVICE_PRINCIPAL_ID, AZURE_SERVICE_PRINCIPAL_SECRET
# Optional: AZURE_CA_FILE (custom CA bundle). The tests carry the "azure-credentials" label.
if(AVEVA_AZURE_CLIENT_RUN_INTEGRATION_TESTS)
    add_executable(aveva-azure-client-integration-tests
        AzureBlobIntegrationTests.cpp
        AzureIntegrationTestHelpers.cpp
    )
    target_include_directories(aveva-azure-client-integration-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../src)
    target_link_libraries(aveva-azure-client-integration-tests
        PRIVATE aveva::azure-client aveva::http-client Boost::asio Boost::beast GTest::gtest_main OpenSSL::SSL OpenSSL::Crypto
    )
    aveva_azure_client_enable_clang_tidy(aveva-azure-client-integration-tests)
    aveva_azure_client_instrument(aveva-azure-client-integration-tests)
    if(WIN32)
        target_link_libraries(aveva-azure-client-integration-tests PRIVATE Crypt32)
    endif()

    set(aveva_azure_client_integration_test_labels integration azure-credentials)
    if(AVEVA_AZURE_CLIENT_ENABLE_ASAN)
        list(APPEND aveva_azure_client_integration_test_labels sanitizer asan)
    elseif(AVEVA_AZURE_CLIENT_ENABLE_UBSAN)
        list(APPEND aveva_azure_client_integration_test_labels sanitizer ubsan)
    elseif(AVEVA_AZURE_CLIENT_ENABLE_TSAN)
        list(APPEND aveva_azure_client_integration_test_labels sanitizer tsan)
    endif()
    gtest_discover_tests(
        aveva-azure-client-integration-tests
        PROPERTIES
            TIMEOUT 180
            LABELS "${aveva_azure_client_integration_test_labels}"
    )
endif()

if(AVEVA_AZURE_CLIENT_ENABLE_ASAN OR AVEVA_AZURE_CLIENT_ENABLE_UBSAN OR AVEVA_AZURE_CLIENT_ENABLE_TSAN)
    add_custom_target(
        aveva-azure-client-sanitizer-tests
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L sanitizer -C $<CONFIG>
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        USES_TERMINAL
    )
endif()
