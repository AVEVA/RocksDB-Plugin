# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

find_package(aveva-http-client CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS asio beast)
find_package(GTest CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

include(GoogleTest)

set(aveva_azure_client_unit_test_labels unit)
if(AVEVA_AZURE_CLIENT_ENABLE_ASAN)
    list(APPEND aveva_azure_client_unit_test_labels sanitizer asan)
elseif(AVEVA_AZURE_CLIENT_ENABLE_UBSAN)
    list(APPEND aveva_azure_client_unit_test_labels sanitizer ubsan)
elseif(AVEVA_AZURE_CLIENT_ENABLE_TSAN)
    list(APPEND aveva_azure_client_unit_test_labels sanitizer tsan)
endif()

# Shared fake-transport instantiations and the main() that drains FakeHttpClient completions.
add_library(aveva-azure-client-test-main STATIC FakeInstantiations.cpp TestEventListener.cpp)
target_include_directories(aveva-azure-client-test-main PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_link_libraries(aveva-azure-client-test-main
    PUBLIC aveva::azure-client aveva::http-client Boost::asio Boost::beast GTest::gtest OpenSSL::SSL OpenSSL::Crypto
)
if(MSVC)
    target_compile_options(aveva-azure-client-test-main PRIVATE /bigobj)
endif()

# One executable per subsystem keeps each link and rebuild small and lets CI run a subsystem on its own.
function(aveva_azure_client_add_unit_tests target)
    add_executable(${target} ${ARGN})
    target_link_libraries(${target} PRIVATE aveva-azure-client-test-main)
    aveva_azure_client_enable_clang_tidy(${target})
    aveva_azure_client_instrument(${target})
    if(WIN32)
        target_link_libraries(${target} PRIVATE Crypt32)
    endif()
    if(MSVC)
        target_compile_options(${target} PRIVATE /bigobj)
    endif()
    gtest_discover_tests(${target} DISCOVERY_TIMEOUT 15 PROPERTIES TIMEOUT 30 LABELS "${aveva_azure_client_unit_test_labels}")
endfunction()

aveva_azure_client_add_unit_tests(aveva-azure-client-pipeline-tests
    AsyncBehaviorTests.cpp
    RetryTests.cpp
    ParseContentRangeTests.cpp
    RequestOptionsTests.cpp
    OptionValidationTests.cpp
    EndpointValidationTests.cpp
    HeaderInjectionTests.cpp
    MetadataValidationTests.cpp
    ParserRobustnessTests.cpp
    SuppressedErrorResponseTests.cpp
    ResponseTests.cpp
    BlockOnTests.cpp
    SharedHttpOwnershipTests.cpp
)
aveva_azure_client_add_unit_tests(aveva-azure-client-blob-tests
    BlobContainerClientTests.cpp
    BlobRequestHelpersTests.cpp
    BlobServiceClientTests.cpp
    BlobStorageErrorCategoryTests.cpp
    PageBlobClientTests.cpp
    ModelTests.cpp
    LeaseTests.cpp
    RequestShapeTests.cpp
    BatchDeleteTests.cpp
)
aveva_azure_client_add_unit_tests(aveva-azure-client-transfer-tests
    DownloadToFileTests.cpp
    ParallelDownloadTests.cpp
)
aveva_azure_client_add_unit_tests(aveva-azure-client-credentials-tests
    CredentialsTests.cpp
    TokenCredentialTests.cpp
)
# Consumer view of the library (T01): no include path into src/ or tests/, only the installed
# public headers, so any public header that depends on a private one fails to compile here.
add_executable(aveva-azure-client-public-api-tests PublicHeaderSelfContainedTests.cpp)
target_link_libraries(aveva-azure-client-public-api-tests PRIVATE aveva::azure-client GTest::gtest_main)
if(MSVC)
    target_compile_options(aveva-azure-client-public-api-tests PRIVATE /bigobj)
endif()
aveva_azure_client_enable_clang_tidy(aveva-azure-client-public-api-tests)
aveva_azure_client_instrument(aveva-azure-client-public-api-tests)
if(WIN32)
    target_link_libraries(aveva-azure-client-public-api-tests PRIVATE Crypt32)
endif()
gtest_discover_tests(aveva-azure-client-public-api-tests PROPERTIES TIMEOUT 30 LABELS "${aveva_azure_client_unit_test_labels}")

# Engines driven by a multi-threaded executor (T30). Label `concurrency`; run under LinuxDebugTSan.
add_executable(aveva-azure-client-concurrency-tests MultiThreadedExecutorTests.cpp)
target_include_directories(aveva-azure-client-concurrency-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_link_libraries(aveva-azure-client-concurrency-tests PRIVATE aveva::azure-client GTest::gtest_main)
if(MSVC)
    target_compile_options(aveva-azure-client-concurrency-tests PRIVATE /bigobj)
endif()
aveva_azure_client_enable_clang_tidy(aveva-azure-client-concurrency-tests)
aveva_azure_client_instrument(aveva-azure-client-concurrency-tests)
if(WIN32)
    target_link_libraries(aveva-azure-client-concurrency-tests PRIVATE Crypt32)
endif()
gtest_discover_tests(
    aveva-azure-client-concurrency-tests
    PROPERTIES TIMEOUT 60 LABELS "${aveva_azure_client_unit_test_labels};concurrency")

if(AVEVA_AZURE_CLIENT_ENABLE_ASAN OR AVEVA_AZURE_CLIENT_ENABLE_UBSAN OR AVEVA_AZURE_CLIENT_ENABLE_TSAN)
    add_custom_target(
        aveva-azure-client-sanitizer-tests
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L sanitizer -C $<CONFIG>
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        USES_TERMINAL
    )
endif()
