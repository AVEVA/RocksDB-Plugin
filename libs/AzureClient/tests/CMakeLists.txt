# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

find_package(aveva-http-client CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS asio beast)
find_package(boost_url CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

include(GoogleTest)

set(aveva_azure_client_unit_test_labels unit azure-client)
if(NOT PROJECT_IS_TOP_LEVEL AND AVEVA_ROCKSDB_SANITIZER)
    list(APPEND aveva_azure_client_unit_test_labels sanitizer)
    if("address" IN_LIST AVEVA_ROCKSDB_SANITIZER)
        list(APPEND aveva_azure_client_unit_test_labels asan)
    endif()
    if("undefined" IN_LIST AVEVA_ROCKSDB_SANITIZER)
        list(APPEND aveva_azure_client_unit_test_labels ubsan)
    endif()
    if("thread" IN_LIST AVEVA_ROCKSDB_SANITIZER)
        list(APPEND aveva_azure_client_unit_test_labels tsan)
    endif()
elseif(AVEVA_AZURE_CLIENT_ENABLE_ASAN)
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
    PUBLIC aveva::azure-client aveva::http-client Boost::asio Boost::beast Boost::url GTest::gtest OpenSSL::SSL OpenSSL::Crypto
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
    gtest_discover_tests(${target} DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 30 LABELS "${aveva_azure_client_unit_test_labels}")
endfunction()

aveva_azure_client_add_unit_tests(aveva-azure-client-pipeline-tests
    AsyncCancellationTests.cpp
    AsyncLifetimeTests.cpp
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
gtest_discover_tests(aveva-azure-client-public-api-tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 30 LABELS "${aveva_azure_client_unit_test_labels}")

# End-to-end loopback allocation/latency measurement (R22): the production BlobClient over the real HttpClient and a
# page-blob server on its own thread. It replaces operator new, so it is its own executable. Label `benchmark`;
# loopback CPU evidence only, never Azure network results.
add_executable(aveva-azure-client-e2e-benchmark EndToEndAllocationBenchmark.cpp)
target_link_libraries(aveva-azure-client-e2e-benchmark PRIVATE aveva::azure-client aveva::http-client Boost::asio OpenSSL::Crypto GTest::gtest_main)
if(MSVC)
    target_compile_options(aveva-azure-client-e2e-benchmark PRIVATE /bigobj)
endif()
if(WIN32)
    target_link_libraries(aveva-azure-client-e2e-benchmark PRIVATE Crypt32)
endif()
gtest_discover_tests(aveva-azure-client-e2e-benchmark DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 120 LABELS "benchmark;azure-client")

# Engines driven by a multi-threaded executor (T30). Label `concurrency`; run with AVEVA_AZURE_CLIENT_ENABLE_TSAN=ON.
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
    DISCOVERY_TIMEOUT 60
    PROPERTIES TIMEOUT 60 LABELS "${aveva_azure_client_unit_test_labels};concurrency")

if(AVEVA_AZURE_CLIENT_ENABLE_ASAN OR AVEVA_AZURE_CLIENT_ENABLE_UBSAN OR AVEVA_AZURE_CLIENT_ENABLE_TSAN)
    add_custom_target(
        aveva-azure-client-sanitizer-tests
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -L sanitizer -C $<CONFIG>
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        USES_TERMINAL
    )
endif()
