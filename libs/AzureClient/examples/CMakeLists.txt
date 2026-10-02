# Builds as part of the main project, or standalone against an installed package:
#   cmake -S examples -B build/examples -DCMAKE_PREFIX_PATH=<install prefix>;<dependency prefix>
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    cmake_minimum_required(VERSION 3.22)
    project(aveva-azure-client-examples LANGUAGES CXX)
    find_package(aveva-azure-client CONFIG REQUIRED)
endif()

add_executable(aveva-azure-client-quick-start quick_start.cpp)
add_executable(aveva-azure-client-example-shared-key shared_key.cpp)
add_executable(aveva-azure-client-example-token-credential token_credential.cpp)
add_executable(aveva-azure-client-example-cancellation cancellation.cpp)
foreach(example_target
    aveva-azure-client-quick-start
    aveva-azure-client-example-shared-key
    aveva-azure-client-example-token-credential
    aveva-azure-client-example-cancellation)
    target_link_libraries(${example_target} PRIVATE aveva::azure-client)
    target_compile_features(${example_target} PRIVATE cxx_std_23)
    if(COMMAND aveva_azure_client_enable_clang_tidy)
        aveva_azure_client_enable_clang_tidy(${example_target})
    endif()
    if(WIN32)
        target_link_libraries(${example_target} PRIVATE Crypt32)
    endif()
endforeach()
