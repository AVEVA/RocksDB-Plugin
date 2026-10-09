# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

cmake_minimum_required(VERSION 3.22)
project(aveva-azure-client LANGUAGES CXX VERSION 0.0.1)

option(AVEVA_AZURE_CLIENT_TESTS "Build tests" ON)
option(AVEVA_AZURE_CLIENT_ENABLE_ASAN "Enable AddressSanitizer instrumentation" OFF)
option(AVEVA_AZURE_CLIENT_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer instrumentation" OFF)
option(AVEVA_AZURE_CLIENT_ENABLE_TSAN "Enable ThreadSanitizer instrumentation (incompatible with AVEVA_AZURE_CLIENT_ENABLE_ASAN)" OFF)
option(AVEVA_AZURE_CLIENT_ENABLE_COVERAGE "Enable compiler coverage instrumentation" OFF)
option(AVEVA_AZURE_CLIENT_ENABLE_CLANG_TIDY "Enable clang-tidy analysis during compilation" OFF)


function(aveva_azure_client_enable_clang_tidy target_name)
    if(NOT AVEVA_AZURE_CLIENT_ENABLE_CLANG_TIDY)
        return()
    endif()

    if(NOT DEFINED AVEVA_AZURE_CLIENT_CLANG_TIDY)
        find_program(AVEVA_AZURE_CLIENT_CLANG_TIDY_EXE NAMES clang-tidy REQUIRED)
        set(AVEVA_AZURE_CLIENT_CLANG_TIDY_EXTRA_ARGS "")
        if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_FRONTEND_VARIANT STREQUAL "MSVC")
            # With clang-cl + --driver-mode=cl, clang-tidy's "-std=c++23" extra arg otherwise resets the
            # exception model to the plain-Clang default (no exceptions), even though /EHsc is on the
            # command line, so re-assert it explicitly for clang-tidy's own invocation.
            set(AVEVA_AZURE_CLIENT_CLANG_TIDY_EXTRA_ARGS ";--extra-arg=/EHsc")
        endif()
        set(
            AVEVA_AZURE_CLIENT_CLANG_TIDY
            "${AVEVA_AZURE_CLIENT_CLANG_TIDY_EXE};--config-file=${CMAKE_SOURCE_DIR}/.clang-tidy${AVEVA_AZURE_CLIENT_CLANG_TIDY_EXTRA_ARGS}"
            CACHE INTERNAL
            "clang-tidy command for aveva-azure-client")
    endif()

    set_property(TARGET "${target_name}" PROPERTY CXX_CLANG_TIDY "${AVEVA_AZURE_CLIENT_CLANG_TIDY}")
endfunction()

# Instrumentation is carried by INTERFACE targets that are linked only into the library, tests, so
# nothing else is compiled with sanitizer or coverage flags. Each target is
# always defined (empty when its option is off) so consumers can link all of them unconditionally.
add_library(aveva_azure_client_asan INTERFACE)
add_library(aveva_azure_client_ubsan INTERFACE)
add_library(aveva_azure_client_tsan INTERFACE)
add_library(aveva_azure_client_coverage INTERFACE)

if(AVEVA_AZURE_CLIENT_ENABLE_ASAN)
    if(MSVC)
        target_compile_options(aveva_azure_client_asan INTERFACE /fsanitize=address)
        target_link_options(aveva_azure_client_asan INTERFACE /fsanitize=address)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(aveva_azure_client_asan INTERFACE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(aveva_azure_client_asan INTERFACE -fsanitize=address)
    else()
        message(WARNING "AVEVA_AZURE_CLIENT_ENABLE_ASAN is ON, but ${CMAKE_CXX_COMPILER_ID} is not configured for AddressSanitizer.")
    endif()
endif()

if(AVEVA_AZURE_CLIENT_ENABLE_UBSAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(aveva_azure_client_ubsan INTERFACE -fsanitize=undefined -fno-omit-frame-pointer)
        target_link_options(aveva_azure_client_ubsan INTERFACE -fsanitize=undefined)
    else()
        message(WARNING "AVEVA_AZURE_CLIENT_ENABLE_UBSAN is ON, but ${CMAKE_CXX_COMPILER_ID} is not configured for UndefinedBehaviorSanitizer.")
    endif()
endif()

if(AVEVA_AZURE_CLIENT_ENABLE_TSAN)
    if(AVEVA_AZURE_CLIENT_ENABLE_ASAN)
        message(FATAL_ERROR "AVEVA_AZURE_CLIENT_ENABLE_TSAN and AVEVA_AZURE_CLIENT_ENABLE_ASAN cannot be combined.")
    endif()
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(aveva_azure_client_tsan INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(aveva_azure_client_tsan INTERFACE -fsanitize=thread)
    else()
        message(WARNING "AVEVA_AZURE_CLIENT_ENABLE_TSAN is ON, but ${CMAKE_CXX_COMPILER_ID} is not configured for ThreadSanitizer.")
    endif()
endif()

if(AVEVA_AZURE_CLIENT_ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(aveva_azure_client_coverage INTERFACE --coverage)
        target_link_options(aveva_azure_client_coverage INTERFACE --coverage)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "AVEVA_AZURE_CLIENT_ENABLE_COVERAGE is supported with GCC (gcovr) only; the Clang path is not exercised by CI.")
    elseif(MSVC)
        message(STATUS "AVEVA_AZURE_CLIENT_ENABLE_COVERAGE is ON. Use OpenCppCoverage to wrap ctest on Windows builds.")
    else()
        message(WARNING "AVEVA_AZURE_CLIENT_ENABLE_COVERAGE is ON, but ${CMAKE_CXX_COMPILER_ID} is not configured for coverage instrumentation.")
    endif()
endif()

# Links every instrumentation target into `target` (a no-op for those that are disabled). BUILD_INTERFACE
# keeps these build-only targets out of the exported/installed link interface.
function(aveva_azure_client_instrument target_name)
    target_link_libraries("${target_name}" PRIVATE
        "$<BUILD_INTERFACE:aveva_azure_client_asan>"
        "$<BUILD_INTERFACE:aveva_azure_client_ubsan>"
        "$<BUILD_INTERFACE:aveva_azure_client_tsan>"
        "$<BUILD_INTERFACE:aveva_azure_client_coverage>")
endfunction()

add_subdirectory(src)

if(AVEVA_AZURE_CLIENT_TESTS)
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
endif()

