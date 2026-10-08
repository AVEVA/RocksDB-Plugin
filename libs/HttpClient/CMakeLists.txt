# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

cmake_minimum_required(VERSION 3.22)
project(aveva-http-client LANGUAGES CXX VERSION 0.0.1)
option(AVEVA_HTTP_CLIENT_TESTS "Build tests" ${PROJECT_IS_TOP_LEVEL})
add_subdirectory(src)
if (${AVEVA_HTTP_CLIENT_TESTS})
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
endif()
