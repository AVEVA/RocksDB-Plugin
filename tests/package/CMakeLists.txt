# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

cmake_minimum_required(VERSION 3.25)
project(aveva-rocksdb-plugin-package-test LANGUAGES CXX)

# Consumer smoke test for the installed packages. It is deliberately not part of the main build: it needs an
# install tree. See README.md in this directory for how to run it.
find_package(aveva-rocksdb-plugin-azure CONFIG REQUIRED)

add_executable(package-consumer main.cpp)
target_link_libraries(package-consumer PRIVATE aveva::rocksdb-plugin-azure)
