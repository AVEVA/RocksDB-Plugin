# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2025 AVEVA

find_package(GTest CONFIG REQUIRED)
include_directories(.)
add_subdirectory(AVEVA/RocksDB/Plugin/Core)
add_subdirectory(AVEVA/RocksDB/Plugin/Azure/Impl)
