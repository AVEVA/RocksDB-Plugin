# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

# Builds the in-tree aveva-http-client library (libs/) as part of this project
# and makes them consumable both in the build tree and from an installed package.
include_guard(GLOBAL)

set(AVEVA_HTTP_CLIENT_TESTS ${AVEVA_ROCKSDB_TESTS})
# Installed by aveva_install_library() using the library's own config template.
set(AVEVA_HTTP_CLIENT_INSTALL_CONFIG_FILE_PACKAGE ON)
add_subdirectory("${PROJECT_SOURCE_DIR}/libs/HttpClient" "${PROJECT_BINARY_DIR}/libs/HttpClient")

# Asio types are shared across the plugin and the client libraries, so they must agree on the Windows API level.
target_compile_definitions(aveva-http-client PUBLIC $<$<PLATFORM_ID:Windows>:_WIN32_WINNT=0x0A00>)
