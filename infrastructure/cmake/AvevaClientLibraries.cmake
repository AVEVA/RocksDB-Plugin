# Builds the in-tree aveva-http-client and aveva-azure-client libraries (libs/) as part of this project
# and makes them consumable both in the build tree and from an installed package.
include_guard(GLOBAL)

set(AVEVA_HTTP_CLIENT_TESTS ${AVEVA_BUILD_CLIENT_LIBRARY_TESTS})
set(AVEVA_HTTP_CLIENT_BENCHMARKS OFF)
# Installed below with a config file template owned by this repository.
set(AVEVA_HTTP_CLIENT_INSTALL_CONFIG_FILE_PACKAGE OFF)
add_subdirectory("${PROJECT_SOURCE_DIR}/libs/HttpClient" "${PROJECT_BINARY_DIR}/libs/HttpClient")

# The client libraries are third-party code from this project's point of view, so do not fail the build on
# their warnings.
set_target_properties(aveva-http-client PROPERTIES COMPILE_WARNING_AS_ERROR OFF)

# aveva-azure-client resolves aveva-http-client through find_package(); point it at a shim that reuses the
# target built above instead of looking for an installed package.
set(_aveva_http_client_shim_dir "${PROJECT_BINARY_DIR}/cmake/aveva-http-client")
file(WRITE "${_aveva_http_client_shim_dir}/aveva-http-client-config.cmake"
    "if(NOT TARGET aveva::http-client)\n"
    "    message(FATAL_ERROR \"aveva::http-client must be built in-tree before aveva-azure-client.\")\n"
    "endif()\n")
set(aveva-http-client_DIR "${_aveva_http_client_shim_dir}" CACHE INTERNAL "In-tree aveva-http-client package shim")

set(AVEVA_AZURE_CLIENT_TESTS ${AVEVA_BUILD_CLIENT_LIBRARY_TESTS})
set(AVEVA_AZURE_CLIENT_EXAMPLES OFF)
set(AVEVA_AZURE_CLIENT_BENCHMARKS OFF)
set(AVEVA_AZURE_CLIENT_INSTALL_TEST OFF)
set(AVEVA_AZURE_CLIENT_INSTALL_CONFIG_FILE_PACKAGE ON)
add_subdirectory("${PROJECT_SOURCE_DIR}/libs/AzureClient" "${PROJECT_BINARY_DIR}/libs/AzureClient")
set_target_properties(aveva-azure-client PROPERTIES COMPILE_WARNING_AS_ERROR OFF)

# Header-only test support (FakeHttpClient.hpp, TestFixtures.hpp) of the vendored AzureClient tests, shared with
# the plugin tests. All dependence on the library's test layout is confined to this target.
add_library(aveva-azure-client-test-support INTERFACE)
target_include_directories(aveva-azure-client-test-support INTERFACE "${PROJECT_SOURCE_DIR}/libs/AzureClient/tests")
target_link_libraries(aveva-azure-client-test-support INTERFACE aveva::azure-client)

# Installed config package for aveva-http-client (the library does not ship a config template of its own).
include(CMakePackageConfigHelpers)
get_directory_property(AVEVA_HTTP_CLIENT_PACKAGE_VERSION
    DIRECTORY "${PROJECT_SOURCE_DIR}/libs/HttpClient" DEFINITION PROJECT_VERSION)
set(_aveva_http_client_install_dir "share/aveva-http-client")
configure_package_config_file(
    "${CMAKE_CURRENT_LIST_DIR}/templates/aveva-http-client-config.cmake.in"
    "${PROJECT_BINARY_DIR}/cmake/install/aveva-http-client-config.cmake"
    INSTALL_DESTINATION "${_aveva_http_client_install_dir}")
write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/cmake/install/aveva-http-client-config-version.cmake"
    VERSION "${AVEVA_HTTP_CLIENT_PACKAGE_VERSION}"
    COMPATIBILITY ExactVersion)
install(FILES
    "${PROJECT_BINARY_DIR}/cmake/install/aveva-http-client-config.cmake"
    "${PROJECT_BINARY_DIR}/cmake/install/aveva-http-client-config-version.cmake"
    DESTINATION "${_aveva_http_client_install_dir}"
    COMPONENT aveva-http-client)
install(EXPORT aveva-http-client
    DESTINATION "${_aveva_http_client_install_dir}"
    NAMESPACE aveva::
    FILE aveva-http-client-targets.cmake
    COMPONENT aveva-http-client)
