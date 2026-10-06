# Builds the in-tree aveva-http-client and aveva-azure-client libraries (libs/) as part of this project
# and makes them consumable both in the build tree and from an installed package.
include_guard(GLOBAL)

set(AVEVA_HTTP_CLIENT_TESTS ${AVEVA_BUILD_CLIENT_LIBRARY_TESTS})
set(AVEVA_HTTP_CLIENT_BENCHMARKS OFF)
# Installed by aveva_install_library() using the library's own config template.
set(AVEVA_HTTP_CLIENT_INSTALL_CONFIG_FILE_PACKAGE ON)
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

# The Linux presets put -Werror straight into CMAKE_CXX_FLAGS, which COMPILE_WARNING_AS_ERROR cannot undo, so
# relax it explicitly on every target (libraries and their tests) of the vendored directories.
if(MSVC)
    set(_aveva_no_werror /WX-)
else()
    set(_aveva_no_werror -Wno-error)
endif()
foreach(_aveva_dir IN ITEMS libs/HttpClient libs/AzureClient)
    get_property(_aveva_lib_targets DIRECTORY "${PROJECT_SOURCE_DIR}/${_aveva_dir}" PROPERTY BUILDSYSTEM_TARGETS)
    get_property(_aveva_lib_subdirs DIRECTORY "${PROJECT_SOURCE_DIR}/${_aveva_dir}" PROPERTY SUBDIRECTORIES)
    foreach(_aveva_sub IN LISTS _aveva_lib_subdirs)
        get_property(_aveva_sub_targets DIRECTORY "${_aveva_sub}" PROPERTY BUILDSYSTEM_TARGETS)
        list(APPEND _aveva_lib_targets ${_aveva_sub_targets})
    endforeach()
    foreach(_aveva_target IN LISTS _aveva_lib_targets)
        get_target_property(_aveva_type ${_aveva_target} TYPE)
        if(_aveva_type MATCHES "^(STATIC_LIBRARY|SHARED_LIBRARY|OBJECT_LIBRARY|EXECUTABLE)$")
            set_target_properties(${_aveva_target} PROPERTIES COMPILE_WARNING_AS_ERROR OFF)
            target_compile_options(${_aveva_target} PRIVATE ${_aveva_no_werror})
        endif()
    endforeach()
endforeach()

# Header-only test support (FakeHttpClient.hpp, TestFixtures.hpp) of the vendored AzureClient tests, shared with
# the plugin tests. All dependence on the library's test layout is confined to this target.
add_library(aveva-azure-client-test-support INTERFACE)
target_include_directories(aveva-azure-client-test-support INTERFACE "${PROJECT_SOURCE_DIR}/libs/AzureClient/tests")
target_link_libraries(aveva-azure-client-test-support INTERFACE aveva::azure-client)

# Asio types are shared across the plugin and both libraries, so they must agree on the Windows API level.
foreach(_aveva_lib IN ITEMS aveva-http-client aveva-azure-client)
    target_compile_definitions(${_aveva_lib} PUBLIC $<$<PLATFORM_ID:Windows>:_WIN32_WINNT=0x0A00>)
endforeach()
