# Fails unless the CMake project, vcpkg.json and the overlay port manifest agree on one version.
# Usage: cmake -DSOURCE_DIR=<repo root> -DPROJECT_VERSION=<x.y.z> -P CheckVersions.cmake
foreach(manifest "vcpkg.json" "infrastructure/overlay-ports/aveva-azure-client/vcpkg.json")
    file(READ "${SOURCE_DIR}/${manifest}" contents)
    string(JSON manifest_version GET "${contents}" "version")
    if(NOT manifest_version STREQUAL PROJECT_VERSION)
        message(SEND_ERROR "${manifest} version '${manifest_version}' != project version '${PROJECT_VERSION}'")
    endif()
endforeach()
