# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

# Installs the plugin to a scratch prefix, then configures and builds tests/package against it.
# Invoked by CTest as `cmake -P` with:
#   BUILD_DIR, SOURCE_DIR, CONFIG, GENERATOR, DEPENDENCY_PREFIX, WORK_DIR, TOOLCHAIN_FILE (optional)
foreach(required BUILD_DIR SOURCE_DIR CONFIG GENERATOR DEPENDENCY_PREFIX WORK_DIR)
    if(NOT DEFINED ${required})
        message(FATAL_ERROR "RunPackageTest.cmake: ${required} is required")
    endif()
endforeach()

set(prefix "${WORK_DIR}/prefix")
set(consumer_build "${WORK_DIR}/consumer-build")
file(REMOVE_RECURSE "${WORK_DIR}")

function(run_step description)
    execute_process(COMMAND ${ARGN} RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE output)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "${description} failed (${result}):\n${output}")
    endif()
    message(STATUS "${description}: ok")
endfunction()

run_step("Install" "${CMAKE_COMMAND}" --install "${BUILD_DIR}" --prefix "${prefix}" --config "${CONFIG}")

# Passed through the environment because a second ';'-separated entry in -DCMAKE_PREFIX_PATH would be split here.
set(ENV{CMAKE_PREFIX_PATH} "${DEPENDENCY_PREFIX}")

set(configure_args -S "${SOURCE_DIR}" -B "${consumer_build}" -G "${GENERATOR}"
    "-DCMAKE_PREFIX_PATH=${prefix}" "-DCMAKE_BUILD_TYPE=${CONFIG}" "-DCMAKE_CONFIGURATION_TYPES=${CONFIG}")
if(TOOLCHAIN_FILE)
    list(APPEND configure_args "-DCMAKE_TOOLCHAIN_FILE=${TOOLCHAIN_FILE}" -DVCPKG_MANIFEST_MODE=OFF)
endif()
run_step("Configure consumer" "${CMAKE_COMMAND}" ${configure_args})
run_step("Build consumer" "${CMAKE_COMMAND}" --build "${consumer_build}" --config "${CONFIG}")
