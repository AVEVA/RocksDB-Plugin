# Installs the library to a scratch prefix, then configures, builds and runs a separate consumer project
# against that prefix. Invoked by CTest as `cmake -P` with:
#   BUILD_DIR, SOURCE_DIR, CONFIG, GENERATOR, DEPENDENCY_PREFIX, WORK_DIR, TOOLCHAIN_FILE (optional)
foreach(required BUILD_DIR SOURCE_DIR CONFIG GENERATOR WORK_DIR)
    if(NOT DEFINED ${required})
        message(FATAL_ERROR "RunInstallConsumerTest.cmake: ${required} is required")
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

# The dependency prefix goes through the environment: a second ';'-separated entry in -DCMAKE_PREFIX_PATH
# would be split by the list handling here.
set(ENV{CMAKE_PREFIX_PATH} "${DEPENDENCY_PREFIX}")

set(configure_args -S "${SOURCE_DIR}" -B "${consumer_build}" -G "${GENERATOR}"
    "-DCMAKE_PREFIX_PATH=${prefix}" "-DCMAKE_BUILD_TYPE=${CONFIG}" "-DCMAKE_CONFIGURATION_TYPES=${CONFIG}")
if(TOOLCHAIN_FILE)
    list(APPEND configure_args "-DCMAKE_TOOLCHAIN_FILE=${TOOLCHAIN_FILE}")
endif()
run_step("Configure consumer" "${CMAKE_COMMAND}" ${configure_args})
run_step("Build consumer" "${CMAKE_COMMAND}" --build "${consumer_build}" --config "${CONFIG}")

foreach(candidate
    "${consumer_build}/${CONFIG}/install-consumer.exe"
    "${consumer_build}/${CONFIG}/install-consumer"
    "${consumer_build}/install-consumer.exe"
    "${consumer_build}/install-consumer")
    if(EXISTS "${candidate}")
        set(consumer_exe "${candidate}")
        break()
    endif()
endforeach()
if(NOT consumer_exe)
    message(FATAL_ERROR "Consumer executable was not produced under ${consumer_build}")
endif()
run_step("Run consumer" "${consumer_exe}")
