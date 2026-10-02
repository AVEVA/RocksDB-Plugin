find_package(benchmark CONFIG REQUIRED)

add_executable(aveva-azure-client-benchmarks
    RequestBuildingBenchmarks.cpp
    SharedKeyBenchmarks.cpp
    UploadThroughputBenchmarks.cpp
)

target_include_directories(aveva-azure-client-benchmarks
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(aveva-azure-client-benchmarks
    PRIVATE
        aveva::azure-client
        benchmark::benchmark_main
)

target_compile_features(aveva-azure-client-benchmarks PRIVATE cxx_std_23)
aveva_azure_client_enable_clang_tidy(aveva-azure-client-benchmarks)

if(WIN32)
    target_link_libraries(aveva-azure-client-benchmarks PRIVATE Crypt32)
endif()

# Smoke run only (confirms the benchmarks execute); use real measurement settings for numbers.
add_test(NAME aveva-azure-client-benchmarks-smoke
    COMMAND aveva-azure-client-benchmarks --benchmark_min_time=0.01s --benchmark_repetitions=1)
set_tests_properties(aveva-azure-client-benchmarks-smoke PROPERTIES LABELS "benchmark" TIMEOUT 300)
