find_package(boost_asio CONFIG REQUIRED)

add_executable(aveva-http-client-benchmarks
    BodyViewAllocationBenchmark.cpp
)

target_include_directories(aveva-http-client-benchmarks
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(aveva-http-client-benchmarks
    PRIVATE
        aveva::http-client
        Boost::asio
)

target_compile_features(aveva-http-client-benchmarks PRIVATE cxx_std_23)

if(WIN32)
    target_link_libraries(aveva-http-client-benchmarks PRIVATE Crypt32)
endif()
