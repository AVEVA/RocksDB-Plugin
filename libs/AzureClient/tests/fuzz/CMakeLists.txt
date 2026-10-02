find_package(aveva-http-client CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS asio)
find_package(OpenSSL REQUIRED)

# libFuzzer target for the response parsers (T28). Run, for example:
#   ./aveva-azure-client-parse-fuzz -max_total_time=300 -max_len=65536 corpus/
add_executable(aveva-azure-client-parse-fuzz ParseXmlFuzz.cpp)
target_include_directories(aveva-azure-client-parse-fuzz PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../src)
target_link_libraries(aveva-azure-client-parse-fuzz PRIVATE aveva::azure-client aveva::http-client Boost::asio OpenSSL::SSL OpenSSL::Crypto)
target_link_options(aveva-azure-client-parse-fuzz PRIVATE -fsanitize=fuzzer,address)
aveva_azure_client_instrument(aveva-azure-client-parse-fuzz)
