# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

find_package(boost_asio CONFIG REQUIRED)
find_package(boost_beast CONFIG REQUIRED)
find_package(boost_intrusive CONFIG REQUIRED)
find_package(boost_unordered CONFIG REQUIRED)
find_package(boost_url CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

include(GoogleTest)

add_executable(aveva-http-client-tests
	ConnectionPoolTests.cpp
	HttpClientConstructionTests.cpp
	HttpClientRequestTests.cpp
	HttpClientResponseTests.cpp
	HttpClientRobustnessTests.cpp
	HttpClientTestHelpers.cpp
	HttpClientTlsTests.cpp
)
# ConnectionPoolTests.cpp exercises the internal pool directly, so the tests need the private
# headers that live alongside the library sources.
target_include_directories(aveva-http-client-tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../src")
target_compile_features(aveva-http-client-tests PRIVATE cxx_std_23)
target_link_libraries(aveva-http-client-tests
	PRIVATE aveva::http-client Boost::asio Boost::beast Boost::intrusive Boost::unordered Boost::url GTest::gtest_main OpenSSL::SSL OpenSSL::Crypto
)
gtest_discover_tests(aveva-http-client-tests PROPERTIES TIMEOUT 10)
