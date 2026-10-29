# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA

add_library(aveva-http-client STATIC
    HttpHeader.cpp
    HttpClient.cpp
    HttpClientOptions.cpp
    HttpMethod.cpp
    HttpRequest.cpp
    HttpRequestOptions.cpp
    HttpResponse.cpp
    private/HttpErrorCategory.cpp
    private/Exchange.cpp
    private/IdleSweeper.cpp
    private/RequestValidation.cpp
    private/TlsContextConfigurator.cpp
)
add_library(aveva::http-client ALIAS aveva-http-client)
set_target_properties(aveva-http-client PROPERTIES POSITION_INDEPENDENT_CODE ON)
set(base-include-dir "${CMAKE_CURRENT_SOURCE_DIR}/../include")
target_sources(aveva-http-client
    PUBLIC
    FILE_SET HEADERS BASE_DIRS "${base-include-dir}" FILES
        "${base-include-dir}/AVEVA/HttpClient/HttpHeader.hpp"
        "${base-include-dir}/AVEVA/HttpClient/Detail/Lifetime.hpp"
        "${base-include-dir}/AVEVA/HttpClient/HttpClient.hpp"
        "${base-include-dir}/AVEVA/HttpClient/HttpClientError.hpp"
        "${base-include-dir}/AVEVA/HttpClient/HttpClientOptions.hpp"
        "${base-include-dir}/AVEVA/HttpClient/HttpMethod.hpp"
        "${base-include-dir}/AVEVA/HttpClient/HttpRequest.hpp"
        "${base-include-dir}/AVEVA/HttpClient/HttpRequestOptions.hpp"
        "${base-include-dir}/AVEVA/HttpClient/HttpResponse.hpp"
)

find_package(boost_asio CONFIG REQUIRED)
find_package(boost_beast CONFIG REQUIRED)
find_package(boost_intrusive CONFIG REQUIRED)
find_package(boost_scope CONFIG REQUIRED)
find_package(boost_unordered CONFIG REQUIRED)
find_package(boost_url CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Microsoft.GSL CONFIG REQUIRED)
find_package(aveva-install-library CONFIG REQUIRED)

target_compile_features(aveva-http-client PUBLIC cxx_std_23)
if(MSVC)
    target_compile_options(aveva-http-client PRIVATE /bigobj)
endif()
target_link_libraries(aveva-http-client
    PUBLIC Boost::asio Microsoft.GSL::GSL
    PRIVATE Boost::beast Boost::intrusive Boost::scope Boost::unordered Boost::url OpenSSL::SSL OpenSSL::Crypto
)
aveva_install_library(aveva-http-client)